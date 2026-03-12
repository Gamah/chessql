/*
 * lichess_import.cpp — main entry point
 *
 * Build:
 *   g++ -O3 -std=c++17 -o lichess_import \
 *       lichess_import.cpp zobrist.cpp \
 *       -lpq -lzstd -lpthread
 *
 * Usage:
 *   ./lichess_import \
 *       --file   db.pgn.zst \
 *       --dsn    "postgresql://lichess:lichess@localhost/lichess" \
 *       --parsers 54 \
 *       --batch   50000
 *
 * Resume: on restart, reads ~/.lichess_import_ckpt and skips games
 *         up to and including the checkpointed lichess_id.
 *
 * Verify: ./lichess_import --verify 1000 ...
 *         Parses first 1000 games, writes JSON to stdout for comparison
 *         against python-chess output.
 */

#include "pgn_parser.h"   // includes board.h, zobrist.h

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/stat.h>
#include <libpq-fe.h>
#include <zstd.h>

// ── Global stop flag ──────────────────────────────────────────────────────────
static std::atomic<bool> g_stop{false};
static std::atomic<bool> g_fatal{false};
static std::string       g_fatal_msg;

// ── Bounded queue ─────────────────────────────────────────────────────────────
template<typename T>
struct BoundedQueue {
    std::deque<T>           q;
    std::mutex              mu;
    std::condition_variable cv_push, cv_pop;
    size_t                  cap;
    bool                    done = false;

    BoundedQueue(size_t capacity) : cap(capacity) {}

    void push(T item) {
        std::unique_lock<std::mutex> lk(mu);
        cv_push.wait(lk, [&]{ return q.size() < cap || g_stop.load(); });
        if (g_stop.load()) return;
        q.push_back(std::move(item));
        cv_pop.notify_one();
    }

    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(mu);
        cv_pop.wait(lk, [&]{ return !q.empty() || done || g_stop.load(); });
        if (q.empty()) return false;
        out = std::move(q.front());
        q.pop_front();
        cv_push.notify_one();
        return true;
    }

    void set_done() {
        std::unique_lock<std::mutex> lk(mu);
        done = true;
        cv_pop.notify_all();
        cv_push.notify_all();
    }

    void wake_all() {
        cv_push.notify_all();
        cv_pop.notify_all();
    }
};

// ── Checkpoint ────────────────────────────────────────────────────────────────
static std::string checkpoint_path() {
    const char* home = getenv("HOME");
    return std::string(home ? home : ".") + "/.lichess_import_ckpt";
}

static std::string read_checkpoint() {
    std::ifstream f(checkpoint_path());
    if (!f.is_open()) return "";
    std::string s;
    std::getline(f, s);
    return s;
}

static void write_checkpoint(const std::string& lichess_id, long long games) {
    std::ofstream f(checkpoint_path());
    f << lichess_id << "\n" << games << "\n";
}

// ── Postgres COPY helpers ─────────────────────────────────────────────────────
// We use libpq's COPY IN protocol for maximum bulk-insert throughput.
// Each field is tab-separated; NULL represented as \N.

static std::string esc_copy(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c=='\t')       out += "\\t";
        else if (c=='\n')  out += "\\n";
        else if (c=='\r')  out += "\\r";
        else if (c=='\\')  out += "\\\\";
        else               out += c;
    }
    return out;
}




// Execute a SQL statement, die on failure
static void exec_sql(PGconn* conn, const std::string& sql) {
    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(conn);
        PQclear(res);
        throw std::runtime_error("SQL error: " + err + "\nQuery: " + sql);
    }
    PQclear(res);
}

// COPY a vector of tab-delimited rows into table
static void copy_rows(PGconn* conn, const std::string& copy_cmd,
                      const std::vector<std::string>& rows) {
    if (rows.empty()) return;

    PGresult* res = PQexec(conn, copy_cmd.c_str());
    if (PQresultStatus(res) != PGRES_COPY_IN) {
        std::string err = PQerrorMessage(conn);
        PQclear(res);
        throw std::runtime_error("COPY failed to start: " + err);
    }
    PQclear(res);

    for (const auto& row : rows) {
        if (PQputCopyData(conn, row.c_str(), (int)row.size()) != 1) {
            std::string err = PQerrorMessage(conn);
            PQputCopyEnd(conn, "aborted");
            throw std::runtime_error("COPY data error: " + err);
        }
    }

    if (PQputCopyEnd(conn, nullptr) != 1) {
        throw std::runtime_error("COPY end error: " + std::string(PQerrorMessage(conn)));
    }

    // Wait for result
    PGresult* final_res = PQgetResult(conn);
    ExecStatusType status = PQresultStatus(final_res);
    std::string err = PQerrorMessage(conn);
    PQclear(final_res);
    // Drain any remaining results
    while (PGresult* r = PQgetResult(conn)) PQclear(r);

    if (status != PGRES_COMMAND_OK) {
        throw std::runtime_error("COPY final error: " + err);
    }
}

// ── Dimension resolver ────────────────────────────────────────────────────────
// Resolves player IDs, time control IDs, opening IDs, move IDs.
// Thread-safe: protected by mutex.
// Move IDs are resolved lazily during write (same as Python version).

struct DimResolver {
    std::unordered_map<std::string,int> players;
    std::unordered_map<std::string,int16_t> time_controls;
    std::unordered_map<std::string,int16_t> openings; // "ECO|name" → id
    std::unordered_map<std::string,int32_t> moves;    // uci → id

    PGconn* conn;
    std::mutex mu;

    void load(PGconn* c) {
        conn = c;
        auto load_table = [&](const std::string& sql,
                               auto& map, auto key_col, auto val_col) {
            PGresult* res = PQexec(conn, sql.c_str());
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::string err = PQerrorMessage(conn);
                PQclear(res);
                throw std::runtime_error("Load dim failed: " + err);
            }
            int nr = PQntuples(res);
            for (int i=0; i<nr; i++) {
                std::string k = PQgetvalue(res, i, key_col);
                int         v = std::stoi(PQgetvalue(res, i, val_col));
                map[k] = v;
            }
            PQclear(res);
        };

        load_table("SELECT username, id FROM players",
                   players, 0, 1);
        load_table("SELECT raw, id FROM time_controls",
                   time_controls, 0, 1);
        load_table("SELECT eco||'|'||name, id FROM openings",
                   openings, 0, 1);
        load_table("SELECT uci, id FROM moves",
                   moves, 0, 1);
    }

    // Upsert a player, return ID. Caller holds mu.
    int get_player(const std::string& name) {
        auto it = players.find(name);
        if (it != players.end()) return it->second;

        std::string esc = name;
        // Simple escape: replace ' with ''
        std::string sql = "INSERT INTO players(username) VALUES('" ;
        for (char c : name) { if (c=='\'') sql+="''"; else sql+=c; }
        sql += "') ON CONFLICT(username) DO UPDATE SET username=EXCLUDED.username RETURNING id";
        PGresult* res = PQexec(conn, sql.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::string err = PQerrorMessage(conn);
            PQclear(res); throw std::runtime_error("player upsert: "+err);
        }
        int id = std::stoi(PQgetvalue(res,0,0));
        PQclear(res);
        players[name] = id;
        return id;
    }

    std::optional<int16_t> get_tc(const std::string& raw) {
        if (raw.empty() || raw=="-") return std::nullopt;
        auto it = time_controls.find(raw);
        if (it != time_controls.end()) return it->second;

        // Parse base+increment
        int base=0, inc=0;
        sscanf(raw.c_str(), "%d+%d", &base, &inc);

        std::string sql = "INSERT INTO time_controls(raw,base_secs,increment) VALUES('" +
            raw + "'," + std::to_string(base) + "," + std::to_string(inc) +
            ") ON CONFLICT(raw) DO UPDATE SET raw=EXCLUDED.raw RETURNING id";
        PGresult* res = PQexec(conn, sql.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res); return std::nullopt;
        }
        int16_t id = (int16_t)std::stoi(PQgetvalue(res,0,0));
        PQclear(res);
        time_controls[raw] = id;
        return id;
    }

    std::optional<int16_t> get_opening(const std::string& eco, const std::string& name) {
        if (eco.empty()) return std::nullopt;
        std::string key = eco + "|" + name;
        auto it = openings.find(key);
        if (it != openings.end()) return it->second;

        std::string sql = "INSERT INTO openings(eco,name) VALUES($1,$2) "
                          "ON CONFLICT(eco,name) DO UPDATE SET eco=EXCLUDED.eco RETURNING id";
        const char* params[2] = {eco.c_str(), name.c_str()};
        PGresult* res = PQexecParams(conn, sql.c_str(), 2, nullptr,
                                     params, nullptr, nullptr, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res); return std::nullopt;
        }
        int16_t id = (int16_t)std::stoi(PQgetvalue(res,0,0));
        PQclear(res);
        openings[key] = id;
        return id;
    }

    // Batch-upsert all unique UCI strings in a set of games
    void upsert_moves(const std::vector<std::string>& ucis) {
        std::vector<std::string> new_ucis;
        for (const auto& u : ucis) {
            if (moves.find(u) == moves.end()) new_ucis.push_back(u);
        }
        if (new_ucis.empty()) return;

        // Deduplicate
        std::sort(new_ucis.begin(), new_ucis.end());
        new_ucis.erase(std::unique(new_ucis.begin(), new_ucis.end()), new_ucis.end());

        // Build multi-value INSERT
        std::string sql = "INSERT INTO moves(uci, from_sq, to_sq, promotion) VALUES ";
        bool first = true;
        for (const auto& u : new_ucis) {
            if (!first) sql += ",";
            first = false;
            std::string from = u.substr(0,2);
            std::string to   = u.substr(2,2);
            std::string promo = (u.size()==5) ? std::string(1,u[4]) : "";
            sql += "('" + u + "','" + from + "','" + to + "'," +
                   (promo.empty() ? "NULL" : "'" + promo + "'") + ")";
        }
        sql += " ON CONFLICT(uci) DO NOTHING RETURNING uci, id";

        PGresult* res = PQexec(conn, sql.c_str());
        ExecStatusType upsert_status = PQresultStatus(res);
        if (upsert_status == PGRES_TUPLES_OK) {
            int nr = PQntuples(res);
            for (int i=0; i<nr; i++) {
                std::string u = PQgetvalue(res,i,0);
                int32_t     id = std::stoi(PQgetvalue(res,i,1));
                moves[u] = id;
            }
        } else if (upsert_status != PGRES_COMMAND_OK) {
            std::string err = PQerrorMessage(conn);
            PQclear(res);
            // Print the failing SQL prefix for diagnosis
            std::string sql_prefix = sql.size() > 200 ? sql.substr(0,200)+"..." : sql;
            throw std::runtime_error("upsert_moves INSERT failed: " + err + " SQL: " + sql_prefix);
        }
        PQclear(res);

        // Also fetch any that already existed (conflict = no RETURNING)
        if (!new_ucis.empty()) {
            std::string sel = "SELECT uci, id FROM moves WHERE uci IN (";
            bool f2=true;
            for (const auto& u : new_ucis) {
                if (!f2) sel+=",";
                f2=false;
                sel += "'" + u + "'";
            }
            sel += ")";
            PGresult* r2 = PQexec(conn, sel.c_str());
            if (PQresultStatus(r2) == PGRES_TUPLES_OK) {
                int nr = PQntuples(r2);
                for (int i=0;i<nr;i++) {
                    std::string u = PQgetvalue(r2,i,0);
                    int32_t id = std::stoi(PQgetvalue(r2,i,1));
                    moves[u] = id;
                }
            }
            PQclear(r2);
        }
    }
};

// ── ZST reader + PGN splitter ─────────────────────────────────────────────────
static void reader_thread(const std::string& path,
                          const std::string& resume_after,
                          BoundedQueue<std::string>& raw_q) {
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) {
        g_fatal_msg = "Cannot open file: " + path;
        g_fatal.store(true); g_stop.store(true);
        raw_q.set_done(); return;
    }

    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    static constexpr size_t IN_BUF  = 1 << 22;  // 4MB compressed input
    static constexpr size_t OUT_BUF = 1 << 24;  // 16MB decompressed output
    std::vector<char> in_buf(IN_BUF), out_buf(OUT_BUF);

    // State machine for PGN splitting
    std::string game_lines;
    game_lines.reserve(4096);
    bool in_game = false;
    std::string leftover; // partial line between chunks

    bool resuming = !resume_after.empty();
    bool found_resume = false;

    // Result token detection (word-boundary aware)
    auto has_result = [](const std::string& line) -> bool {
        static const char* results[] = {"1-0","0-1","1/2-1/2","*"};
        for (const char* r : results) {
            size_t p = line.find(r);
            while (p != std::string::npos) {
                // Check left boundary
                bool left_ok = (p==0) || (line[p-1]==' ' || line[p-1]=='\n');
                // Check right boundary
                size_t end = p + strlen(r);
                bool right_ok = (end>=line.size()) || (line[end]==' '||line[end]=='\n'||line[end]=='\r');
                if (left_ok && right_ok) return true;
                p = line.find(r, p+1);
            }
        }
        return false;
    };

    bool skip_next = false;  // skip the first game after resume (it's a fragment)

    auto emit_game = [&]() {
        if (game_lines.empty()) return;
        if (resuming) {
            // Extract lichess_id from Site header
            auto site = game_lines.find("[Site ");
            if (site != std::string::npos) {
                auto sl = game_lines.find("lichess.org/", site);
                if (sl != std::string::npos) {
                    sl += 12;
                    auto end = game_lines.find('"', sl);
                    if (end != std::string::npos) {
                        std::string lid = game_lines.substr(sl, end-sl);
                        if (lid == resume_after) {
                            found_resume = true;
                            resuming = false;
                            skip_next = true; // next emit is the checkpoint game itself — skip it
                        }
                    }
                }
            }
            game_lines.clear();
            in_game = false;
            return;
        }
        if (skip_next) {
            skip_next = false;
            game_lines.clear();
            game_lines.reserve(4096);
            in_game = false;
            return;
        }
        raw_q.push(std::move(game_lines));
        game_lines.clear();
        game_lines.reserve(4096);
        in_game = false;
    };

    auto process_line = [&](const std::string& line) {
        if (line.empty()) return;

        if (line[0] == '[') {
            // New header line — emit prior game only if it has a movetext line.
            // Must check line-by-line: a line not starting with '[' = movetext.
            // (The old char-scan incorrectly matched header content like spaces/quotes.)
            if (in_game && !game_lines.empty()) {
                bool has_movetext = false;
                size_t p = 0;
                while (p < game_lines.size()) {
                    if (game_lines[p] != '[' && game_lines[p] != '\n') {
                        has_movetext = true;
                        break;
                    }
                    while (p < game_lines.size() && game_lines[p] != '\n') p++;
                    if (p < game_lines.size()) p++;
                }
                if (has_movetext) emit_game();
            }
            game_lines += line + '\n';
            in_game = true;
        } else {
            game_lines += line + '\n';
            if (has_result(line)) {
                emit_game();
            }
        }
    };

    // Main decompression loop
    ZSTD_inBuffer  in_pos  = {nullptr, 0, 0};
    size_t bytes_read = 0;

    while (!g_stop.load()) {
        // Refill input if empty
        if (in_pos.pos >= in_pos.size) {
            bytes_read = fread(in_buf.data(), 1, IN_BUF, fp);
            if (bytes_read == 0) break; // EOF
            in_pos = {in_buf.data(), bytes_read, 0};
        }

        ZSTD_outBuffer out_pos = {out_buf.data(), OUT_BUF, 0};
        size_t ret = ZSTD_decompressStream(dctx, &out_pos, &in_pos);

        if (ZSTD_isError(ret)) {
            // Try to continue — some frames may have minor errors
            // Reset decompressor and try next chunk
            ZSTD_DCtx_reset(dctx, ZSTD_reset_session_only);
            continue;
        }

        // Process decompressed output
        const char* data = (const char*)out_buf.data();
        size_t      dlen = out_pos.pos;
        size_t      pos  = 0;

        // Prepend any leftover from last chunk
        if (!leftover.empty()) {
            size_t nl = std::string(data, dlen).find('\n');
            if (nl != std::string::npos) {
                leftover += std::string(data, nl);
                process_line(leftover);
                leftover.clear();
                pos = nl + 1;
            } else {
                leftover += std::string(data, dlen);
                continue;
            }
        }

        // Split remaining output by newlines
        while (pos < dlen) {
            size_t nl = pos;
            while (nl < dlen && data[nl] != '\n') nl++;
            if (nl >= dlen) {
                // Partial line — save for next chunk
                leftover = std::string(data+pos, dlen-pos);
                break;
            }
            std::string line(data+pos, nl-pos);
            // Strip \r
            if (!line.empty() && line.back()=='\r') line.pop_back();
            process_line(line);
            pos = nl + 1;
        }
    }

    // Flush leftover
    if (!leftover.empty()) { process_line(leftover); leftover.clear(); }
    // Flush final game if file ends without blank line
    if (!game_lines.empty() && in_game) emit_game();

    ZSTD_freeDCtx(dctx);
    fclose(fp);
    raw_q.set_done();

    if (resuming && !found_resume && !resume_after.empty()) {
        std::cerr << "WARNING: checkpoint lichess_id '" << resume_after
                  << "' not found in file — importing all games\n";
    }
}

// ── Parser workers ────────────────────────────────────────────────────────────
static void parser_worker(BoundedQueue<std::string>& raw_q,
                          BoundedQueue<GameRow>& parsed_q) {
    std::string raw;
    while (!g_stop.load()) {
        if (!raw_q.pop(raw)) break;
        GameRow g = parse_game(raw);
        if (!g.lichess_id.empty()) {
            parsed_q.push(std::move(g));
        }
    }
}

// ── Writer thread ─────────────────────────────────────────────────────────────
static void writer_thread(const std::string& dsn,
                          int batch_size,
                          BoundedQueue<GameRow>& parsed_q,
                          std::atomic<long long>& games_written,
                          std::atomic<long long>& moves_written) {

    auto connect = [&]() -> PGconn* {
        PGconn* c = PQconnectdb(dsn.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            std::string err = PQerrorMessage(c);
            PQfinish(c);
            throw std::runtime_error("DB connect failed: " + err);
        }
        // Speed settings
        PQexec(c, "SET synchronous_commit = off");
        PQexec(c, "SET work_mem = '256MB'");
        return c;
    };

    PGconn* conn = nullptr;
    try { conn = connect(); }
    catch (const std::exception& e) {
        g_fatal_msg = e.what();
        g_fatal.store(true); g_stop.store(true);
        parsed_q.wake_all(); return;
    }

    DimResolver dims;
    dims.load(conn);

    std::vector<GameRow> batch;
    batch.reserve(batch_size);

    auto flush = [&]() {
        if (batch.empty()) return;

        // ── 1. Collect all unique UCI strings ────────────────────────────────
        std::vector<std::string> all_ucis;
        for (const auto& g : batch)
            for (const auto& m : g.moves)
                all_ucis.push_back(m.uci);

        // ── 2. Resolve dimensions + upsert moves ─────────────────────────────
        // All DB work happens inside a single transaction so any error is
        // caught, rolled back, and re-raised cleanly.

        // ── 3. COPY games ────────────────────────────────────────────────────
        std::vector<std::string> game_rows;
        game_rows.reserve(batch.size());

        // Map lichess_id → resolved game DB id comes back from COPY RETURNING.
        // But COPY doesn't support RETURNING — use INSERT instead for games
        // so we get the IDs back.

        // Build VALUES for a multi-row INSERT with RETURNING
        std::string insert_games =
            "INSERT INTO games(lichess_id,white_id,black_id,"
            "white_elo,black_elo,white_rating_diff,black_rating_diff,"
            "result,termination,utc_date,utc_time,"
            "time_control_id,opening_id,event_type,speed,movetext) VALUES ";

        bool first_game = true;
        std::vector<const GameRow*> valid_games;

        for (const auto& g : batch) {
            if (g.lichess_id.empty()) continue;

            int white_id, black_id;
            try {
                white_id = dims.get_player(g.white);
                black_id = dims.get_player(g.black);
            } catch (...) { continue; }

            auto tc_id  = dims.get_tc(g.tc_raw);
            auto op_id  = dims.get_opening(g.eco, g.opening);

            if (!first_game) insert_games += ",";
            first_game = false;

            insert_games += "(";
            // Helper lambdas for SQL literal building
            auto sq_str = [](const std::string& s) -> std::string {
                std::string out = "'";
                for (char c : s) { if (c=='\'') out+="''"; else out+=c; }
                out += "'";
                return out;
            };
            auto sq_opt_int = [](const std::optional<int>& v) -> std::string {
                return v ? std::to_string(*v) : "NULL";
            };
            auto sq_opt_i16 = [](const std::optional<int16_t>& v) -> std::string {
                return v ? std::to_string(*v) : "NULL";
            };

            insert_games += sq_str(g.lichess_id) + ",";
            insert_games += std::to_string(white_id) + ",";
            insert_games += std::to_string(black_id) + ",";
            insert_games += sq_opt_int(g.white_elo) + ",";
            insert_games += sq_opt_int(g.black_elo) + ",";
            insert_games += sq_opt_int(g.white_diff) + ",";
            insert_games += sq_opt_int(g.black_diff) + ",";
            insert_games += sq_str(g.result) + ",";
            insert_games += sq_str(g.termination) + ",";
            insert_games += (g.utc_date.empty() ? "NULL" : sq_str(g.utc_date)) + ",";
            insert_games += (g.utc_time.empty() ? "NULL" : sq_str(g.utc_time)) + ",";
            insert_games += sq_opt_i16(tc_id) + ",";
            insert_games += sq_opt_i16(op_id) + ",";
            insert_games += sq_str(g.event_type) + ",";
            insert_games += sq_str(g.speed) + ",";
            insert_games += sq_str(g.movetext);
            insert_games += ")";

            valid_games.push_back(&g);
        }

        if (valid_games.empty()) { batch.clear(); return; }

        insert_games += " ON CONFLICT(lichess_id) DO NOTHING RETURNING lichess_id, id";

        exec_sql(conn, "BEGIN");

        // Upsert moves inside the transaction so any error rolls back cleanly
        dims.upsert_moves(all_ucis);

        PGresult* res = PQexec(conn, insert_games.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::string err = PQerrorMessage(conn);
            PQclear(res);
            PQexec(conn, "ROLLBACK");
            throw std::runtime_error("games INSERT failed: " + err);
        }

        // Build lichess_id → DB id map.
        // CHAR(8) pads with spaces in RETURNING — strip them to match the
        // unpadded lichess_id stored in GameRow.
        std::unordered_map<std::string,long long> id_map;
        int nr = PQntuples(res);
        for (int i=0; i<nr; i++) {
            std::string lid = PQgetvalue(res,i,0);
            // rtrim
            while (!lid.empty() && lid.back()==' ') lid.pop_back();
            id_map[lid] = std::stoll(PQgetvalue(res,i,1));
        }
        PQclear(res);

        // ── 4. COPY game_moves ────────────────────────────────────────────────
        std::vector<std::string> move_rows;
        long long move_count = 0;

        for (const auto* gp : valid_games) {
            auto it = id_map.find(gp->lichess_id);
            if (it == id_map.end()) continue; // already existed (idempotent)
            long long gid = it->second;

            for (const auto& m : gp->moves) {
                auto mit = dims.moves.find(m.uci);
                if (mit == dims.moves.end()) continue;
                if (m.ply > 2147483647) continue;

                std::string row;
                row.reserve(64);
                row += std::to_string(gid) + "\t";
                row += std::to_string(m.ply) + "\t";
                row += std::to_string(mit->second) + "\t";
                row += std::to_string(m.pos_hash) + "\t";
                if (m.clock_secs) row += std::to_string(*m.clock_secs);
                else row += "\\N";
                row += "\t";
                if (m.eval_cp) row += std::to_string(*m.eval_cp);
                else row += "\\N";
                row += "\t";
                if (m.eval_mate) row += std::to_string(*m.eval_mate);
                else row += "\\N";
                row += "\t";
                row += m.piece_moved;
                row += "\t";
                row += std::to_string(m.material);
                row += "\n";
                move_rows.push_back(std::move(row));
                move_count++;
            }
        }

        if (!move_rows.empty()) {
            copy_rows(conn,
                "COPY game_moves(game_id,ply,move_id,position_hash,"
                "clock_secs,eval_cp,eval_mate,piece_moved,material) FROM STDIN",
                move_rows);
        }

        // ── 5. game_tags ────────────────────────────────────────────────────
        std::vector<std::string> tag_rows;
        for (const auto* gp : valid_games) {
            auto it = id_map.find(gp->lichess_id);
            if (it == id_map.end()) continue;
            long long gid = it->second;
            for (const auto& [k,v] : gp->extra_tags) {
                std::string row = std::to_string(gid) + "\t" +
                                  esc_copy(k) + "\t" + esc_copy(v) + "\n";
                tag_rows.push_back(std::move(row));
            }
        }
        if (!tag_rows.empty()) {
            copy_rows(conn,
                "COPY game_tags(game_id,key,value) FROM STDIN",
                tag_rows);
        }

        exec_sql(conn, "COMMIT");

        // ── 6. Checkpoint ────────────────────────────────────────────────────
        long long total = games_written.fetch_add((long long)nr) + nr;
        moves_written.fetch_add(move_count);

        // Write checkpoint: last lichess_id in this batch
        if (!valid_games.empty()) {
            write_checkpoint(valid_games.back()->lichess_id, total);
        }

        batch.clear();
    }; // end flush lambda

    // ── Main write loop ───────────────────────────────────────────────────────
    GameRow g;
    while (!g_stop.load()) {
        if (!parsed_q.pop(g)) break;
        batch.push_back(std::move(g));
        if ((int)batch.size() >= batch_size) {
            try { flush(); }
            catch (const std::exception& e) {
                std::cerr << "\nFATAL: write failed: " << e.what() << "\n";
                g_fatal_msg = e.what();
                g_fatal.store(true); g_stop.store(true);
                PQfinish(conn);
                parsed_q.wake_all();
                return;
            }
        }
    }

    // Flush remaining
    if (!g_stop.load() || !g_fatal.load()) {
        try { flush(); }
        catch (const std::exception& e) {
            std::cerr << "\nFATAL: final flush failed: " << e.what() << "\n";
            g_fatal_msg = e.what();
            g_fatal.store(true);
        }
    }

    PQfinish(conn);
}

// ── Progress display ──────────────────────────────────────────────────────────
static void progress_thread(const std::atomic<long long>& games,
                             const std::atomic<long long>& moves,
                             long long target) {
    auto t_start = std::chrono::steady_clock::now();
    long long prev_games = 0;
    auto prev_time = t_start;

    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(15));
        if (g_stop.load()) break;

        auto now = std::chrono::steady_clock::now();
        long long g_now = games.load();
        long long m_now = moves.load();

        double elapsed  = std::chrono::duration<double>(now - t_start).count();
        double interval = std::chrono::duration<double>(now - prev_time).count();
        double rate = (interval > 0) ? (g_now - prev_games) / interval * 60.0 : 0;

        prev_games = g_now;
        prev_time  = now;

        double pct = (target > 0) ? (100.0 * g_now / target) : 0;
        int bar_filled = (int)(pct / 5);
        std::string bar(std::min(bar_filled,20), '\xe2'); // rough Unicode fill
        bar = "";
        for (int i=0;i<20;i++) bar += (i < bar_filled ? "█" : "░");

        double eta_secs = (rate > 0 && target > 0)
                          ? (target - g_now) / (rate / 60.0) : 0;
        int    eta_h  = (int)eta_secs / 3600;
        int    eta_m  = ((int)eta_secs % 3600) / 60;
        int    eta_s  = (int)eta_secs % 60;

        int    el_h   = (int)elapsed / 3600;
        int    el_m   = ((int)elapsed % 3600) / 60;
        int    el_s   = (int)elapsed % 60;

        fprintf(stderr,
            "\r\033[K"
            "  [%s] %.1f%%  "
            "Games: %lld/%lld  "
            "Moves: %lld  "
            "Rate: %.0f/min  "
            "Elapsed: %02d:%02d:%02d  "
            "ETA: %02d:%02d:%02d",
            bar.c_str(), pct,
            g_now, target,
            m_now,
            rate,
            el_h, el_m, el_s,
            eta_h, eta_m, eta_s);
        fflush(stderr);
    }
    fprintf(stderr, "\n");
}

// ── CLI argument parsing ──────────────────────────────────────────────────────
struct Args {
    std::string file;
    std::string dsn;
    int parsers  = 4;
    int batch    = 50000;
    bool resume  = true;
    bool verify  = false;
    int  verify_n = 1000;
    long long target = 0; // estimated total games (for progress bar)
};

static Args parse_args(int argc, char** argv) {
    Args a;
    for (int i=1; i<argc; i++) {
        std::string arg = argv[i];
        if      (arg=="--file"    && i+1<argc) a.file     = argv[++i];
        else if (arg=="--dsn"     && i+1<argc) a.dsn      = argv[++i];
        else if (arg=="--parsers" && i+1<argc) a.parsers  = std::stoi(argv[++i]);
        else if (arg=="--batch"   && i+1<argc) a.batch    = std::stoi(argv[++i]);
        else if (arg=="--target"  && i+1<argc) a.target   = std::stoll(argv[++i]);
        else if (arg=="--no-resume")           a.resume   = false;
        else if (arg=="--verify")              a.verify   = true;
        else if (arg=="--verify-n" && i+1<argc) a.verify_n = std::stoi(argv[++i]);
    }
    return a;
}

// ── Verify mode ───────────────────────────────────────────────────────────────
// Parses first N games and dumps JSON for comparison with python-chess
static void run_verify(const std::string& path, int n) {
    BoundedQueue<std::string> raw_q(64);
    std::thread reader([&]{
        reader_thread(path, "", raw_q);
    });

    int count = 0;
    std::cout << "[\n";
    std::string raw;
    while (count < n && raw_q.pop(raw)) {
        GameRow g = parse_game(raw);
        if (g.lichess_id.empty()) continue;
        std::cout << "  {\"id\":\"" << g.lichess_id
                  << "\",\"moves\":[\n";
        for (size_t i=0; i<g.moves.size(); i++) {
            const auto& m = g.moves[i];
            std::cout << "    {\"ply\":" << m.ply
                      << ",\"uci\":\"" << m.uci << "\""
                      << ",\"hash\":" << m.pos_hash << "}";
            if (i+1 < g.moves.size()) std::cout << ",";
            std::cout << "\n";
        }
        std::cout << "  ]}";
        if (count+1 < n) std::cout << ",";
        std::cout << "\n";
        count++;
    }
    std::cout << "]\n";

    g_stop.store(true);
    raw_q.set_done();
    reader.join();
}

// ── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    Args args = parse_args(argc, argv);

    if (args.file.empty() || args.dsn.empty()) {
        std::cerr << "Usage: lichess_import --file db.pgn.zst --dsn <dsn> "
                     "[--parsers N] [--batch N] [--target N] [--no-resume] [--verify]\n";
        return 1;
    }

    if (args.verify) {
        run_verify(args.file, args.verify_n);
        return 0;
    }

    // Estimate target from file size if not given
    if (args.target == 0) {
        struct stat st;
        if (stat(args.file.c_str(), &st) == 0)
            args.target = st.st_size / 322; // ~322 bytes/game compressed
    }

    // Resume checkpoint — but only if the DB actually has data.
    // A stale checkpoint with an empty DB causes the reader to skip the entire
    // file searching for a lichess_id that will never match.
    std::string resume_after;
    long long   resume_games = 0;
    if (args.resume) {
        resume_after = read_checkpoint();
        if (!resume_after.empty()) {
            // Verify DB has games — if empty, checkpoint is stale
            PGconn* chk = PQconnectdb(args.dsn.c_str());
            bool db_empty = true;
            if (PQstatus(chk) == CONNECTION_OK) {
                PGresult* r = PQexec(chk, "SELECT COUNT(*) FROM games");
                if (PQresultStatus(r) == PGRES_TUPLES_OK) {
                    long long cnt = std::stoll(PQgetvalue(r,0,0));
                    db_empty = (cnt == 0);
                }
                PQclear(r);
            }
            PQfinish(chk);

            if (db_empty) {
                std::cerr << "WARNING: stale checkpoint (DB is empty) — ignoring checkpoint and starting fresh\n";
                std::remove(checkpoint_path().c_str());
                resume_after.clear();
            } else {
                // Read game count from checkpoint
                std::ifstream f(checkpoint_path());
                std::string l1; std::getline(f,l1);
                std::string l2; if (std::getline(f,l2)) {
                    try { resume_games = std::stoll(l2); } catch(...) {}
                }
                std::cerr << "Resuming after lichess_id=" << resume_after
                          << " (" << resume_games << " games already imported)\n";
            }
        }
    }

    // Queues
    BoundedQueue<std::string> raw_q(args.parsers * 8);
    BoundedQueue<GameRow>     parsed_q(args.parsers * 4);

    std::atomic<long long> games_written{resume_games};
    std::atomic<long long> moves_written{0};

    // Threads
    std::thread reader([&]{
        reader_thread(args.file, resume_after, raw_q);
    });

    std::vector<std::thread> parsers;
    parsers.reserve(args.parsers);
    for (int i=0; i<args.parsers; i++) {
        parsers.emplace_back([&]{
            parser_worker(raw_q, parsed_q);
        });
    }

    std::thread writer([&]{
        writer_thread(args.dsn, args.batch, parsed_q, games_written, moves_written);
    });

    std::thread progress([&]{
        progress_thread(games_written, moves_written, args.target);
    });

    // Wait for pipeline
    reader.join();
    raw_q.set_done();

    for (auto& p : parsers) p.join();
    parsed_q.set_done();

    writer.join();
    g_stop.store(true); // stop progress thread
    progress.join();

    if (g_fatal.load()) {
        std::cerr << "FATAL: " << g_fatal_msg << "\n";
        return 1;
    }

    std::cerr << "\nDone. " << games_written.load() << " games, "
              << moves_written.load() << " moves.\n";

    // Clear checkpoint on success
    remove(checkpoint_path().c_str());
    return 0;
}
