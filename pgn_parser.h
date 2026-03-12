#pragma once
#include "board.h"
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// ── Data structures ───────────────────────────────────────────────────────────

struct MoveRow {
    int     ply;
    std::string uci;
    int64_t pos_hash;
    std::optional<int> clock_secs;
    std::optional<int> eval_cp;
    std::optional<int> eval_mate;
    char    piece_moved = '?'; // P N B R Q K  — piece that moved (pawn for promotions)
    int64_t material    = 0;   // both sides packed as nibbles; see Board::material_packed()
};

struct GameRow {
    std::string lichess_id;
    std::string white, black;
    std::optional<int> white_elo, black_elo;
    std::optional<int> white_diff, black_diff;
    std::string result;
    std::string termination;
    std::string utc_date;
    std::string utc_time;
    std::string tc_raw;
    std::string eco;
    std::string opening;
    std::string event_type;
    std::string speed;
    std::string movetext;
    std::vector<MoveRow> moves;
    std::unordered_map<std::string,std::string> extra_tags;
    bool parse_error = false;
};

static inline bool is_known_tag(const std::string& k) {
    static const std::unordered_map<std::string,bool> known = {
        {"Event",1},{"Site",1},{"Date",1},{"Round",1},
        {"White",1},{"Black",1},{"Result",1},
        {"WhiteElo",1},{"BlackElo",1},
        {"WhiteRatingDiff",1},{"BlackRatingDiff",1},
        {"UTCDate",1},{"UTCTime",1},{"TimeControl",1},
        {"ECO",1},{"Opening",1},{"Termination",1},{"Annotator",1}
    };
    return known.count(k) > 0;
}

static inline std::optional<int> try_int(const std::string& s) {
    if (s.empty() || s=="?" || s=="-") return std::nullopt;
    try { return std::stoi(s); } catch(...) { return std::nullopt; }
}

static inline std::optional<int> extract_clock(const std::string& cmt) {
    auto p = cmt.find("%clk");
    if (p == std::string::npos) return std::nullopt;
    p += 4;
    while (p < cmt.size() && cmt[p]==' ') p++;
    int h=0,m=0; double s=0;
    if (sscanf(cmt.c_str()+p, "%d:%d:%lf",&h,&m,&s) >= 2)
        return h*3600 + m*60 + (int)s;
    return std::nullopt;
}

static inline std::pair<std::optional<int>,std::optional<int>>
extract_eval(const std::string& cmt) {
    auto p = cmt.find("%eval");
    if (p == std::string::npos) return {std::nullopt,std::nullopt};
    p += 5;
    while (p < cmt.size() && cmt[p]==' ') p++;
    if (p >= cmt.size()) return {std::nullopt,std::nullopt};
    if (cmt[p]=='#') {
        int mv=0;
        if (sscanf(cmt.c_str()+p+1,"%d",&mv)==1)
            return {std::nullopt, std::max(-32767,std::min(32767,mv))};
        return {std::nullopt,std::nullopt};
    }
    double v=0;
    if (sscanf(cmt.c_str()+p,"%lf",&v)==1) {
        int cp=(int)(v*100);
        return {std::max(-32767,std::min(32767,cp)), std::nullopt};
    }
    return {std::nullopt,std::nullopt};
}

static inline std::string extract_speed(const std::string& event) {
    std::string low = event;
    for (auto& c : low) c = (char)tolower((unsigned char)c);
    if (low.find("ultrabullet") != std::string::npos) return "ultrabullet";
    if (low.find("bullet")      != std::string::npos) return "bullet";
    if (low.find("blitz")       != std::string::npos) return "blitz";
    if (low.find("rapid")       != std::string::npos) return "rapid";
    if (low.find("classical")   != std::string::npos) return "classical";
    if (low.find("correspondence") != std::string::npos) return "correspondence";
    return "";
}

// ── Main parser ───────────────────────────────────────────────────────────────

inline GameRow parse_game(const std::string& raw, bool parse_moves_flag = true) {
    GameRow g;
    std::istringstream ss(raw);
    std::string line;
    std::string movetext;

    // ── Phase 1: headers ──────────────────────────────────────────────────────
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back()=='\r') line.pop_back();
        if (line.empty()) { if (!movetext.empty()) movetext += '\n'; continue; }

        if (line[0]=='[') {
            auto ks = line.find(' ');
            auto vs = line.find('"');
            auto ve = line.rfind('"');
            if (ks==std::string::npos || vs==std::string::npos || vs==ve) continue;
            std::string key   = line.substr(1, ks-1);
            std::string value = line.substr(vs+1, ve-vs-1);

            if      (key=="Site")             g.lichess_id  = value.substr(value.rfind('/')+1);
            else if (key=="White")            g.white       = value;
            else if (key=="Black")            g.black       = value;
            else if (key=="Result")           g.result      = value;
            else if (key=="WhiteElo")         g.white_elo   = try_int(value);
            else if (key=="BlackElo")         g.black_elo   = try_int(value);
            else if (key=="WhiteRatingDiff")  g.white_diff  = try_int(value);
            else if (key=="BlackRatingDiff")  g.black_diff  = try_int(value);
            else if (key=="UTCDate")          g.utc_date    = value;
            else if (key=="UTCTime")          g.utc_time    = value;
            else if (key=="TimeControl")      g.tc_raw      = value;
            else if (key=="ECO")              g.eco         = value;
            else if (key=="Opening")          g.opening     = value;
            else if (key=="Termination")      g.termination = value;
            else if (key=="Event") { g.event_type = value; g.speed = extract_speed(value); }
            else if (key=="FEN")  { parse_moves_flag = false; } // non-standard start
            else if (!is_known_tag(key)) g.extra_tags[key] = value;
        } else {
            movetext += line + '\n';
        }
    }

    g.movetext = movetext;
    if (!parse_moves_flag || movetext.empty()) return g;

    // ── Phase 2: tokenise movetext ────────────────────────────────────────────
    struct TC { std::string san; std::string comment; };
    std::vector<TC> tcs;

    {
        const std::string& mt = movetext;
        size_t i = 0;
        while (i < mt.size()) {
            char c = mt[i];
            if (c == '{') {
                size_t end = mt.find('}', i);
                if (end == std::string::npos) break;
                std::string cmt = mt.substr(i+1, end-i-1);
                if (!tcs.empty() && tcs.back().comment.empty())
                    tcs.back().comment = cmt;
                i = end+1; continue;
            }
            if (c == '(') {
                int depth=1; i++;
                while (i < mt.size() && depth>0) {
                    if (mt[i]=='(') depth++;
                    else if (mt[i]==')') depth--;
                    i++;
                }
                continue;
            }
            if (std::isspace((unsigned char)c)) { i++; continue; }

            size_t start = i;
            while (i < mt.size() && !std::isspace((unsigned char)mt[i])
                   && mt[i]!='{' && mt[i]!='(') i++;
            std::string tok = mt.substr(start, i-start);

            // Skip move numbers
            if (!tok.empty() && std::isdigit((unsigned char)tok[0])) continue;
            // Skip result tokens
            if (tok=="1-0"||tok=="0-1"||tok=="1/2-1/2"||tok=="*") continue;
            // Strip markers
            while (!tok.empty() && (tok.back()=='+'||tok.back()=='#'||
                                     tok.back()=='!'||tok.back()=='?'))
                tok.pop_back();
            if (!tok.empty()) tcs.push_back({tok, ""});
        }
    }

    // ── Phase 3: board walk → UCI + Zobrist ──────────────────────────────────
    Board board = Board::start_pos();
    int ply = 0;

    for (auto& tc : tcs) {
        if (ply >= 50000) break;
        ply++;

        int64_t hash_before = board.zobrist_i64();
        std::string uci = board.san_to_uci(tc.san);

        if (uci.empty()) { g.parse_error = true; break; }

        char    piece_moved = piece_to_char(board.sq[parse_sq(uci[0], uci[1])]);
        int64_t mat         = board.material_packed();

        auto [eval_cp, eval_mate] = extract_eval(tc.comment);
        auto clock = extract_clock(tc.comment);

        g.moves.push_back({ply, uci, hash_before, clock, eval_cp, eval_mate,
                           piece_moved, mat});

        if (!board.apply_uci(uci)) { g.parse_error = true; break; }
    }

    return g;
}
