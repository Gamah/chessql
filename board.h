#pragma once
#include "zobrist.h"
#include <array>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

// ── Piece enum ────────────────────────────────────────────────────────────────

enum Piece : uint8_t {
    EMPTY=0,
    wP=1, wN=2, wB=3, wR=4, wQ=5, wK=6,
    bP=7, bN=8, bB=9, bR=10, bQ=11, bK=12
};

inline bool is_white (Piece p) { return p >= wP && p <= wK; }
inline bool is_black (Piece p) { return p >= bP; }
inline bool is_pawn  (Piece p) { return p == wP || p == bP; }
inline bool is_rook  (Piece p) { return p == wR || p == bR; }
inline bool is_king  (Piece p) { return p == wK || p == bK; }

// Maps any piece to its type letter (colour-agnostic).
inline char piece_to_char(Piece p) {
    switch (p) {
        case wP: case bP: return 'P';
        case wN: case bN: return 'N';
        case wB: case bB: return 'B';
        case wR: case bR: return 'R';
        case wQ: case bQ: return 'Q';
        case wK: case bK: return 'K';
        default:          return '?';
    }
}

inline Piece color_piece(bool white, int type) {
    return white ? (Piece)type : (Piece)(type + 6);
}

// ── Square helpers  (use S() not sq() to avoid clash with Board::sq) ─────────
// s: a1=0, b1=1, ..., h1=7, a2=8, ..., h8=63

inline int  S(int file, int rank)  { return rank * 8 + file; }
inline int  file_of(int s)         { return s % 8; }
inline int  rank_of(int s)         { return s / 8; }
inline bool on_board(int f, int r) { return f>=0 && f<8 && r>=0 && r<8; }

inline std::string sq_name(int s) {
    return {(char)('a' + file_of(s)), (char)('1' + rank_of(s))};
}
inline int parse_sq(char f, char r) { return S(f-'a', r-'1'); }
inline int parse_sq(const char* s)  { return parse_sq(s[0], s[1]); }

// ── Board ─────────────────────────────────────────────────────────────────────

struct Board {
    std::array<Piece,64> sq{};   // indexed by S(file,rank)
    bool  white_to_move = true;
    bool  castle_wK = false, castle_wQ = false;
    bool  castle_bK = false, castle_bQ = false;
    int   ep_sq = -1;            // en-passant target square (-1 = none)

    static Board start_pos() {
        Board b;
        const Piece back[8]  = {wR,wN,wB,wQ,wK,wB,wN,wR};
        const Piece bback[8] = {bR,bN,bB,bQ,bK,bB,bN,bR};
        for (int f=0;f<8;f++) {
            b.sq[S(f,0)]=back[f];  b.sq[S(f,1)]=wP;
            b.sq[S(f,7)]=bback[f]; b.sq[S(f,6)]=bP;
        }
        b.castle_wK=b.castle_wQ=b.castle_bK=b.castle_bQ=true;
        return b;
    }

    // Zobrist hash — matches python-chess / Polyglot exactly
    uint64_t zobrist_u64() const {
        uint64_t h = 0;
        for (int s=0; s<64; s++) {
            if (sq[s] == EMPTY) continue;
            uint64_t pi = polyglot_piece_index(sq[s]);
            h ^= POLYGLOT_RANDOM_ARRAY[64*pi + (uint64_t)s];
        }
        if (castle_wK) h ^= POLYGLOT_RANDOM_ARRAY[768];
        if (castle_wQ) h ^= POLYGLOT_RANDOM_ARRAY[769];
        if (castle_bK) h ^= POLYGLOT_RANDOM_ARRAY[770];
        if (castle_bQ) h ^= POLYGLOT_RANDOM_ARRAY[771];
        if (ep_sq >= 0) h ^= POLYGLOT_RANDOM_ARRAY[772 + file_of(ep_sq)];
        if (!white_to_move) h ^= POLYGLOT_RANDOM_ARRAY[780];
        return h;
    }

    int64_t zobrist_i64() const {
        uint64_t u = zobrist_u64();
        int64_t  s2;
        std::memcpy(&s2, &u, sizeof s2);
        return s2;
    }

    // Apply UCI move (e.g. "e2e4", "e1g1", "e7e8q").
    // Trusts that the move is legal (Lichess games are pre-validated).
    bool apply_uci(const std::string& uci) {
        if (uci.size() < 4) return false;
        int from = parse_sq(uci[0], uci[1]);
        int to   = parse_sq(uci[2], uci[3]);
        if (from < 0 || from > 63 || to < 0 || to > 63) return false;

        Piece moving = sq[from];
        if (moving == EMPTY) return false;

        bool white = white_to_move;

        Piece promo = EMPTY;
        if (uci.size() == 5) {
            char c = uci[4];
            if (white) promo = (c=='q'?wQ:c=='r'?wR:c=='b'?wB:wN);
            else       promo = (c=='q'?bQ:c=='r'?bR:c=='b'?bB:bN);
        }

        int prev_ep = ep_sq;
        ep_sq = -1;

        // ── Castling ─────────────────────────────────────────────────────────
        if (moving == wK) {
            castle_wK = castle_wQ = false;
            // Find rooks on rank 1 for Chess960 support
            int rook_ks = -1, rook_qs = -1;
            int king_file = file_of(from);
            for (int f=0; f<8; f++) {
                if (sq[S(f,0)] == wR) {
                    if (f > king_file) rook_ks = S(f,0);
                    else               rook_qs = S(f,0);
                }
            }
            int to_file = file_of(to);
            bool ks = (to_file == 6) || (rook_ks >= 0 && to == rook_ks);
            bool qs = (to_file == 2) || (rook_qs >= 0 && to == rook_qs);
            if (ks && rank_of(from)==0) {
                int rook_from = (rook_ks >= 0) ? rook_ks : S(7,0);
                sq[from] = sq[rook_from] = EMPTY;
                sq[S(6,0)] = wK; sq[S(5,0)] = wR;
                white_to_move = false; return true;
            }
            if (qs && rank_of(from)==0) {
                int rook_from = (rook_qs >= 0) ? rook_qs : S(0,0);
                sq[from] = sq[rook_from] = EMPTY;
                sq[S(2,0)] = wK; sq[S(3,0)] = wR;
                white_to_move = false; return true;
            }
        }
        if (moving == bK) {
            castle_bK = castle_bQ = false;
            int rook_ks = -1, rook_qs = -1;
            int king_file = file_of(from);
            for (int f=0; f<8; f++) {
                if (sq[S(f,7)] == bR) {
                    if (f > king_file) rook_ks = S(f,7);
                    else               rook_qs = S(f,7);
                }
            }
            int to_file = file_of(to);
            bool ks = (to_file == 6) || (rook_ks >= 0 && to == rook_ks);
            bool qs = (to_file == 2) || (rook_qs >= 0 && to == rook_qs);
            if (ks && rank_of(from)==7) {
                int rook_from = (rook_ks >= 0) ? rook_ks : S(7,7);
                sq[from] = sq[rook_from] = EMPTY;
                sq[S(6,7)] = bK; sq[S(5,7)] = bR;
                white_to_move = true; return true;
            }
            if (qs && rank_of(from)==7) {
                int rook_from = (rook_qs >= 0) ? rook_qs : S(0,7);
                sq[from] = sq[rook_from] = EMPTY;
                sq[S(2,7)] = bK; sq[S(3,7)] = bR;
                white_to_move = true; return true;
            }
        }

        // Castling rights lost on rook/king moves
        if (from == S(7,0) || to == S(7,0)) castle_wK = false;
        if (from == S(0,0) || to == S(0,0)) castle_wQ = false;
        if (from == S(7,7) || to == S(7,7)) castle_bK = false;
        if (from == S(0,7) || to == S(0,7)) castle_bQ = false;

        // En passant capture
        if (is_pawn(moving) && to == prev_ep && prev_ep >= 0) {
            int cap = white ? to - 8 : to + 8;
            sq[cap] = EMPTY;
        }

        // Double pawn push → set EP square
        if (moving == wP && rank_of(from)==1 && rank_of(to)==3) ep_sq = from + 8;
        if (moving == bP && rank_of(from)==6 && rank_of(to)==4) ep_sq = from - 8;

        sq[to]   = (promo != EMPTY) ? promo : moving;
        sq[from] = EMPTY;
        white_to_move = !white;
        return true;
    }

    // Packs both sides' non-king piece counts into a BIGINT.
    // Each count occupies 4 bits (nibble), allowing values 0-15.
    // Kings are excluded — they are always present and never captured.
    //
    // Bit layout (one nibble per piece type, 10 nibbles = 40 bits total):
    //
    //   bits  0- 3  white pawns    (wP >> 0)  & 0xF
    //   bits  4- 7  white knights  (wN >> 4)  & 0xF
    //   bits  8-11  white bishops  (wB >> 8)  & 0xF
    //   bits 12-15  white rooks    (wR >> 12) & 0xF
    //   bits 16-19  white queens   (wQ >> 16) & 0xF
    //   bits 20-23  black pawns    (bP >> 20) & 0xF
    //   bits 24-27  black knights  (bN >> 24) & 0xF
    //   bits 28-31  black bishops  (bB >> 28) & 0xF
    //   bits 32-35  black rooks    (bR >> 32) & 0xF
    //   bits 36-39  black queens   (bQ >> 36) & 0xF
    //
    // Starting position value:
    //   wP=8<<0 | wN=2<<4 | wB=2<<8 | wR=2<<12 | wQ=1<<16
    //   bP=8<<20| bN=2<<24| bB=2<<28| bR=2<<32 | bQ=1<<36
    //   = 0x0000_1222_8_1222_8  (little-endian nibble view)
    //
    // Example SQL extractions:
    //   white queens:  (material >> 16) & 15
    //   black knights: (material >> 24) & 15
    //   white pawns:    material        & 15
    int64_t material_packed() const {
        int wP=0, wN=0, wB=0, wR=0, wQ=0;
        int bP=0, bN=0, bB=0, bR=0, bQ=0;
        for (int s = 0; s < 64; s++) {
            switch (sq[s]) {
                case ::wP: wP++; break; case ::wN: wN++; break;
                case ::wB: wB++; break; case ::wR: wR++; break;
                case ::wQ: wQ++; break;
                case ::bP: bP++; break; case ::bN: bN++; break;
                case ::bB: bB++; break; case ::bR: bR++; break;
                case ::bQ: bQ++; break;
                default: break;
            }
        }
        // Verify shift offsets match the bit layout documented above:
        //   wP@ 0, wN@ 4, wB@ 8, wR@12, wQ@16,
        //   bP@20, bN@24, bB@28, bR@32, bQ@36
        return  (int64_t)wP
            | ((int64_t)wN <<  4) | ((int64_t)wB <<  8)
            | ((int64_t)wR << 12) | ((int64_t)wQ << 16)
            | ((int64_t)bP << 20) | ((int64_t)bN << 24)
            | ((int64_t)bB << 28) | ((int64_t)bR << 32)
            | ((int64_t)bQ << 36);
    }

    // SAN → UCI conversion.
    // Parses destination + piece type from SAN, scans board for source square.
    // Returns "" on failure.
    std::string san_to_uci(const std::string& san) const {
        if (san.empty()) return "";

        // Castling
        if (san == "O-O" || san == "0-0") {
            int r = white_to_move ? 0 : 7;
            return sq_name(S(4,r)) + sq_name(S(6,r));
        }
        if (san == "O-O-O" || san == "0-0-0") {
            int r = white_to_move ? 0 : 7;
            return sq_name(S(4,r)) + sq_name(S(2,r));
        }

        std::string s = san;
        // Strip trailing markers
        while (!s.empty() && (s.back()=='+' || s.back()=='#' ||
                               s.back()=='!' || s.back()=='?'))
            s.pop_back();

        // Promotion: e7e8=Q
        std::string promo_str;
        auto eq = s.find('=');
        if (eq != std::string::npos && eq+1 < s.size()) {
            promo_str = std::string(1, (char)tolower(s[eq+1]));
            s = s.substr(0, eq);
        }
        if (s.size() < 2) return "";

        // Piece type
        bool is_piece = std::isupper((unsigned char)s[0]) && s[0] != 'O';
        int piece_type = 1; // pawn default
        if (is_piece) {
            switch(s[0]) {
                case 'N': piece_type=2; break;
                case 'B': piece_type=3; break;
                case 'R': piece_type=4; break;
                case 'Q': piece_type=5; break;
                case 'K': piece_type=6; break;
            }
            s = s.substr(1);
        }

        // Remove capture marker
        std::string s2;
        for (char c : s) if (c != 'x') s2 += c;
        s = s2;

        if (s.size() < 2) return "";
        char dest_f = s[s.size()-2];
        char dest_r = s[s.size()-1];
        if (dest_f < 'a' || dest_f > 'h') return "";
        if (dest_r < '1' || dest_r > '8') return "";
        int to_sq = parse_sq(dest_f, dest_r);

        std::string disambig = s.substr(0, s.size()-2);

        Piece target = color_piece(white_to_move, piece_type);
        std::vector<int> cands;
        for (int s3=0; s3<64; s3++) {
            if (sq[s3] != target) continue;
            if (can_reach(s3, to_sq, piece_type)) cands.push_back(s3);
        }
        if (cands.empty()) return "";

        int from_sq = -1;
        if (cands.size() == 1) {
            from_sq = cands[0];
        } else {
            for (int c : cands) {
                bool match = true;
                for (char d : disambig) {
                    if (d >= 'a' && d <= 'h' && file_of(c) != d-'a') match=false;
                    if (d >= '1' && d <= '8' && rank_of(c) != d-'1') match=false;
                }
                if (match) { from_sq = c; break; }
            }
            if (from_sq < 0) from_sq = cands[0];
        }
        if (from_sq < 0) return "";

        return sq_name(from_sq) + sq_name(to_sq) + promo_str;
    }

private:
    bool can_reach(int from, int to_sq, int piece_type) const {
        int df = file_of(to_sq) - file_of(from);
        int dr = rank_of(to_sq) - rank_of(from);
        int adf = std::abs(df), adr = std::abs(dr);

        switch (piece_type) {
        case 1: { // Pawn
            int dir = white_to_move ? 1 : -1;
            int start_r = white_to_move ? 1 : 6;
            if (adf==1 && dr==dir)
                return (to_sq == ep_sq) || (sq[to_sq] != EMPTY);
            if (df != 0) return false;
            if (dr == dir && sq[to_sq] == EMPTY) return true;
            if (dr == 2*dir && rank_of(from)==start_r &&
                sq[to_sq]==EMPTY && sq[from+8*dir]==EMPTY) return true;
            return false;
        }
        case 2: return (adf==1&&adr==2)||(adf==2&&adr==1);
        case 3: return (adf==adr) && ray_clear(from, to_sq, df>0?1:-1, dr>0?1:-1);
        case 4:
            if (df==0) return ray_clear(from, to_sq, 0, dr>0?1:-1);
            if (dr==0) return ray_clear(from, to_sq, df>0?1:-1, 0);
            return false;
        case 5:
            if (adf==adr) return ray_clear(from, to_sq, df>0?1:-1, dr>0?1:-1);
            if (df==0)    return ray_clear(from, to_sq, 0, dr>0?1:-1);
            if (dr==0)    return ray_clear(from, to_sq, df>0?1:-1, 0);
            return false;
        case 6: return adf<=1 && adr<=1 && (adf+adr>0);
        }
        return false;
    }

    bool ray_clear(int from, int to_sq, int df, int dr) const {
        int f = file_of(from) + df;
        int r = rank_of(from) + dr;
        int tf = file_of(to_sq), tr = rank_of(to_sq);
        while (f != tf || r != tr) {
            if (!on_board(f,r)) return false;
            if (sq[S(f,r)] != EMPTY) return false;
            f += df; r += dr;
        }
        return true;
    }
};
