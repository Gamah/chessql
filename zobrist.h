#pragma once
#include <cstdint>
#include <cstring>

// Polyglot random array — 781 entries.
// Source: http://hardy.uhasselt.be/Toga/book_format.html
// These are the exact values used by python-chess (chess/polyglot.py)
// so Zobrist hashes produced here are bit-identical to python-chess output.
//
// Layout:
//   [0..767]   piece-square: 12 pieces × 64 squares
//              piece order: bP wP bN wN bB wB bR wR bQ wQ bK wK
//              square order: a1=0 .. h8=63
//   [768..771] castling: wK wQ bK bQ
//   [772..779] en-passant file: a..h
//   [780]      black to move

// Generated with:
//   python3 -c "
//   import chess.polyglot as p
//   for v in p.POLYGLOT_RANDOM_ARRAY: print(f'0x{v:016X}ULL,')
//   " | head -781

// We embed the full table to avoid a runtime dependency on python-chess.
// The table is 781 × 8 = 6248 bytes — negligible.

extern const uint64_t POLYGLOT_RANDOM_ARRAY[781];

// Compute Polyglot Zobrist hash of a board position.
// Input: board[64] using piece encoding below, turn, castling flags, ep_file.
//
// Piece encoding (matches Board::Piece enum in board.h):
//   0=empty 1=wP 2=wN 3=wB 4=wR 5=wQ 6=wK 7=bP 8=bN 9=bB 10=bR 11=bQ 12=bK
//
// Returns UNSIGNED 64-bit hash.
// Cast to int64_t with memcpy to store as Postgres BIGINT (matches Python ctypes.c_int64).

inline uint64_t polyglot_piece_index(int piece) {
    // Polyglot piece order: bP=0 wP=1 bN=2 wN=3 bB=4 wB=5 bR=6 wR=7 bQ=8 wQ=9 bK=10 wK=11
    switch (piece) {
        case  7: return  0; // bP
        case  1: return  1; // wP
        case  8: return  2; // bN
        case  2: return  3; // wN
        case  9: return  4; // bB
        case  3: return  5; // wB
        case 10: return  6; // bR
        case  4: return  7; // wR
        case 11: return  8; // bQ
        case  5: return  9; // wQ
        case 12: return 10; // bK
        case  6: return 11; // wK
        default: return 99; // empty — caller checks
    }
}
