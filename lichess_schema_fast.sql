-- ============================================================
-- lichess_schema_fast.sql
-- Idempotent: all CREATE TABLE use IF NOT EXISTS.
-- Safe to run on a database that already has some tables.
--
-- UNLOGGED tables for bulk import speed (no WAL overhead).
-- Converted to LOGGED and indexed by post_load.sql.
-- ============================================================

-- ── Dimension tables (LOGGED — small, survive Postgres restart) ─

CREATE TABLE IF NOT EXISTS players (
    id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username    TEXT NOT NULL,
    CONSTRAINT players_username_uq UNIQUE (username)
);

CREATE TABLE IF NOT EXISTS openings (
    id          SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    eco         CHAR(3),
    name        TEXT,
    CONSTRAINT openings_eco_name_uq UNIQUE (eco, name)
);

CREATE TABLE IF NOT EXISTS time_controls (
    id          SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw         TEXT NOT NULL,
    base_secs   INT,
    increment   INT,
    CONSTRAINT time_controls_raw_uq UNIQUE (raw)
);

-- Move dimension: every distinct UCI move token.
-- INT not SMALLINT: Chess960 generates >32,767 distinct UCI moves.

CREATE TABLE IF NOT EXISTS moves (
    id          INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uci         VARCHAR(5) NOT NULL,
    from_sq     CHAR(2)    NOT NULL,
    to_sq       CHAR(2)    NOT NULL,
    promotion   CHAR(1),
    CONSTRAINT moves_uci_uq UNIQUE (uci)
);

-- ── Fact tables (UNLOGGED during import) ─────────────────────────

CREATE UNLOGGED TABLE IF NOT EXISTS games (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lichess_id          VARCHAR(12) UNIQUE,
    white_id            INT,
    black_id            INT,
    white_elo           SMALLINT,
    black_elo           SMALLINT,
    white_rating_diff   SMALLINT,
    black_rating_diff   SMALLINT,
    result              VARCHAR(9),
    termination         TEXT,
    utc_date            DATE,
    utc_time            TIME,
    time_control_id     SMALLINT,
    opening_id          SMALLINT,
    event_type          TEXT,
    speed               TEXT,
    movetext            TEXT
) WITH (autovacuum_enabled = false);

-- position_hash: Zobrist hash of board BEFORE this move (signed BIGINT).
-- Primary query pattern: WHERE position_hash = X (opening explorer).

-- piece_moved: the piece type that moved — one of P N B R Q K (CHAR 1).
--   For promotions this is 'P' (the pawn that moved, not the promoted piece).
--   Stored as the piece on the from-square before the move is applied.
--
-- material: both sides' non-king piece counts packed as 4-bit nibbles in a BIGINT.
--   Kings are excluded — they are always present and never captured.
--   10 nibbles × 4 bits = 40 bits used; fits comfortably in a signed BIGINT.
--
--   Bit layout:
--     bits  0- 3  white pawns    extract: (material      ) & 15
--     bits  4- 7  white knights  extract: (material >>  4) & 15
--     bits  8-11  white bishops  extract: (material >>  8) & 15
--     bits 12-15  white rooks    extract: (material >> 12) & 15
--     bits 16-19  white queens   extract: (material >> 16) & 15
--     bits 20-23  black pawns    extract: (material >> 20) & 15
--     bits 24-27  black knights  extract: (material >> 24) & 15
--     bits 28-31  black bishops  extract: (material >> 28) & 15
--     bits 32-35  black rooks    extract: (material >> 32) & 15
--     bits 36-39  black queens   extract: (material >> 36) & 15
--
--   Captured at the position BEFORE the move (consistent with position_hash).

CREATE UNLOGGED TABLE IF NOT EXISTS game_moves (
    game_id         BIGINT   NOT NULL,
    ply             INT      NOT NULL,
    move_id         INT      NOT NULL,
    position_hash   BIGINT   NOT NULL,
    clock_secs      INT,
    eval_cp         SMALLINT,
    eval_mate       SMALLINT,
    piece_moved     CHAR(1),
    material        BIGINT
) WITH (autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS game_tags (
    game_id     BIGINT NOT NULL,
    key         TEXT   NOT NULL,
    value       TEXT
) WITH (autovacuum_enabled = false);
