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
    from_sq     SMALLINT   NOT NULL,   -- 0-63: rank*8 + file (a1=0, h8=63)
    to_sq       SMALLINT   NOT NULL,
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
    variant             TEXT,          -- 'standard','chess960','crazyhouse', etc.
    ply_count           SMALLINT,      -- total half-moves played
    movetext            TEXT
) WITH (autovacuum_enabled = false);

-- position_hash: Zobrist hash of board BEFORE this move (signed BIGINT).
-- Primary query pattern: WHERE position_hash = X (opening explorer).
--
-- moving_piece / capture_piece: FEN-style single character
--   uppercase = white (P N B R Q K), lowercase = black (p n b r q k)
--   capture_piece is NULL when no capture occurred.
--
-- mate: NULL = move did not give check, FALSE = check, TRUE = checkmate.
--
-- material: packed piece counts after the move.
--   Use the pieces() SQL function to extract individual counts.
--   Layout (4 bits per nibble):
--     bits  0- 3  white pawns    bits  4- 7  black pawns
--     bits  8-11  white knights  bits 12-15  black knights
--     bits 16-19  white bishops  bits 20-23  black bishops
--     bits 24-27  white rooks    bits 28-31  black rooks
--     bits 32-35  white queens   bits 36-39  black queens

CREATE UNLOGGED TABLE IF NOT EXISTS game_moves (
    game_id         BIGINT   NOT NULL,
    ply             INT      NOT NULL,
    move_id         INT      NOT NULL,
    position_hash   BIGINT   NOT NULL,
    clock_secs      INT,
    eval_cp         SMALLINT,
    eval_mate       SMALLINT,
    moving_piece    CHAR(1)  NOT NULL,
    capture_piece   CHAR(1),
    mate            BOOL,
    material        BIGINT   NOT NULL
) WITH (autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS game_tags (
    game_id     BIGINT NOT NULL,
    key         TEXT   NOT NULL,
    value       TEXT
) WITH (autovacuum_enabled = false);
