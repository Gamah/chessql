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

CREATE UNLOGGED TABLE IF NOT EXISTS game_moves (
    game_id         BIGINT   NOT NULL,
    ply             INT      NOT NULL,
    move_id         INT      NOT NULL,
    position_hash   BIGINT   NOT NULL,
    clock_secs      INT,
    eval_cp         SMALLINT,
    eval_mate       SMALLINT
) WITH (autovacuum_enabled = false);

CREATE UNLOGGED TABLE IF NOT EXISTS game_tags (
    game_id     BIGINT NOT NULL,
    key         TEXT   NOT NULL,
    value       TEXT
) WITH (autovacuum_enabled = false);
