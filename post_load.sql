-- ============================================================
-- post_load.sql — Run after lichess_import_fast.py completes.
-- Called by deploy.sh automatically.
--
-- FIX: CLUSTER and CREATE INDEX CONCURRENTLY cannot run inside
-- a transaction block. deploy.sh calls psql with
-- --single-transaction=off (default), so each statement is its
-- own transaction — this is correct. Do NOT wrap this file in
-- BEGIN/COMMIT.
-- ============================================================

-- ── 1. Make tables durable ───────────────────────────────────
ALTER TABLE games      SET LOGGED;
ALTER TABLE game_moves SET LOGGED;
ALTER TABLE game_tags  SET LOGGED;

-- ── 2. Re-enable autovacuum ──────────────────────────────────
ALTER TABLE games      RESET (autovacuum_enabled);
ALTER TABLE game_moves RESET (autovacuum_enabled);
ALTER TABLE game_tags  RESET (autovacuum_enabled);

-- ── 3. Constraints ──────────────────────────────────────────
-- games_lichess_id_uq already exists (defined in schema).
-- NOT VALID: adds FK without scanning existing rows (instant).
-- VALIDATE below checks existing rows without holding a lock.
ALTER TABLE games ADD CONSTRAINT games_white_id_fk
    FOREIGN KEY (white_id) REFERENCES players(id) NOT VALID;
ALTER TABLE games ADD CONSTRAINT games_black_id_fk
    FOREIGN KEY (black_id) REFERENCES players(id) NOT VALID;
ALTER TABLE games ADD CONSTRAINT games_tc_fk
    FOREIGN KEY (time_control_id) REFERENCES time_controls(id) NOT VALID;
ALTER TABLE games ADD CONSTRAINT games_opening_fk
    FOREIGN KEY (opening_id) REFERENCES openings(id) NOT VALID;
ALTER TABLE game_moves ADD CONSTRAINT game_moves_game_fk
    FOREIGN KEY (game_id) REFERENCES games(id) NOT VALID;
ALTER TABLE game_moves ADD CONSTRAINT game_moves_move_fk
    FOREIGN KEY (move_id) REFERENCES moves(id) NOT VALID;
ALTER TABLE game_tags ADD CONSTRAINT game_tags_game_fk
    FOREIGN KEY (game_id) REFERENCES games(id) NOT VALID;

ALTER TABLE games VALIDATE CONSTRAINT games_white_id_fk;
ALTER TABLE games VALIDATE CONSTRAINT games_black_id_fk;

-- ── 5. Primary key on game_moves ─────────────────────────────
ALTER TABLE game_moves ADD CONSTRAINT game_moves_pk
    PRIMARY KEY (game_id, ply);

-- ── 6. Indexes (CONCURRENTLY = no table lock) ────────────────
-- These are slow on large tables — run them in parallel by
-- opening multiple psql sessions and running one group each.
-- deploy.sh runs them sequentially (simpler, fine for one-time import).

-- games: player lookups
CREATE INDEX CONCURRENTLY games_white_id_idx   ON games(white_id);
CREATE INDEX CONCURRENTLY games_black_id_idx   ON games(black_id);

-- games: date range (BRIN = kilobytes vs GB for btree, fast on append-ordered data)
CREATE INDEX CONCURRENTLY games_utc_date_brin  ON games USING BRIN (utc_date);

-- games: combined player+date (covers "all games by player, newest first")
CREATE INDEX CONCURRENTLY games_white_date_idx ON games(white_id, utc_date);
CREATE INDEX CONCURRENTLY games_black_date_idx ON games(black_id, utc_date);

-- games: misc filters
CREATE INDEX CONCURRENTLY games_result_idx     ON games(result);
CREATE INDEX CONCURRENTLY games_tc_id_idx      ON games(time_control_id);
CREATE INDEX CONCURRENTLY games_opening_id_idx ON games(opening_id);
CREATE INDEX CONCURRENTLY games_white_elo_idx  ON games(white_elo);
CREATE INDEX CONCURRENTLY games_black_elo_idx  ON games(black_elo);

-- game_moves: move-centric lookups
CREATE INDEX CONCURRENTLY game_moves_move_id_idx  ON game_moves(move_id);

-- game_moves: position lookup ("what was played from this position?")
CREATE INDEX CONCURRENTLY game_moves_pos_hash_idx ON game_moves(position_hash);

-- game_moves: opening explorer — covered index-only scan
CREATE INDEX CONCURRENTLY game_moves_pos_move_idx ON game_moves(position_hash, move_id);

-- game_moves: inverse — "all positions where this move appeared"
CREATE INDEX CONCURRENTLY game_moves_move_pos_idx ON game_moves(move_id, position_hash);

-- ── 7. Cluster games by player+date ──────────────────────────
-- Physically rewrites the table so "all games by player" scans
-- hit contiguous disk pages instead of random I/O.
-- Takes ~20-40 min per 100M rows. Safe to skip and run later.
CLUSTER games USING games_white_date_idx;

-- ── 8. Update planner statistics ─────────────────────────────
ANALYZE players;
ANALYZE openings;
ANALYZE time_controls;
ANALYZE moves;
ANALYZE games;
ANALYZE game_moves;
ANALYZE game_tags;
