#!/usr/bin/env bash
# ============================================================
# deploy.sh — Full Lichess import on fresh Ubuntu 24.04
#
# Place ALL these files in the same directory:
#   db.pgn.zst
#   lichess_schema_fast.sql
#   post_load.sql
#   lichess_import.cpp  zobrist.cpp
#   board.h  pgn_parser.h  zobrist.h
#   Makefile
#
# Run as a non-root user with sudo:
#   chmod +x deploy.sh && ./deploy.sh
#
# Re-runnable: safe to run again after a crash.
#   - Packages: apt is idempotent
#   - Binary: make only rebuilds if sources changed
#   - DB role/database: created only if not exists
#   - Schema: created only if tables don't exist
#   - Import: resumes from ~/.lichess_import_ckpt if present
#   - post_load: skipped if indexes already exist
# ============================================================

set -euo pipefail

# ── Debug mode ────────────────────────────────────────────────────────────────
# Usage:  ./deploy.sh --debug [N]
#   Imports only the first N games (default 1,000,000) then stops.
#   Skips the slow post-load index build so you get a fast schema/pipeline check.
#   Uses a separate database (lichess_debug) so it never pollutes a real import.
#   Re-drops and recreates the debug DB on each run for a clean slate.
#
DEBUG=0
DEBUG_LIMIT=1000000
if [[ "${1:-}" == "--debug" ]]; then
    DEBUG=1
    [[ -n "${2:-}" && "${2}" =~ ^[0-9]+$ ]] && DEBUG_LIMIT="$2"
    echo "━━━  DEBUG MODE — first ${DEBUG_LIMIT} games only  ━━━"
fi

DIR="$(cd "$(dirname "$0")" && pwd)"
DB_USER="lichess"
DB_PASS="lichess"
if [[ $DEBUG -eq 1 ]]; then
    DB_NAME="lichess_debug"
else
    DB_NAME="lichess"
fi
DSN="postgresql://${DB_USER}:${DB_PASS}@localhost/${DB_NAME}"
IMPORTER="$DIR/lichess_import"
IMPORT_LOG="$DIR/import.log"
CKPT="$HOME/.lichess_import_ckpt"

BATCH=50000

log()  { echo; echo "━━━  $*  ━━━"; }
die()  { echo "✗ FATAL: $*" >&2; exit 1; }

pg_admin() { sudo -u postgres psql -v ON_ERROR_STOP=1 "$@" || die "psql (admin) failed"; }
pg_run()   { psql -v ON_ERROR_STOP=1 "$DSN" "$@"           || die "psql (lichess) failed"; }

# Wait for Postgres to accept connections (up to 30s)
pg_wait() {
    local i=0
    until sudo -u postgres psql -c "SELECT 1" >/dev/null 2>&1; do
        i=$(( i+1 ))
        [[ $i -gt 30 ]] && die "Postgres did not start within 30s"
        sleep 1
    done
}

# ── 1. System packages ───────────────────────────────────────────────────────

log "Installing system packages"
sudo apt-get update -qq
sudo apt-get install -y \
    postgresql \
    libpq-dev libzstd-dev \
    build-essential g++

# ── 2. Pull latest + compile C++ importer ────────────────────────────────────

log "Pulling latest from $(git -C "$DIR" branch --show-current)"
git -C "$DIR" pull --ff-only

log "Compiling lichess_import"
cd "$DIR"
make -j"$(nproc)"
cd - > /dev/null
echo "  Built: $IMPORTER"

# ── 3. Start Postgres + create role/database ─────────────────────────────────

log "Starting PostgreSQL"
sudo systemctl enable --now postgresql
pg_wait

pg_admin << SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASS}';
  END IF;
END
\$\$;
SQL

if [[ $DEBUG -eq 1 ]]; then
    # Drop and recreate debug DB every run for a clean slate
    pg_admin << SQL
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};
SQL
else
    pg_admin << SQL
SELECT 'CREATE DATABASE ${DB_NAME} OWNER ${DB_USER}'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')
\gexec
SQL
fi

# ── 4. Tune Postgres for bulk import ─────────────────────────────────────────
#
# C++ threads are lightweight (~4MB stack each) — no per-worker RAM bloat.
# Budget = available RAM minus OS overhead (4GB) and a 4GB safety margin.

log "Applying import-time Postgres tuning"

NCPU=$(nproc)
HALF_CPU=$(( NCPU / 2 < 1 ? 1 : NCPU / 2 ))

TOTAL_MB=$(free -m | awk '/^Mem/{print $2}')
AVAIL_MB=$(free -m | awk '/^Mem/{print $7}')
BUDGET_MB=$(( AVAIL_MB - 8192 ))   # reserve 8GB for OS + safety margin
[[ $BUDGET_MB -lt 4096 ]] && BUDGET_MB=4096

# shared_buffers: 25% of budget, capped at 16GB
SHARED_MB=$(( BUDGET_MB / 4 ))
[[ $SHARED_MB -gt 16384 ]] && SHARED_MB=16384

# work_mem: 0.5% of budget, capped at 256MB, min 64MB
WORK_MB=$(( BUDGET_MB / 200 ))
[[ $WORK_MB -gt 256 ]] && WORK_MB=256
[[ $WORK_MB -lt 64  ]] && WORK_MB=64

# maintenance_work_mem: 5% of budget, capped at 8GB, min 256MB
MAINT_MB=$(( BUDGET_MB / 20 ))
[[ $MAINT_MB -gt 8192 ]] && MAINT_MB=8192
[[ $MAINT_MB -lt 256  ]] && MAINT_MB=256

# effective_cache_size: 75% of total RAM (planner hint only)
CACHE_MB=$(( TOTAL_MB * 3 / 4 ))

# wal_buffers: 1% of budget, capped at 1GB, min 16MB
WAL_BUFFERS_MB=$(( BUDGET_MB / 100 ))
[[ $WAL_BUFFERS_MB -gt 1024 ]] && WAL_BUFFERS_MB=1024
[[ $WAL_BUFFERS_MB -lt 16   ]] && WAL_BUFFERS_MB=16

echo "  RAM: total=${TOTAL_MB}MB avail=${AVAIL_MB}MB budget=${BUDGET_MB}MB"
echo "  PG:  shared_buffers=${SHARED_MB}MB work_mem=${WORK_MB}MB maint=${MAINT_MB}MB"

pg_admin << SQL
ALTER SYSTEM SET synchronous_commit               = 'off';
ALTER SYSTEM SET fsync                            = 'off';
ALTER SYSTEM SET full_page_writes                 = 'off';
ALTER SYSTEM SET wal_level                        = 'minimal';
ALTER SYSTEM SET max_wal_senders                  = 0;
ALTER SYSTEM SET checkpoint_timeout               = '1h';
ALTER SYSTEM SET max_wal_size                     = '16GB';
ALTER SYSTEM SET wal_buffers                      = '${WAL_BUFFERS_MB}MB';
ALTER SYSTEM SET autovacuum                       = 'off';
ALTER SYSTEM SET work_mem                         = '${WORK_MB}MB';
ALTER SYSTEM SET maintenance_work_mem             = '${MAINT_MB}MB';
ALTER SYSTEM SET max_parallel_workers             = ${NCPU};
ALTER SYSTEM SET max_parallel_maintenance_workers = ${HALF_CPU};
ALTER SYSTEM SET max_worker_processes             = ${NCPU};
ALTER SYSTEM SET shared_buffers                   = '${SHARED_MB}MB';
ALTER SYSTEM SET effective_cache_size             = '${CACHE_MB}MB';
SQL

# wal_level=minimal requires full restart
sudo systemctl restart postgresql
pg_wait
log "PostgreSQL restarted with import settings"

# ── 5. Schema ────────────────────────────────────────────────────────────────
# Idempotent: IF NOT EXISTS on all tables so re-runs skip this safely.

log "Creating schema (skipped if already exists)"
pg_run -f "$DIR/lichess_schema_fast.sql"

# ── 6. Import ────────────────────────────────────────────────────────────────

PARSERS=$(( NCPU - 2 < 1 ? 1 : NCPU - 2 ))
FILE_SIZE=$(stat -c%s "$DIR/db.pgn.zst" 2>/dev/null || echo 0)
TARGET_GAMES=$(( FILE_SIZE / 322 ))

# Always clear checkpoint when DB is empty — it may be baked into a VM snapshot
DB_GAME_COUNT=$(psql "$DSN" -At -c "SELECT COUNT(*) FROM games" 2>/dev/null || echo 0)
if [[ "$DB_GAME_COUNT" -eq 0 ]]; then
    [[ -f "$CKPT" ]] && echo "  Clearing checkpoint (DB is empty)" && rm -f "$CKPT"
fi

if [[ -f "$CKPT" ]]; then
    RESUME_ID=$(head -1 "$CKPT")
    RESUME_GAMES=$(sed -n '2p' "$CKPT" || echo 0)
    log "Resuming import from checkpoint: $RESUME_ID (~$RESUME_GAMES games already done)"
else
    log "Starting fresh import"
fi

echo "  Parser threads : $PARSERS"
echo "  Estimated total: $TARGET_GAMES games"
echo "  Batch size     : $BATCH"
echo "  Log            : $IMPORT_LOG"
echo ""

# ── Launch importer in background ────────────────────────────────────────────
IMPORTER_ARGS=(
    --file    "$DIR/db.pgn.zst"
    --dsn     "$DSN"
    --parsers "$PARSERS"
    --batch   "$BATCH"
    --target  "$TARGET_GAMES"
)
if [[ $DEBUG -eq 1 ]]; then
    IMPORTER_ARGS+=(--limit "$DEBUG_LIMIT" --no-resume)
fi

"$IMPORTER" "${IMPORTER_ARGS[@]}" >> "$IMPORT_LOG" 2>&1 &
IMPORT_PID=$!
echo "  Importer PID: $IMPORT_PID"
echo ""

# ── Inline progress monitor ───────────────────────────────────────────────────
# Polls DB every 30s and prints a single status line showing games + moves.
# If moves stay at 0 while games climb, prints a warning.
echo "  Waiting for first batch (~30s)..."
sleep 25

LAST_GAMES=0; LAST_MOVES=0; LAST_TS=$(date +%s)

while kill -0 "$IMPORT_PID" 2>/dev/null; do
    NOW=$(date +%s)
    GAMES=$(psql    "$DSN" -At -c "SELECT COUNT(*) FROM games"      2>/dev/null || echo 0)
    MOVES=$(psql    "$DSN" -At -c "SELECT COUNT(*) FROM game_moves" 2>/dev/null || echo 0)
    DMOVES=$(psql   "$DSN" -At -c "SELECT COUNT(*) FROM moves"      2>/dev/null || echo 0)
    PLAYERS=$(psql  "$DSN" -At -c "SELECT COUNT(*) FROM players"    2>/dev/null || echo 0)
    DBSZ=$(psql     "$DSN" -At -c "SELECT pg_size_pretty(pg_database_size('${DB_NAME}'))" 2>/dev/null || echo "?")

    INTERVAL=$(( NOW - LAST_TS ))
    GAMES_RATE=0; MOVES_RATE=0
    if [[ $INTERVAL -gt 0 ]]; then
        GAMES_RATE=$(( (GAMES - LAST_GAMES) * 60 / INTERVAL ))
        MOVES_RATE=$(( (MOVES - LAST_MOVES) * 60 / INTERVAL ))
    fi
    LAST_GAMES=$GAMES; LAST_MOVES=$MOVES; LAST_TS=$NOW

    PCT=0
    [[ $TARGET_GAMES -gt 0 ]] && PCT=$(( GAMES * 100 / TARGET_GAMES ))
    FILLED=$(( PCT / 5 )); BAR=""
    for (( i=0; i<20; i++ )); do [[ $i -lt $FILLED ]] && BAR+="█" || BAR+="░"; done

    # Moves health indicator
    if   [[ $MOVES -gt 0 ]];                  then MOVES_STATUS="✓";
    elif [[ $GAMES -gt 0 && $MOVES -eq 0 ]];  then MOVES_STATUS="✗ ZERO — check log!";
    else                                           MOVES_STATUS="(waiting)"; fi

    printf "  [%s] %3d%%  games=%-12s +%s/min  moves=%-14s +%s/min %s  players=%s  uniq_moves=%s  db=%s\n" \
        "$BAR" "$PCT" \
        "$(printf "%'d" "$GAMES")"   "$(printf "%'d" "$GAMES_RATE")" \
        "$(printf "%'d" "$MOVES")"   "$(printf "%'d" "$MOVES_RATE")" \
        "$MOVES_STATUS" \
        "$(printf "%'d" "$PLAYERS")" \
        "$DMOVES" "$DBSZ"

    sleep 30
done

wait "$IMPORT_PID"
IMPORT_EXIT=$?

echo ""
if [[ $IMPORT_EXIT -ne 0 ]]; then
    echo "✗ Importer exited with code $IMPORT_EXIT — last 20 lines of log:"
    tail -20 "$IMPORT_LOG"
    die "Import failed"
fi

# Clear checkpoint on success
rm -f "$CKPT"
echo "  Import complete."

# ── 7. Post-load: indexes + constraints ──────────────────────────────────────
# Idempotent: all statements use IF NOT EXISTS or CONCURRENTLY (which is a no-op
# if the index already exists). Safe to re-run after a partial failure.

if [[ $DEBUG -eq 1 ]]; then
    log "DEBUG MODE — skipping post_load.sql index builds"
else
    log "Running post_load.sql (index builds — may take 30-60 min)"
    pg_run -f "$DIR/post_load.sql"
fi

# ── 8. Restore safe Postgres settings ────────────────────────────────────────

log "Restoring production Postgres settings"
pg_admin << SQL
ALTER SYSTEM SET synchronous_commit               = 'on';
ALTER SYSTEM SET fsync                            = 'on';
ALTER SYSTEM SET full_page_writes                 = 'on';
ALTER SYSTEM SET wal_level                        = 'replica';
ALTER SYSTEM SET max_wal_senders                  = 3;
ALTER SYSTEM SET checkpoint_timeout               = '5min';
ALTER SYSTEM SET max_wal_size                     = '4GB';
ALTER SYSTEM SET wal_buffers                      = '64MB';
ALTER SYSTEM SET autovacuum                       = 'on';
ALTER SYSTEM SET work_mem                         = '64MB';
ALTER SYSTEM SET maintenance_work_mem             = '2GB';
ALTER SYSTEM SET shared_buffers                   = '${SHARED_MB}MB';
ALTER SYSTEM SET effective_cache_size             = '${CACHE_MB}MB';
SQL

# wal_level=replica requires restart
sudo systemctl restart postgresql
pg_wait
log "PostgreSQL restored to production settings"

# ── 9. Final smoke test ───────────────────────────────────────────────────────

log "Final counts"
pg_run -c "SELECT
    (SELECT COUNT(*) FROM games)        AS total_games,
    (SELECT COUNT(*) FROM game_moves)   AS total_moves,
    (SELECT COUNT(*) FROM players)      AS total_players,
    (SELECT COUNT(*) FROM moves)        AS distinct_move_types"

log "All done — connect with:  psql \"$DSN\""
