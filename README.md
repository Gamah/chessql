# chessql

A bulk importer and analytical SQL schema for Lichess PGN exports. Parses compressed `.pgn.zst` files into a PostgreSQL database optimised for pattern queries across hundreds of millions of games.

## Quick start

```bash
# All source files in the same directory as the PGN, then:
sudo ./deploy.sh

# PGN lives elsewhere:
sudo env PGN_FILE=/data/lichess_db_standard_rated_2026-03.pgn.zst ./deploy.sh

# Dedicated data volume (recommended when OS disk < 1 TB):
sudo env PGDATA_DIR=/mnt/data/postgresql ./deploy.sh
```

`deploy.sh` is fully re-runnable. A crash mid-import resumes from `~/.lichess_import_ckpt`.

Connect when done:

```bash
psql "postgresql://lichess:lichess@localhost/lichess"
```

---

## Schema overview

```
players          – unique usernames
openings         – ECO code + opening name
time_controls    – raw time control string + parsed base/increment
moves            – every distinct UCI token (from_sq, to_sq, promotion)

games            – one row per game
game_moves       – one row per half-move
game_tags        – overflow PGN tags not promoted to games columns
```

### Square encoding

All square columns (`from_sq`, `to_sq`, `wk_sq`, `bk_sq`) use the same scheme:

```
square = rank * 8 + file      a1=0, b1=1, ..., h1=7, a2=8, ..., h8=63
rank   = sq / 8               0 = rank 1, 7 = rank 8
file   = sq % 8               0 = file a, 7 = file h
edge   = rank IN (0,7) OR file IN (0,7)
```

---

## game_moves column reference

| Column | Type | Notes |
|---|---|---|
| `game_id` | BIGINT | FK → games.id |
| `ply` | INT | 1-indexed half-moves; odd = White, even = Black |
| `move_id` | INT | FK → moves.id (UCI token) |
| `position_hash` | BIGINT | Polyglot Zobrist hash of board **before** this move |
| `clock_secs` | INT | Clock remaining after move (NULL if no clock annotations) |
| `eval_cp` | SMALLINT | Engine eval in centipawns (NULL if not annotated) |
| `eval_mate` | SMALLINT | Mate-in-N from engine annotation (NULL if not annotated) |
| `moving_piece` | CHAR(1) | FEN character: uppercase = White (`P N B R Q K`), lowercase = Black |
| `capture_piece` | CHAR(1) | Captured piece FEN char; NULL if no capture |
| `mate` | BOOL | NULL = no check, FALSE = check, TRUE = checkmate |
| `material` | BIGINT | Packed piece counts **after** move (see below) |
| `wk_sq` | SMALLINT | White king square **after** move |
| `bk_sq` | SMALLINT | Black king square **after** move |

---

## material — packed piece counts

`material` stores all ten non-king piece counts in a single BIGINT. Use the `pieces()` SQL function defined in `post_load.sql` to unpack it.

**Bit layout** (4 bits per piece type):

| Bits | Piece | Bits | Piece |
|---|---|---|---|
| 0–3 | white pawns | 4–7 | black pawns |
| 8–11 | white knights | 12–15 | black knights |
| 16–19 | white bishops | 20–23 | black bishops |
| 24–27 | white rooks | 28–31 | black rooks |
| 32–35 | white queens | 36–39 | black queens |

**Usage:**

```sql
-- Dot notation (single field)
SELECT (pieces(material)).wr FROM game_moves;

-- LATERAL spread (multiple fields, cleaner)
SELECT p.wr, p.wq, p.br, p.bq
FROM game_moves gm, pieces(gm.material) p
WHERE p.wr + p.wq >= 2;

-- Raw bit shift (no function, useful in WHERE)
((material >> 24) & 15)   -- white rooks
((material >> 28) & 15)   -- black rooks
((material >> 32) & 15)   -- white queens
((material >> 36) & 15)   -- black queens
```

---

## Example queries

### Ladder / lawnmower mates

A rook delivers checkmate, the mating side has two major pieces, and the king was driven to the edge:

```sql
SELECT
    (SELECT COUNT(*) FROM games) AS games_so_far,
    COUNT(*)                     AS ladder_mates
FROM game_moves  gm_mate
JOIN moves       m_mate  ON m_mate.id  = gm_mate.move_id
JOIN game_moves  gm_prev ON gm_prev.game_id = gm_mate.game_id
                        AND gm_prev.ply     = gm_mate.ply - 2
WHERE gm_mate.mate = TRUE
  AND gm_mate.moving_piece IN ('R', 'r')
  AND CASE
        WHEN gm_mate.moving_piece = 'R'
        THEN ((gm_mate.material >> 24) & 15) + ((gm_mate.material >> 32) & 15) >= 2
        ELSE ((gm_mate.material >> 28) & 15) + ((gm_mate.material >> 36) & 15) >= 2
      END
  -- mated king on edge (via wk_sq/bk_sq)
  AND CASE
        WHEN gm_mate.moving_piece IN ('R','Q')   -- White mates, check Black king
        THEN gm_mate.bk_sq % 8 IN (0,7) OR gm_mate.bk_sq / 8 IN (0,7)
        ELSE gm_mate.wk_sq % 8 IN (0,7) OR gm_mate.wk_sq / 8 IN (0,7)
      END
  -- partner piece (same colour, 2 plies back) was also R or Q
  AND gm_prev.moving_piece IN ('R', 'r', 'Q', 'q');
```

### Smothered mate candidates

Knight delivers checkmate while the mated king is still in its starting corner region:

```sql
SELECT COUNT(*) AS smothered_mate_candidates
FROM game_moves gm
WHERE gm.mate = TRUE
  AND gm.moving_piece IN ('N', 'n')
  -- mated king in a corner (squares 0,7,56,63)
  AND CASE
        WHEN gm.moving_piece = 'N'
        THEN gm.bk_sq IN (0, 7, 56, 63)
        ELSE gm.wk_sq IN (0, 7, 56, 63)
      END;
```

### Position / opening explorer

All games reaching a specific position (identified by Polyglot Zobrist hash), with move frequency:

```sql
SELECT m.uci,
       COUNT(*)                              AS frequency,
       AVG(g.white_elo + g.black_elo) / 2   AS avg_elo,
       SUM(CASE g.result WHEN '1-0' THEN 1 ELSE 0 END)::float / COUNT(*) AS white_win_rate
FROM game_moves gm
JOIN moves  m ON m.id  = gm.move_id
JOIN games  g ON g.id  = gm.game_id
WHERE gm.position_hash = -4966175979029733763   -- replace with your hash
GROUP BY m.uci
ORDER BY frequency DESC;
```

### King safety — castled vs uncastled

Track whether White's king ever left the e1 square before move 20:

```sql
SELECT
    g.opening_id,
    o.name,
    COUNT(*) AS games,
    SUM(CASE WHEN early_king.game_id IS NOT NULL THEN 1 ELSE 0 END) AS uncastled_count,
    ROUND(100.0 * SUM(CASE WHEN early_king.game_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_uncastled
FROM games g
JOIN openings o ON o.id = g.opening_id
LEFT JOIN LATERAL (
    SELECT gm.game_id
    FROM game_moves gm
    WHERE gm.game_id = g.id
      AND gm.ply <= 20
      AND gm.wk_sq NOT IN (4, 6, 2)  -- e1, g1 (castled KS), c1 (castled QS)
    LIMIT 1
) early_king ON true
WHERE g.variant = 'standard'
GROUP BY g.opening_id, o.name
HAVING COUNT(*) > 10000
ORDER BY pct_uncastled DESC;
```

### King opposition in endgames

Positions where kings are in direct opposition (Manhattan distance = 2, same file or rank):

```sql
SELECT COUNT(*) AS opposition_positions
FROM game_moves gm, pieces(gm.material) p
WHERE p.wp + p.bp + p.wn + p.bn + p.wb + p.bb
    + p.wr + p.br + p.wq + p.bq <= 2      -- endgame: ≤ 2 non-king pieces
  AND (
    -- vertical opposition: same file, 2 ranks apart
    (gm.wk_sq % 8 = gm.bk_sq % 8 AND ABS(gm.wk_sq / 8 - gm.bk_sq / 8) = 2)
    OR
    -- horizontal opposition: same rank, 2 files apart
    (gm.wk_sq / 8 = gm.bk_sq / 8 AND ABS(gm.wk_sq % 8 - gm.bk_sq % 8) = 2)
  );
```

### Exchange sacrifice detection

Games where a player gave up a rook for a minor piece (capture_piece is R/r, moving_piece is N/B/n/b):

```sql
SELECT
    g.lichess_id,
    g.white_elo,
    g.black_elo,
    g.result,
    gm.ply
FROM game_moves gm
JOIN games g ON g.id = gm.game_id
WHERE gm.capture_piece IN ('R', 'r')
  AND gm.moving_piece  IN ('N', 'B', 'n', 'b')
  AND g.variant = 'standard'
ORDER BY (g.white_elo + g.black_elo) DESC
LIMIT 20;
```

### Fastest checkmates (fewest plies)

```sql
SELECT g.lichess_id, g.ply_count, g.white_elo, g.black_elo, g.result
FROM games g
WHERE g.ply_count <= 8
  AND g.termination = 'Normal'
  AND g.variant = 'standard'
ORDER BY g.ply_count, g.white_elo + g.black_elo DESC
LIMIT 20;
```

### Win rate by time control speed

```sql
SELECT
    g.speed,
    COUNT(*)                                                             AS games,
    ROUND(100.0 * SUM(CASE g.result WHEN '1-0'     THEN 1 ELSE 0 END) / COUNT(*), 1) AS white_win_pct,
    ROUND(100.0 * SUM(CASE g.result WHEN '0-1'     THEN 1 ELSE 0 END) / COUNT(*), 1) AS black_win_pct,
    ROUND(100.0 * SUM(CASE g.result WHEN '1/2-1/2' THEN 1 ELSE 0 END) / COUNT(*), 1) AS draw_pct
FROM games g
WHERE g.variant = 'standard'
GROUP BY g.speed
ORDER BY games DESC;
```

---

## Disk footprint (100 M games estimate)

| Component | Size |
|---|---|
| `games` table (movetext dominates, ~250 bytes/game) | ~50 GB |
| `game_moves` table (~5 B rows × 72 bytes/row) | ~360 GB |
| `game_tags`, `players`, `moves`, etc. | ~5 GB |
| `game_moves` indexes (PK + 9 btrees/partial indexes on 5 B rows) | ~310 GB |
| `games` indexes (11 btrees on 100 M rows) | ~15 GB |
| **Total** | **~740 GB** |

The `game_moves` table and its indexes account for ~90% of storage. The two position-hash covering indexes (`pos_move_idx`, `move_pos_idx`) are ~80 GB each.

If disk is tight, `movetext` in `games` (~25 GB) is safe to drop — all move data is already in `game_moves`.

---

## Architecture

```
reader thread       decompresses .zst, splits PGN games, pushes to raw_queue
parser workers (N)  SAN→UCI, Zobrist hash, piece metadata → GameRow structs
writer thread       batches GameRow structs, bulk-inserts via COPY protocol
```

Parser thread count defaults to `nproc - 2`. The writer is the bottleneck on large imports; parser parallelism past ~8 threads gives diminishing returns.
