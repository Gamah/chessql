# ChessQL

Bulk-imports Lichess PGN dumps into PostgreSQL. C++17, libpq COPY, multi-threaded.

## Usage

```bash
./deploy.sh db.pgn.zst              # full import
./deploy.sh db.pgn.zst --debug      # first 1,000,000 games, separate DB (lichess_debug)
./deploy.sh db.pgn.zst --debug 50000
```

Re-runnable — checkpoints after every batch and resumes on restart.

## Debug mode

Validates the schema and pipeline without a full import. Uses `lichess_debug` (dropped and recreated each run). Skips `post_load.sql` index builds.

**Deadlock fix (this branch):** when the game limit was reached, the writer set `g_stop` but never called `wake_all()` on the queues. Reader/parser threads blocked in `BoundedQueue::push()`/`pop()` on condition variables that were never notified, hanging the process indefinitely. Fixed by calling `raw_q.wake_all()` and `parsed_q.wake_all()` immediately after `g_stop.store(true)` in the limit path.

## game_moves: piece_moved and material

`piece_moved` — piece on the from-square before the move: `P N B R Q K` (always `P` for promotions).

`material` — non-king piece counts packed as 4-bit nibbles in a `BIGINT`, captured before the move (consistent with `position_hash`):

```
bits  0– 3  white pawns      bits 20–23  black pawns
bits  4– 7  white knights    bits 24–27  black knights
bits  8–11  white bishops    bits 28–31  black bishops
bits 12–15  white rooks      bits 32–35  black rooks
bits 16–19  white queens     bits 36–39  black queens
```

Example — rook moves in R-vs-R endings:

```sql
SELECT g.lichess_id, gm.ply
FROM game_moves gm
JOIN games g ON g.id = gm.game_id
WHERE gm.piece_moved = 'R'
  AND (gm.material >> 12) & 15 = 1  -- white rooks = 1
  AND (gm.material >> 32) & 15 = 1  -- black rooks = 1
  AND (gm.material >> 16) & 15 = 0  -- no queens
  AND (gm.material >> 36) & 15 = 0
  AND (gm.material >>  4) & 15 = 0  -- no minor pieces
  AND (gm.material >>  8) & 15 = 0
  AND (gm.material >> 24) & 15 = 0
  AND (gm.material >> 28) & 15 = 0;
```
