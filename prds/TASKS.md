# Tasks

Single source of truth for active/pending tasks. Status lives here only.
Source: TydeWhatMay/infra `docs/prod-logging.md` (production logging review, 2026-10-09).
Trail is the analytics for the TydeWhatMay/open-wander estate (stats.openwander.org) and
tails the platform Traefik's access log.

## Phase 1: Traefik JSON access log — 6 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))

## Phase 2: Trail's own logs

Trail writes ~9,000 stdlib `log` text lines/day in prod, almost all routine
(`tailer: processed 4 lines, new offset=...`, `flushed 10 entries to database`). Standard:
one JSON object per line, `time`/`level`/`msg`, `msg` a stable event key.

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 2.1 | Switch to `log/slog` JSON handler (text handler when not in production, or a `TRAIL_LOG_FORMAT`-style env); per-batch tailer and flush lines at DEBUG; startup, format detection, backfill results and errors at INFO/WARN/ERROR | infra `docs/prod-logging.md` | TODO | Match nectvia/havencast (slog). Tests capture output via a buffer handler. |
| 2.2 | Refuse to start when the htpasswd file is configured but unreadable (today: log warning and serve WITHOUT auth) | infra `platform/trail/README.md` "Auth" | DONE (2026-10-11) | `server.New` returns `(*Server, error)`; a configured htpasswd that is missing, unreadable or has no valid users is a startup error naming the path, with no fallback to env creds. Server built before backfill. `TestNewHtpasswdStartup` (8 cases), 5 mutations red. Operator: a bad htpasswd mount now crash-loops the Nomad job instead of serving without auth. |

## Phase 3: Shutdown and flush hardening

From the Phase 1 sweep (see progress.md, "Sweep after Phase 1"). Runs before Phase 2.

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 3.1 | Test that the tailer never skips a line while the channel is blocked: hold the channel full for longer than a short skip interval before cancelling, then assert the saved offset covers only lines actually sent. Test-only | progress.md sweep | TODO | Re-adding `case <-time.After(50ms): sentOffset = readOffset` beside the ctx case currently passes every test. |
| 3.2 | Test the aggregator's post-cancel flush guards (threshold flush `ctx.Err() == nil`, ticker `ctx.Err() != nil -> continue`) deterministically, with the smallest test seam needed (e.g. injectable ticker channel) | progress.md sweep | TODO | Removing either guard currently passes every test; a flush on a cancelled ctx drops up to 1,000 entries at shutdown. |
| 3.3 | A failed flush must not lose its batch or stop aggregation: `flush()` resets the buffers before the transaction, so a DB error drops the batch, and `Run` returns on any periodic/threshold flush error, which (since 1.6) leaves the tailer blocked on a full channel until restart. Keep the snapshot on failure (merge it back), log and retry on the next tick instead of returning, with a sensible bound so a dead DB cannot grow memory without limit | progress.md sweep | TODO | Fix, not just a test. (needs: 3.2) |
