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
| 2.1 | Switch to `log/slog` JSON on stdout (always JSON; `TRAIL_LOG_LEVEL`=debug\|info\|warn\|error, default info - `TRAIL_LOG_FORMAT` is already the access-log format). `msg` is a stable event key with variables in fields; per-batch tailer and flush lines at DEBUG; startup, format detection, backfill results at INFO; errors at WARN/ERROR. Disable the Fiber startup banner | infra `docs/prod-logging.md` | DONE (2026-10-11) | New `internal/logging` (ParseLevel/New/Init, as nectvia); `logging.Init(TRAIL_LOG_LEVEL)` first in main; all stdlib `log` gone; `error` field key; stable dotted msg keys (inventory in progress.md). Per-batch `tailer.processed`, `aggregator.flushed`, `aggregator.line_skipped` at DEBUG. Fiber banner off; `server.New` returns errors instead of `log.Fatalf`. Smoke run: stdout pure JSON, stderr empty, no secrets. Deploy: logs moved stderr -> stdout and old sentences are gone, so check any Loki query/alert or `prod-logs` skill query that matched them. |
| 2.2 | Refuse to start when the htpasswd file is configured but unreadable (today: log warning and serve WITHOUT auth) | infra `platform/trail/README.md` "Auth" | DONE (2026-10-11) | `server.New` returns `(*Server, error)`; a configured htpasswd that is missing, unreadable or has no valid users is a startup error naming the path, with no fallback to env creds. Server built before backfill. `TestNewHtpasswdStartup` (8 cases), 5 mutations red. Operator: a bad htpasswd mount now crash-loops the Nomad job instead of serving without auth. |
| 2.3 | `http.request` log line per request (standard item 3): `request_id`, `method`, `path` (no query string), `status`, `duration_ms`, `client_ip`; 5xx at ERROR; same `request_id` on handler log lines for that request | infra `docs/prod-logging.md` | TODO | Added 2026-10-11 by user decision. (needs: 2.1) |

## Phase 3: Shutdown and flush hardening — 3 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))
