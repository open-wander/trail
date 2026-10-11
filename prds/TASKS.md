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

## Phase 3: Shutdown and flush hardening — 3 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))
