# Tasks

Single source of truth for active/pending tasks. Status lives here only.
Source: TydeWhatMay/infra `docs/prod-logging.md` (production logging review, 2026-10-09).
Trail is the analytics for the TydeWhatMay/open-wander estate (stats.openwander.org) and
tails the platform Traefik's access log.

## Phase 1: Traefik JSON access log

Production Traefik moves to `--accesslog.format=json` once Trail can read it. Order is
fixed: Trail release and deploy first, Traefik flag second (infra repo).

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 1.1 | `traefik-json` format: `ParseTraefikJSON` into the same `LogEntry`; any line starting with `{` parsed as JSON unless format is `combined` (mixed CLF+JSON files on switchover day and CLF rotated backfill); detection, aggregator default, security-view check, docs with safe header settings | infra `docs/prod-logging.md` | DONE on branch (2026-10-09) | Branch `feat/traefik-json-log` (a187c17..bbf65cf), not merged. Tests pass; verified against 1,000 real prod CLF lines and Traefik v3.7.7 JSON samples. 9 mutations red. |
| 1.2 | Merge 1.1 and tag a release (image built by the release workflow) | infra `docs/prod-logging.md` | TODO | OPERATOR: tag. Deploy is infra task 4.4. (needs: 1.1) |
| 1.3 | Handler-level test for the security view's format check (`internal/server/handlers.go`, `parser.ParseFormat(cfg.LogFormat) == parser.FormatCombined`): traefik-json uses unrouted mode, combined uses suspicious-path mode | infra `docs/prod-logging.md` | TODO | Currently covered only via `TestParseFormat`; the handler line is not mutation-tested. (needs: 1.1) |
| 1.4 | Security view should use the DETECTED format, not the configured string: with `TRAIL_LOG_FORMAT=auto` on a combined (nginx) log it runs in Traefik mode | infra `docs/prod-logging.md` | TODO | Pre-existing bug, not introduced by 1.1. Expose the parser's locked format to the server. (needs: 1.1) |

## Phase 2: Trail's own logs

Trail writes ~9,000 stdlib `log` text lines/day in prod, almost all routine
(`tailer: processed 4 lines, new offset=...`, `flushed 10 entries to database`). Standard:
one JSON object per line, `time`/`level`/`msg`, `msg` a stable event key.

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 2.1 | Switch to `log/slog` JSON handler (text handler when not in production, or a `TRAIL_LOG_FORMAT`-style env); per-batch tailer and flush lines at DEBUG; startup, format detection, backfill results and errors at INFO/WARN/ERROR | infra `docs/prod-logging.md` | TODO | Match nectvia/havencast (slog). Tests capture output via a buffer handler. |
| 2.2 | Refuse to start when the htpasswd file is configured but unreadable (today: log warning and serve WITHOUT auth) | infra `platform/trail/README.md` "Auth" | TODO | Security. Found on first nomad-prod deploy: dashboard answered 200 for four minutes. Test: unreadable file -> startup error. |
