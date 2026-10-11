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
| 1.1 | `traefik-json` format: `ParseTraefikJSON` into the same `LogEntry`; any line starting with `{` parsed as JSON unless format is `combined` (mixed CLF+JSON files on switchover day and CLF rotated backfill); detection, aggregator default, security-view check, docs with safe header settings | infra `docs/prod-logging.md` | DONE (2026-10-09, released v0.4.0/v0.4.1) | Branch `feat/traefik-json-log` (a187c17..bbf65cf), not merged. Tests pass; verified against 1,000 real prod CLF lines and Traefik v3.7.7 JSON samples. 9 mutations red. |
| 1.2 | Merge 1.1 and tag a release (image built by the release workflow) | infra `docs/prod-logging.md` | DONE (2026-10-09) | v0.4.1 = `ghcr.io/open-wander/trail:0.4.1` (amd64+arm64). v0.4.0 has binaries but NO image (Docker Hub timeout on the hosted runner); image job now runs on the self-hosted `mcsvr1-ow`. Deploy is infra task 4.4. |
| 1.3 | Handler-level test for the security view's format check (`internal/server/handlers.go`, `parser.ParseFormat(cfg.LogFormat) == parser.FormatCombined`): traefik-json uses unrouted mode, combined uses suspicious-path mode | infra `docs/prod-logging.md` | TODO | Currently covered only via `TestParseFormat`; the handler line is not mutation-tested. (needs: 1.1) |
| 1.4 | Security view should use the DETECTED format, not the configured string: with `TRAIL_LOG_FORMAT=auto` on a combined (nginx) log it runs in Traefik mode | infra `docs/prod-logging.md` | TODO | Pre-existing bug, not introduced by 1.1. Expose the parser's locked format to the server. (needs: 1.1) |
| 1.5 | Flush on shutdown fails: on SIGTERM the aggregator flush runs with an already-cancelled context (`error flushing on shutdown: context canceled`, seen on the 0.3.8 -> 0.4.1 deploy 2026-10-10), so up to one flush interval (~10 s) of requests is lost on every deploy/restart. Use a fresh context with a short timeout (e.g. `context.WithTimeout(context.Background(), 5*time.Second)`) for the final flush | infra `docs/prod-logging.md` | DONE (2026-10-11) | Final flush (ctx.Done and channel-closed paths) uses a fresh 5 s context; `main` waits up to 10 s for `agg.Run` before the deferred `db.Close`. `TestRunFlushesBufferedEntriesOnShutdown`, `TestRunFlushesOnChannelCloseWithCancelledContext`; each branch's mutation red 10/10. Deploy: check the trail Nomad job `kill_timeout` is at least ~12 s (default 5 s would SIGKILL mid-flush). |
| 1.6 | Lines still queued in the tailer->aggregator channel (capacity 10,000) at shutdown are lost: the tailer saves its offset when it pushes lines into the channel, and `Run` returns on ctx.Done without draining it. Either drain the channel in the shutdown path (bounded by the shutdown timeout) or have the tailer persist the offset only after the aggregator has consumed/flushed | infra `docs/prod-logging.md` | TODO | Found in 1.5 review. Test: fill the channel, cancel, assert every queued line reaches the DB (or is re-read after restart). (needs: 1.5) |

## Phase 2: Trail's own logs

Trail writes ~9,000 stdlib `log` text lines/day in prod, almost all routine
(`tailer: processed 4 lines, new offset=...`, `flushed 10 entries to database`). Standard:
one JSON object per line, `time`/`level`/`msg`, `msg` a stable event key.

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 2.1 | Switch to `log/slog` JSON handler (text handler when not in production, or a `TRAIL_LOG_FORMAT`-style env); per-batch tailer and flush lines at DEBUG; startup, format detection, backfill results and errors at INFO/WARN/ERROR | infra `docs/prod-logging.md` | TODO | Match nectvia/havencast (slog). Tests capture output via a buffer handler. |
| 2.2 | Refuse to start when the htpasswd file is configured but unreadable (today: log warning and serve WITHOUT auth) | infra `platform/trail/README.md` "Auth" | DONE (2026-10-11) | `server.New` returns `(*Server, error)`; a configured htpasswd that is missing, unreadable or has no valid users is a startup error naming the path, with no fallback to env creds. Server built before backfill. `TestNewHtpasswdStartup` (8 cases), 5 mutations red. Operator: a bad htpasswd mount now crash-loops the Nomad job instead of serving without auth. |
