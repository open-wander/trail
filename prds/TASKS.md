# Tasks

Single source of truth for active/pending tasks. Status lives here only.
Source: TydeWhatMay/infra `docs/prod-logging.md` (production logging review, 2026-10-09).
Trail is the analytics for the TydeWhatMay/open-wander estate (stats.openwander.org) and
tails the platform Traefik's access log.

## Phase 1: Traefik JSON access log — 6 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))

## Phase 2: Trail's own logs — 3 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))

## Phase 3: Shutdown and flush hardening — 3 tasks completed 2026-10-11 (see [TASKS-archive.md](TASKS-archive.md))

## Phase 4: Verify gaps

From `norman verify` and the Phase 2 sweep, 2026-10-11 (see verification.md). 4.1 must land before the next release tag.

| # | Task | PRD | Status | Notes |
|---|------|-----|--------|-------|
| 4.1 | Env-credential auth must fail closed: `TRAIL_AUTH_USER` set without `TRAIL_AUTH_PASS` (or the reverse) is a startup error, not "no auth". Fix + tests | verification.md gap 1 | ACTIVE | Today it serves the dashboard unauthenticated and logs `auth=none`. Tests in a new `internal/server/env_auth_test.go`. |
| 4.2 | Test that a valid htpasswd file rejects a wrong password and an unknown user (401). Test-only | verification.md gap 2 | ACTIVE | An Authorizer returning true for anything passes every test today. Tests in `internal/server/server_test.go`. |
| 4.3 | Test that a failed flush's merge-back keeps referrers, browsers, os_stats and countries (not only requests, visitors, user_agents, duration_hist). Test-only | verification.md gap 3 | ACTIVE | Dropping those merges passes today. |
| 4.4 | Make log output testable: stdout destination, routine lines at DEBUG, Fiber banner off, and no secrets in `trail.starting` (build the startup attrs in a testable function) | verification.md gap 4 | TODO | (needs: 4.1 - both touch main.go) |
| 4.5 | Cover main's shutdown wiring (tailer before aggregator, aggregator before db.Close) and close the drain edge: a tailer that misses its 3 s stop timeout can send after the aggregator's drain | verification.md gap 5 | TODO | (needs: 4.4) |
