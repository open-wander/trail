# Progress Log

Append-only execution log. Newest entries at the bottom.

## 2026-10-11 - Task 2.2: refuse to start on unreadable htpasswd

- Changes: `internal/server/server.go` (`New`, `setupMiddleware`, `createAuthMiddleware` return errors; htpasswd failure is fatal and never falls through to env creds), `cmd/trail/main.go` (server built right after DB open, `log.Fatalf` on error), `internal/server/server_test.go` (`TestNewHtpasswdStartup`).
- Verification: go vet, go test ./... green. Orchestrator mutation: restoring log-and-return-nil turned 5 subtests red. Advisor review: PASS (nits on a no-op `recover()` and a negative-status sentinel in the test, fixed by the orchestrator before commit).
- PATTERN: [testing] - `server.New` can be built in tests with a nil `*sql.DB` plus `trail.TemplatesFS`/`trail.StaticFS`; `srv.app.Test` then drives middleware without a database.
- PATTERN: [testing] - Pass a directory as the "unreadable file": `os.Open` succeeds and the read fails on macOS and Linux, independent of running as root.
- PATTERN: [testing] - A `recover()` in the test goroutine cannot catch a panic inside a Fiber handler run by `app.Test`; do not add one.
- PATTERN: [security] - Fail closed on a configured-but-broken auth source: return an error from construction rather than logging and skipping the middleware.
- Note: `gofmt -l` is not clean at baseline (bot_test.go, db/schema.go, queries_test.go, TestParseHtpasswd in server_test.go).

## 2026-10-11 - Task 1.5: shutdown flush with a fresh context

- Changes: `internal/aggregator/aggregator.go` (`flushOnShutdown` with `shutdownFlushTimeout` = 5 s, used on ctx.Done and channel close), `cmd/trail/main.go` (`aggDone` channel; main waits up to 10 s for the aggregator before the deferred `db.Close`), `internal/aggregator/aggregator_test.go` (two Run shutdown tests).
- Verification: go vet, go test ./... green. Advisor review: MINOR - the channel-close test hit its branch only ~half the time because both select cases are ready; the orchestrator made it repeat 50 rounds. Mutating either branch back to `a.flush(ctx)` now fails 10/10.
- Follow-ups: 1.6 (queued channel lines lost on shutdown); deploy check of Nomad `kill_timeout` (infra).
- PATTERN: [shutdown] - Final flushes after cancellation need a fresh bounded context (`context.Background` + timeout), and main must wait on the flushing goroutine before the deferred `db.Close`.
- PATTERN: [testing] - When a select has two ready cases, a test aimed at one branch is a coin flip. Repeat the scenario (cheap with in-memory SQLite) and mutation-test each branch separately with -count=N.
- PATTERN: [deploy] - An in-process shutdown wait only helps if the orchestrator's kill_timeout is longer; check the job spec alongside the code.

## 2026-10-11 - Task 1.3: handler test for the security view's format check

- Changes: new `internal/server/security_handler_test.go` (`TestSecurityThreatPatternMode`). Test-only.
- Verification: orchestrator mutations on handlers.go: `!=` red on all 4 subtests, `|| true` red on both Traefik, `&& false` red on both combined. Advisor review: PASS.
- Workflow note: the isolated worktrees were created from `origin/main`, not local HEAD, so workers did not see the unpushed 2.2/1.5 commits. The test needed adapting to `New` returning `(*Server, error)`.
- PATTERN: [testing] - ThreatPatterns unrouted mode matches only the literal router `unrouted` (the aggregator rewrites an empty router to it); seed that value.
- PATTERN: [testing] - Slice security-page assertions to the threat card ("Threat Pattern Classification" up to "Bot vs Human Traffic"): ErrorPaths lists the same paths in every mode.
- PATTERN: [testing] - Mutate a boolean expression with `|| true` / `&& false` rather than a literal, so imports stay used and the build does not fail.
- PATTERN: [norman] - Push (or base worktrees on local HEAD) before an isolated batch; otherwise workers re-implement or contradict unpushed commits.

## 2026-10-11 - Task 1.6: no lost lines in the tailer->aggregator channel on shutdown

- Changes: `internal/aggregator/aggregator.go` (drain on ctx.Done, `handleLine` extracted, no ticker/threshold flush after cancel), `internal/tailer/tailer.go` (ctx-aware send, no skip-and-advance, `sentOffset` saved; empty-line offset drift fixed), `cmd/trail/main.go` (tailer on its own ctx, stopped and awaited before the aggregator), tests in both packages.
- Merge: the worker's worktree was based on `origin/main`, so it re-implemented 1.5. Orchestrator cherry-picked onto HEAD, kept 1.5's `flushOnShutdown`/`shutdownFlushTimeout`/`aggDone`, dropped the duplicate helper, kept the shutdown log line.
- Verification: `go test -race ./...` green. Orchestrator mutations: removing the drain red 5/5; saving `readOffset` instead of `sentOffset` red 5/5. Advisor review: MINOR.
- MINOR notes (not fixed): (1) the tailer test cancels as soon as the channel fills, so a re-introduced skip with a short timeout next to the ctx case would pass; guarding it needs the channel held full longer than the skip interval. (2) the empty-line `sentOffset` branch is only incidentally covered (a following line overwrites it); a tick ending in empty lines would cover it. (3) the post-cancel ticker/threshold guards are untested (select coin flip). (4) CRLF lines and a final line without newline make the offset off by one (pre-existing).
- PATTERN: [testing] - A cancel-immediately test cannot tell "block until ctx" apart from "timeout-skip plus ctx"; hold the channel blocked longer than the skip interval before cancelling to guard against a skip regression.
- PATTERN: [testing] - When a later happy-path line overwrites the same state, a negative test for an earlier branch passes by accident; put the branch under test at the end of the input.
- PATTERN: [shutdown] - Stop producers before consumers: cancel and await the tailer, then let the aggregator drain a channel that has no sender left.

## 2026-10-11 - Task 1.4: security view uses the detected log format

- Changes: `internal/server/server.go` (`logFormat parser.Format` field, defaulted from config in `New`; `SetLogFormat`), `internal/server/handlers.go` (mode from `s.logFormat`), `cmd/trail/main.go` (`srv.SetLogFormat(p.Format())` after detection, before backfill and serving), `internal/server/security_handler_test.go` (auto + detected combined / traefik CLF / traefik JSON / undetected), README.md and INSTALL.md note.
- Rule: auto with no detection (empty/missing log at startup) stays in unrouted mode; documented, with advice to set `TRAIL_LOG_FORMAT=combined` explicitly for nginx logs.
- Verification: `go test -race ./...` green. Worker mutations: handler reverted to config string -> auto+combined red; `|| FormatAuto` -> auto undetected red. Orchestrator manual run (built binary, auto, two nginx lines): threat card shows /wp-login.php and not /about-us; with the `SetLogFormat` call removed the card is empty. Advisor review: MINOR (untested wiring - now manually verified; Deslop find-similar skipped by the worker; doc wording).
- Correction: the task note (and the orchestrator's brief) claimed auto+nginx flagged every request as a threat. Wrong: the combined parser sets Router to "server", so unrouted mode found nothing and the threat card was empty. Docs written to the real symptom.
- PATTERN: [verification] - Check a bug's stated symptom against the real data path before writing docs or briefs; a manual run disproved the "every request is a threat" claim in one step.
- PATTERN: [design] - Hand the server a value snapshot (parser.Format) set before Start rather than the *Parser, so there is no race surface if detection ever moves to runtime.

## 2026-10-11 - Phase 1 collapsed

All six Phase 1 tasks DONE; table moved to TASKS-archive.md.

## 2026-10-11 - Sweep after Phase 1

Invariants (overlay mutations, working tree never touched): swept 14: 11 guarded, 2 confirmed unguarded, 1 refuted.
- Guarded: combined format never parses JSON (TestParserCombinedRejectsJSON); traefik formats dispatch JSON (TestParserMixedFormats, backfill TestRun_MixedCLFAndJSON); JSON core-field validation, "-" router -> "" and ms duration (TestParseTraefikJSON, TestTraefikJSONMatchesCLF); detection JSON majority and Traefik family vs combined (TestDetectFormat); leading-whitespace JSON (TestIsJSONLine); empty router -> "unrouted" (TestUnroutedTrafficExcluded); SetLogFormat honoured (TestSecurityThreatPatternMode). 1.5/1.6 drain, fresh-context flush and sentOffset mutations were guarded during their tasks.
- CONFIRMED UNGUARDED (high): tailer skip-and-advance regression. Re-adding `case <-time.After(50ms): sentOffset = readOffset` beside the ctx case passes tailer and smoke tests (-count=3). Cost: silent loss of log lines whenever the aggregator is slow, the exact bug 1.6 removed. Test: hold the channel full past a short skip interval before cancelling and assert the saved offset.
- CONFIRMED UNGUARDED (medium): post-cancel flush guards in aggregator Run (`ctx.Err() == nil` on the threshold flush, `ctx.Err() != nil -> continue` on the ticker). Removing either passes aggregator and smoke tests. Cost: flush() resets the buffers before BeginTx, so a flush on a cancelled ctx drops up to 1,000 entries / one interval and Run returns before draining. Narrow window at shutdown. Test needs a deterministic hook (e.g. an injectable flush or ticker channel).
- REFUTED: empty-line sentOffset advance in the tailer. Unguarded, but the only cost is re-reading blank lines on the next tick; no data effect.
- Observation: flush() resetting buffers before the transaction means any DB error during a normal flush also drops that batch. Wider than Phase 1; not a sweep finding.

Comments: no task/phase/requirement identifiers or norman scaffolding in the Phase 1 files; no directive comments in scope. ParseLine doc (7 lines) and TestSecurityThreatPatternMode doc (8 lines) left as is: each line is a distinct claim. No edits.

## 2026-10-11 - Tasks 3.1 and 3.2: tests for the sweep's unguarded shutdown guarantees

- 3.1: `TestProcessTick_BlockedChannelNeverSkips` in `internal/tailer/tailer_test.go`; shared `assertResumed` helper (orchestrator dedup of the resume block, per review). Advisor review: MINOR (duplication, gosec not run - run by orchestrator: only the pre-existing G115 at tailer.go:88).
- 3.2: `onTick`/`onLine` extracted from `Run` (no behaviour change) plus two tests calling them with a cancelled ctx; `guardTestLine` renamed `clfCurlLine` and reused for the two inline copies. Advisor review: PASS.
- Orchestrator re-sweep (overlays): skip-and-advance, skip-without-advance, threshold guard removed, tick guard removed - all four now red. Full suite and -race -count=20 green.
- Deslop: `find-similar` unavailable in this repo (no deslop-lsp socket under .deslop/); duplication checked by grep instead.
- PATTERN: [testing] - To make one select branch testable when ctx.Done is also ready, extract the case body to an unexported method and call it directly with a cancelled ctx; an injectable ticker does not help because select still picks at random.
- PATTERN: [testing] - To catch a skip-after-timeout regression, hold the blocked state longer than (unsent lines x plausible skip interval) and assert the call has not returned.

## 2026-10-11 - Task 3.3: a failed flush keeps its batch and Run keeps going

- Changes: `internal/aggregator/aggregator.go` (`snapshot`, `takeSnapshotLocked`, `writeSnapshot`, `restore`; `maxBufferedEntries`/`maxBuffered` cap; `flushFailed` throttle; `onTick`/`onLine` log instead of returning), `internal/aggregator/aggregator_test.go` (8 new tests, `captureLog` helper; the two AfterCancel tests now also assert no flush was attempted).
- Advisor plan: REVISE. A failed threshold flush leaves bufferSize >= threshold, so every later line would retry and log (retry storm) and the cap would trip within seconds; added the `flushFailed` throttle so only the ticker retries. Also: with merge-back the AfterCancel tests became vacuous (nil error no longer proves the guard), so they now assert no flush attempt via captured logs. Never force failure by closing a `:memory:` DB (MaxOpenConns 1 destroys it); rename a table instead.
- Verification: `go test -race -count=10` and full suite green; gosec clean on the package. Orchestrator overlay mutations, all red: restore removed, cap removed, throttle removed, `return err` in onTick, `return err` in onLine, `flushFailed` never cleared, visitors not merged. Advisor review: MINOR (cap is approximate - comment added; dead `_ = logs` - removed).
- PATTERN: [resilience] - Keep-and-retry needs a retry throttle: a buffer that stays over its flush threshold after a failure re-triggers the flush on every input.
- PATTERN: [testing] - When a fix changes what an error return means, re-check every test whose only assertion is "returned nil"; add a second observable (here: no flush attempted) so the guard stays mutation-tested.

## 2026-10-11 - Phase 3 collapsed

All three Phase 3 tasks DONE; table moved to TASKS-archive.md. No separate sweep: the phase was itself the Phase 1 sweep follow-up and every task was mutation-checked by the orchestrator.

## 2026-10-11 - Task 2.1: slog JSON for Trail's own logs

- Changes: new `internal/logging` (ParseLevel, New, Init + tests); `TRAIL_LOG_LEVEL` (config `LogLevel`, README/INSTALL); every stdlib `log` call in cmd/ and internal/ replaced (87 sites); `log.Fatalf` -> `slog.Error` + `os.Exit(1)`; `server.New` returns errors for embedded template/static failures; Fiber `DisableStartupMessage: true`; aggregator test helpers `captureLog` (DEBUG JSON handler on a buffer), `logRecords`, `countMsg`, `assertDropped`.
- msg keys. INFO: trail.starting, sentry.enabled, format.detected, backfill.pending, backfill.file_imported, backfill.done, tailer.starting, tailer.position_loaded, tailer.rotation_detected, tailer.truncation_detected, tailer.stopped, geoip.loaded, retention.deleted, trail.shutting_down, server.shutting_down, trail.shutdown_complete. DEBUG: tailer.processed, aggregator.flushed, aggregator.line_skipped, retention.nothing_deleted, backfill.file_importing, overview.query, overview.stats_loaded, overview.complete. WARN: sentry.init_failed, aggregator.salt_failed, geoip.unavailable, tailer.tick_failed, server.htpasswd_invalid_line, server.htpasswd_unsupported_hash, query.partial_failed (query=...). ERROR: config.load_failed, db.open_failed, server.config_failed, server.start_failed, backfill.failed, tailer.failed, aggregator.failed, retention.failed, aggregator.flush_failed (trigger=tick|threshold|shutdown|channel_close), aggregator.buffer_dropped, retention.cleanup_failed, server.query_failed, server.render_failed, tailer.stop_timeout, aggregator.flush_timeout, server.shutdown_failed.
- Advisor review: MINOR. Orchestrator fixes: `assertDropped` pins level ERROR; the tick-failure test asserts one ERROR `aggregator.flush_failed` with trigger=tick; `trail.starting` reports the effective level (`ParseLevel(...).String()`), not the raw env string.
- Verification: `go test -race ./...` green; orchestrator overlay mutations red: buffer_dropped at DEBUG, tick key renamed, tick at WARN, restore removed, cap removed, throttle removed. Smoke run (default and debug level): stdout all JSON, stderr empty, basic-auth password absent.
- Noted, not changed: a fresh start logs `tailer.rotation_detected` from inode 0 (pre-existing behaviour, now visible as INFO).
- PATTERN: [testing] - Capture logs at DEBUG in tests so absence assertions cannot pass because a record was filtered, and pin the level explicitly where the level is part of the contract.
- PATTERN: [logging] - slog's JSON handler encodes a Stringer int type (parser.Format) as a number; pass `.String()` explicitly.

## 2026-10-11 - Task 2.3: http.request log line per request

- Changes: new `internal/logging/middleware.go` (`RequestLogger`, `From`, `RequestIDKey`) + tests; `internal/server/server.go` (middleware order: RequestLogger, recover with `server.panic` stack handler, sentry `Repanic: true`, static, basic auth); every log call in `internal/server/handlers.go` via `logging.From(c)`; `internal/server/request_log_test.go`; google/uuid promoted to a direct dependency; README/INSTALL note on `http.request` and `client_ip`.
- Decisions (advisor): uuid v4 per request, inbound X-Request-ID ignored; `duration_ms` as float ms (slog.Duration would write ns under `duration`); ERROR for >= 500, INFO otherwise (no WARN for 4xx: only 401/404/missing-param 400s exist); `client_ip` = immediate peer, no X-Forwarded-For trust. Real end-user IP would be a separate task (trusted proxies + IP validation + untrusted-peer test).
- Found beyond the spec: sentryfiber with `Repanic: false` swallowed panics and the client got 200 (logged INFO 200); without SENTRY_DSN a panic crashed the process. Fixed with fiber recover outermost-but-one and `Repanic: true`.
- Advisor review: MINOR. Orchestrator fixes: 5xx `http.request` lines carry the returned `error` (Fiber's default ErrorHandler does not log, so this was the only trace); recovered panics log `server.panic` with the panic value, stack and the request's request_id.
- Verification: `go test -race` on logging and server green; orchestrator overlay mutations all red: no error field, error field on non-5xx, panic log disabled, OriginalURL instead of Path, status mapping removed, Repanic false, inbound X-Request-ID trusted. Smoke run with the real binary: 401/200/404 logged, /static skipped, no password, base64 credentials or query secret in stdout/stderr, stderr empty.
- PATTERN: [logging] - Put the request logger outside auth and recovery, or 401s and recovered panics are logged with the wrong status or not at all; then check what the recovery layer actually returns (sentryfiber Repanic:false returns nil with status 200).
- PATTERN: [logging] - With Fiber's default ErrorHandler nothing logs a handler's returned error; the request log line is the place to carry it.

## 2026-10-11 - Phase 2 collapsed

All three Phase 2 tasks DONE; table moved to TASKS-archive.md. All phases in TASKS.md are now complete.

## 2026-10-11 - Sweep after Phase 2

Invariants (overlay mutations, tree never touched): swept 8: 4 guarded, 3 confirmed unguarded, 1 refuted (wrong direction) that exposed a bug.
- Guarded: bcrypt mismatch rejected (TestVerifyPassword); non-bcrypt htpasswd lines rejected (TestNewHtpasswdStartup, TestParseHtpasswd*); JSON handler not text (TestNewEmitsJSONAndFiltersByLevel); /static skip does not swallow /staticfoo (TestRequestLoggerStatus). 2.1/2.3 log and request-logging mutations were guarded during their tasks.
- CONFIRMED UNGUARDED (high): htpasswd Authorizer accepting any credentials. `return verifyPassword(pass, hashedPass) || true` passes every test: TestNewHtpasswdStartup only sends no credentials (401) and correct ones; no test sends a wrong password or unknown user against an htpasswd file. Cost: full auth bypass undetected. Test: valid htpasswd + wrong password and unknown user -> 401.
- CONFIRMED UNGUARDED (medium): no secrets in the startup summary. Adding `"auth_pass", cfg.AuthPass` to `trail.starting` in main passes every test (main has no tests). Cost: basic-auth password shipped to Loki. Test needs the startup attrs built by a testable function.
- CONFIRMED UNGUARDED (low): logs on stdout. `logging.Init` writing to os.Stderr passes; Nomad/Docker collect both streams, so the cost is only a standard violation.
- BUG (not a coverage gap): env-credential auth fails open. `TRAIL_AUTH_USER` set with `TRAIL_AUTH_PASS` empty or unset (or the reverse) is treated as "no auth configured": the dashboard is served without auth and only `auth=none` is logged at INFO. Same class as 2.2. Fix: one of the pair set without the other is a startup error.

Comments: no task/phase identifiers, norman scaffolding or directive comments in the Phase 2 files; no edits.

## 2026-10-11 - Phase 4 batch A: tasks 4.1, 4.2, 4.3

- 4.1: `Config.ValidateEnvAuth` (one rule, called from `config.Load` and `createAuthMiddleware`). Half-set or blank env credentials are a startup error, including alongside htpasswd; errors name the variable, never the value. README/INSTALL updated. Advisor review: PASS. Breaking for any deployment that ran with one variable set - those were serving the dashboard unauthenticated.
- 4.2: wrong-password and unknown-user cases added to `TestNewHtpasswdStartup`. Advisor review: MINOR - the spec's literal mutant `!exists && false` is equivalent (users[unknown] is "" and verifyPassword("") is false), so the worker substituted a hash-borrowing mutant.
- 4.3: `TestFailedFlushKeepsBreakdowns`. Advisor review: MINOR - a comment claimed the overlapping line catches assign-instead-of-add in the merge; false, because restore merges into empty buffers inside flush. Comment corrected by the orchestrator.
- Orchestrator verification: `go test -race ./...` green; overlay mutations all red (Load check removed, server check removed, blank allowed, Authorizer accepts all, each of four merge loops removed). Smoke run: half-set pair exits 1 with one JSON line and no password.
- PATTERN: [testing] - Check a specified mutant is not equivalent before demanding it go red: a missing-map-key branch whose zero value already fails the next check cannot be detected.
- PATTERN: [security] - Put a fail-closed rule in one function called from both the config layer and the component that enforces it, so the two cannot drift.

## 2026-10-11 - Task 4.4: log-output guarantees under test

- Changes: `startupAttrs` extracted in `cmd/trail/main.go`; new `cmd/trail/main_test.go` (TestAuthMode, TestStartupAttrs, TestStartupAttrsValues); TestInitWritesToStdout; TestProcessTick_ProcessedLoggedAtDebug; TestFlush_FlushedLoggedAtDebug; TestRun_FileImportingLoggedAtDebug; TestNew_StartupBannerDisabled.
- Advisor review: PASS (non-blocking: values beyond log_level/geoip unchecked - orchestrator added TestStartupAttrsValues comparing the full slice).
- Orchestrator overlay mutations, all red: Init to stderr, password in attrs, db_path wired to LogFile, each of three routine lines at INFO, banner on.
- v0.5.0 released (run 38156722050): tests green on Linux amd64/arm64, image built on the self-hosted runner, GitHub release created.

## 2026-10-11 - Task 4.5: shutdown wiring under test

- Changes: new `cmd/trail/shutdown.go` (`lineProducer`/`lineConsumer`, `startPipeline`, `pipeline.stop`) and `shutdown_test.go`; `main.go` uses them. tailer.go and aggregator.go unchanged.
- Advisor plan: REVISE. The brief's premise was wrong: the normal path was already lossless (all sends happen before tailDone closes, and the aggregator was only cancelled after it). The only loss window is the tailer.stop_timeout path, which no bounded wait can close; documented on `stop` and exercised (no panic) by TestPipelineTailerTimeout. Chosen design: the sole sender's goroutine closes the channel; the aggregator gets its own context and exits through its channel-close path.
- Advisor review: MINOR (doubled aggCancel on the aggregator-timeout path - orchestrator removed the in-branch call; late-send test proves no panic but does not pin that the line is unprocessed; behaviour notes below).
- Behaviour changes: `srv.Shutdown` runs after the aggregator wait (still before db.Close); if the tailer fails early, the aggregator now flushes and exits instead of idling.
- Orchestrator verification: TestPipeline* -race -count=20 green, full suite green; overlay mutations red: aggregator cancelled first, channel not closed, aggregator not awaited, tailer.stop_timeout log removed, no aggregator cancel on tailer timeout. SIGTERM smoke with 50 buffered lines: exit 0, log order tailer.stopped -> aggregator.flushed(50) -> server.shutting_down -> trail.shutdown_complete, 50 rows in the DB.
- PATTERN: [concurrency] - Let the sole sender close the channel (defer close in its own goroutine) rather than a coordinator: the close can never race a send.
- PATTERN: [process] - Briefs can state a loss that does not exist; the advisor's job is to check the happens-before chain before any code is written.

## 2026-10-11 - Phase 4 collapsed

All five Phase 4 tasks DONE; table moved to TASKS-archive.md. Every phase in TASKS.md is complete. v0.5.0 (tagged at ce224b1) contains 4.1; 4.2-4.5 are on main after it.
