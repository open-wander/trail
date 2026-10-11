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
