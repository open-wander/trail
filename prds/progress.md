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
