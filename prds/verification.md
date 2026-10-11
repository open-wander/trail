# Verification

Date: 2026-10-11. HEAD 5c000db. Rigor: single (one Opus verifier per requirement group, read-only, overlay mutations allowed). Requirements: the 12 DONE task rows in TASKS-archive.md, checked against code and tests; logging tasks also against TydeWhatMay infra docs/prod-logging.md.

Result: 7 PASS, 5 PARTIAL, 0 FAIL.

| Req | Verdict |
|-----|---------|
| 1.1 | PASS |
| 1.2 | PASS |
| 1.3 | PASS |
| 1.4 | PASS |
| 1.5 | PARTIAL |
| 1.6 | PARTIAL |
| 2.1 | PARTIAL |
| 2.2 | PARTIAL |
| 2.3 | PASS |
| 3.1 | PASS |
| 3.2 | PASS |
| 3.3 | PARTIAL |

## Gaps

1. **2.2 - env-credential auth fails open (bug, high).** `TRAIL_AUTH_USER` set with `TRAIL_AUTH_PASS` empty, or the reverse, starts without error and serves the dashboard unauthenticated (`createAuthMiddleware`, `authMode` reports `none`). 2.2's goal - no configured-but-broken auth source yields an open dashboard - is not met for env credentials.
2. **2.2 - htpasswd rejection untested (coverage, high).** An Authorizer that accepts any credentials passes every test; no test sends a wrong password or unknown user against an htpasswd file.
3. **3.3 - merge-back only partly guarded (coverage, medium).** Dropping the restore merge for referrers, browsers, os_stats or countries passes; tests only pin requests, visitors, user_agents, duration_hist.
4. **2.1 - log destination and levels untested (coverage, medium).** stdout vs stderr, the DEBUG level of `tailer.processed`/`aggregator.flushed`, the disabled banner, and "no secrets in `trail.starting`" (main has no tests) are unguarded.
5. **1.5/1.6 - main shutdown wiring untested; one edge (coverage, low).** Tailer-before-aggregator ordering and the wait on `aggDone` before `db.Close` are verified by reading only; the 5 s timeout bound is untested. Edge: if the tailer misses its 3 s stop timeout, a send landing after the aggregator's non-blocking drain is lost while its offset is saved.

Also: the 1.1 archive note "not merged" is stale; v0.4.1 contains the branch and is on main. Whether the v0.4.1 image actually published was not checked (GitHub not contacted).

## Evidence

### 1.1 - PASS

Impl: internal/parser/traefik_json.go:35 ParseTraefikJSON -> same LogEntry (core-field check, '-'->'' , ns->ms). parser.go:103-105 ParseLine: `p.format != FormatCombined && isJSONLine(line)` -> JSON, else CLF (mixed files/backfill). detect.go:7-38 Traefik CLF+JSON vs Combined, JSON needs strict majority. aggregator.go:126-127 nil parser -> NewParser("traefik") (accepts JSON). Security view: handlers.go:1184 mode from s.logFormat; main.go:77-86 SetLogFormat(p.Format()) after detection. Docs: README.md:64,79-102 (defaultmode=drop, keep only User-Agent/Referer, warns defaultmode=keep logs Cookie/Authorization); the infra docs/prod-logging.md is in another repo and was not checked. Tests all PASS (-count=1): parser TestParseTraefikJSON, TestTraefikJSONMatchesCLF, TestParserMixedFormats, TestParserCombinedRejectsJSON, TestParserTraefikRejectsMalformedJSON, TestParseFormat, TestIsJSONLine, TestDetectFormat, TestParserDetect; backfill TestRun_MixedCLFAndJSON (4 formats); aggregator TestNewNilParserAcceptsTraefikJSON; server TestSecurityThreatPatternMode (8 subtests incl. auto_detected_traefik_json). The progress.md sweep (lines 58-60) lists these as guarded by mutation. The main.go wiring has no unit test; progress.md:48 records only a manual run. git status unchanged.

### 1.2 - PASS

`git tag -l v0.4*` -> v0.4.0, v0.4.1. v0.4.1 is an annotated tag (cat-file -t = tag) on 0be9e2f 'ci(release): build the image on the self-hosted runner'. `merge-base --is-ancestor v0.4.1 HEAD` -> true, so the tag is on main. `merge-base --is-ancestor bbf65cf v0.4.1` -> true, so the 1.1 branch head is in the tag. The 1.1 archive note 'not merged' is out of date. In `git show v0.4.1:.github/workflows/release.yml`, the docker job runs on `[self-hosted, linux, x64]` (line 53), needs build, and pushes ghcr.io/${{github.repository}}:<version>+latest for linux/amd64,arm64 through docker/build-push-action@v6. The binary and release jobs stay on ubuntu-latest. The tag push triggers the workflow (`tags: ["v*"]`). GitHub was not contacted, so whether the image actually published is unverified.

### 1.3 - PASS

Test: TestSecurityThreatPatternMode (internal/server/security_handler_test.go:28). It is a table test that seeds one row that only combined mode can see (router=blog, 404, /wp-login.php) and one row that only unrouted mode can see (router=unrouted, 200, /.env) (lines 53-58). It calls GET /api/security?tab=summary through srv.app.Test and checks the 'Threat Pattern Classification' card for both the label or path it expects and the one it must not show (lines 87-105). Cases: combined, COMBINED, traefik, traefik-json (lines 38-41). Implementation: internal/server/handlers.go:1184 `suspiciousPathMode := s.logFormat == parser.FormatCombined`, passed to ThreatPatterns. Run: `go test ./internal/server -run TestSecurityThreatPatternMode -count=1 -v` gives `--- PASS: TestSecurityThreatPatternMode`, ok. Each row shows up in exactly one mode, so flipping the mode for any format would fail the checks for both the expected and the absent label and path.

### 1.4 - PASS

Impl: Server.logFormat field (server.go:38), initialised from parser.ParseFormat(cfg.LogFormat) (server.go:140), with SetLogFormat at server.go:43. In main, cmd/trail/main.go:74-86 runs p.Detect on the first 10 lines when the format is auto, then calls srv.SetLogFormat(p.Format()). This happens before srv.Start (main.go:150). Handler reads s.logFormat (handlers.go:1181-1184). Tests: the auto cases in TestSecurityThreatPatternMode (test:42-45) run the real parser.NewParser('auto').Detect(samples) followed by SetLogFormat. 'auto detected combined' expects WordPress, i.e. suspicious-path mode. 'auto undetected' expects unrouted mode. All pass. Mutation check (overlay at /private/tmp/verif-sec-*): replacing handlers.go:1184 with ParseFormat(s.config.LogFormat)==FormatCombined, meaning the configured string, makes '--- FAIL: TestSecurityThreatPatternMode/auto_detected_combined' fail, so the test guards the detected-format requirement. Docs: README.md:71 and INSTALL.md:87 describe the undetected-auto fallback. Caveat: cmd/trail has no tests, so the SetLogFormat call in main.go:86 is checked by reading only. The test reproduces the same Detect-then-SetLogFormat sequence.

### 1.5 - PARTIAL

Implemented. internal/aggregator/aggregator.go:34-36 sets shutdownFlushTimeout=5s. :168-172 flushOnShutdown uses context.WithTimeout(context.Background(), ...). It is called on the ctx.Done path (:197) and the channel-close path (:211). cmd/trail/main.go:124-127 closes aggDone when agg.Run returns, and :182-186 waits on aggDone (10s cap) before main returns and runs the deferred database.Close at :63. Tests pass (go test ./internal/aggregator -run ... -count=1 -> ok): TestRunFlushesBufferedEntriesOnShutdown, TestRunFlushesOnChannelCloseWithCancelledContext, TestOnLineThresholdAfterCancelKeepsEntries, TestOnTickAfterCancelKeepsEntries. Mutation via -overlay: changing the channel-close path to a.flush(ctx) fails TestRunFlushesOnChannelCloseWithCancelledContext ('round 1: Run returned context canceled'). Changing the ctx.Done path fails 4 tests. Gaps: nothing tests main's wait on aggDone before DB close (cmd/trail has no tests). Nothing tests the timeout bound itself (a Background ctx with no timeout would also pass).

### 1.6 - PARTIAL

Implemented. aggregator.go:183-196 drains queued lines without blocking before flushOnShutdown. tailer.go:131-166: blocking select on lines<-line or ctx.Done with no skip path; sentOffset advances only on a send or an empty line; savePosition(sentOffset). main.go:112-121 tailCtx/tailDone; :161-169 tailCancel and wait on tailDone, then cancel() at :172. Tests pass: TestRunDrainsQueuedLinesOnCancel, TestProcessTick_CancelDoesNotAdvancePastUnsentLines, TestProcessTick_BlockedChannelNeverSkips. Mutations: removing the drain fails TestRunDrainsQueuedLinesOnCancel. Saving readOffset fails both tailer tests ('saved offset = 13, want 9'). Gaps: main's tailer-before-aggregator ordering is untested. Edge case: if the tailer misses the 3s stop timeout (main.go:167), cancel() runs anyway. A send that lands after the non-blocking drain is then lost even though its offset is saved.

### 2.1 - PARTIAL

Implemented: internal/logging/logger.go:14-16 Init sets slog.NewJSONHandler(os.Stdout) as default; ParseLevel :25-35 (unknown/empty -> info); cmd/trail/main.go:29 Init(os.Getenv("TRAIL_LOG_LEVEL")); config.go:34 default "info"; server.go:51 DisableStartupMessage:true. Per-batch DEBUG: aggregator.go:662 aggregator.flushed, tailer.go:163 tailer.processed, backfill.go:69. grep found no "log" import, log.Print/Fatal, fmt.Print or os.Stderr in non-test code; every literal msg matches ^[a-z_]+(\.[a-z_]+)+$; ~86 call sites (claim 87, within grep error). Tests: TestParseLevel, TestNewEmitsJSONAndFiltersByLevel PASS (go test ./internal/logging -count=1). Untested: stdout destination (known gap a: stderr passes); per-batch lines being DEBUG (no test references aggregator.flushed/tailer.processed); banner disabled (no test); main has no tests (known gap b: password in trail.starting passes).

### 2.2 - PARTIAL

Implemented: internal/server/server.go:174-179 createAuthMiddleware returns an error for any parseHtpasswd failure (open error :210, no valid users :255), with no fallback; setupMiddleware:160-163 passes it up; cmd/trail/main.go:67-71 calls os.Exit(1). TestNewHtpasswdStartup (server_test.go:186) has 8 cases: missing, directory, no valid users x2, broken file plus env creds, valid, none, env. Run: `go test ./internal/server -run TestNewHtpasswdStartup -count=1` gives ok, all 8 pass. Gaps confirmed with -overlay, no repo edits: (a) mutant `if !exists && false` plus `verifyPassword(...) || true` makes the htpasswd Authorizer accept anything, and the whole server package still passes. The 'valid file' case checks only no-creds=401 and right-creds!=401, never a wrong password or unknown user. (b) Overlay test: AuthUser='u' with AuthPass='' (and the reverse) starts with no error and GET /api/filters returns 200 unauthenticated (server.go:194; main.go:200 authMode reports 'none'). So the stated goal, that no configured-but-broken auth source gives an unauthenticated dashboard, is not met. git status unchanged.

### 2.3 - PASS

internal/logging/middleware.go:42-87 RequestLogger: fresh uuid (:44, inbound header never read), c.Path() (no query), status from fiber.Error or 500 (:57-64), duration_ms, client_ip, ERROR+error for >=500 (:74-82), static skip (:30,51). From(c) :21-26 adds request_id; used at handlers.go:189..1139. server.go:56 registered outermost; :60-64 recover logs server.panic via From(c) with stack; :70 sentryfiber Repanic:true inside recover. Tests PASS (-count=1): logging TestRequestLoggerFields, ...Status (404 INFO, 503/500 ERROR with error), ...SkipsStatic (+/staticfoo logged), ...HandlerSharesRequestID, ...IgnoresInboundRequestID, ...NeverLogsQueryString; server TestRequestLogAuthFailuresNeverLogCredentials, TestRequestLogSkipsStatic, TestRequestLogHandlerLinesShareRequestID (real handler server.query_failed), TestRequestLogPanicIsLoggedAs500 (with and without SENTRY_DSN; 500, error contains kaboom, server.panic same request_id, stack present). git status unchanged.

### 3.1 - PASS

internal/tailer/tailer_test.go:413 TestProcessTick_BlockedChannelNeverSkips. It fills a cap-1 channel nobody reads and asserts processTick stays blocked for 250ms with no position saved (offset 0). After cancel it asserts a ctx error, that only [aaa] was received, that saved offset = 4, and that resume delivers [bbb ccc ddd]. Run: '--- PASS: TestProcessTick_BlockedChannelNeverSkips (0.26s)'. Overlay mutation adding a skip-after-100ms case (sentOffset=readOffset; continue) to tailer.go makes it fail at tailer_test.go:472 ('saved offset = 12, want 4'). Saving readOffset instead of sentOffset also fails it ('saved offset = 8, want 4'). Limit: a skip timeout much longer than the test's 250ms hold would not be exercised. git status is unchanged (only M prds/progress.md and ?? examples/).

### 3.2 - PASS

Run's cases are extracted into onTick (internal/aggregator/aggregator.go:227-236, guard `if ctx.Err() != nil { return nil }` at 228) and onLine (240-257, guard `ctx.Err() == nil` at 251). Run calls them at 204 and 218. The tests are TestOnLineThresholdAfterCancelKeepsEntries (aggregator_test.go:713) and TestOnTickAfterCancelKeepsEntries (:738). Each one calls the helper directly with a ctx that is already cancelled, so there is no timing dependency. Each asserts zero aggregator.flush_failed logs (captured at DEBUG), checks that bufferSize is kept, and checks that the shutdown flush writes every entry (assertShutdownKeepsEntries). `go test -race -count=3 -run ...` passed 3/3 for both. I ran overlay mutations from /private/tmp/verify32_33. Removing the onTick guard turns TestOnTickAfterCancelKeepsEntries red. Dropping `ctx.Err() == nil` from onLine turns TestOnLineThresholdAfterCancelKeepsEntries red. git status is unchanged.

### 3.3 - PARTIAL

Implemented in aggregator.go. flush (471-488) takes a snapshot, then writeSnapshot. On error it calls restore(s) and sets flushFailed=true. restore (426-466) applies the cap check `bufferSize+s.size > maxBuffered`, logs slog.Error aggregator.buffer_dropped and returns, otherwise merges all 8 maps. onTick and onLine log failures and return nil (231-235, 252-254). The throttle is `!a.flushFailed` at 251. maxBufferedEntries=100,000 (32). The 8 tests (aggregator_test.go:915-1200) all passed under -race -count=3. Overlay mutations that went red: cap `>` changed to `>=` (TestFailedFlushCap); drop logged at WARN (TestFailedFlushCap, TestRestoreCapCountsLiveBuffer); request Bytes not summed (TestRestoreMergesIntoLiveBuffer). Gap: removing the merge-back loop in restore for referrers, browsers, osStats or countries stays ALL GREEN. chromeLine sends referer "-", no test has a GeoIP DB, and no test asserts browsers or os_stats after a retry. So 'no loss' is only enforced for requests, visitors, user_agents and duration_hist. Documented known limit: a Commit that errors after applying would double-count on retry.

## Follow-up (2026-10-11)

All five gaps were turned into Phase 4 and closed: 4.1 (`82c2843`, in v0.5.0) env-credential pair fails closed; 4.2 (`6b1a0c7`) htpasswd wrong-password and unknown-user tests; 4.3 (`ce224b1`) merge-back tests for every buffer; 4.4 (`a9a30b6`) log destination, levels, banner and startup-secret tests; 4.5 (`c4e30be`) shutdown pipeline extracted and tested. Remaining by design: a tailer that misses its 3 s stop timeout can still hand over a line after the aggregator has finished (documented on `pipeline.stop`).
