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
