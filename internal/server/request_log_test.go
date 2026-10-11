package server

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/config"
	"github.com/open-wander/trail/internal/logging"
	_ "modernc.org/sqlite"
)

// captureSlog swaps the global slog default for a DEBUG JSON handler on a
// buffer. Tests using it must not run in parallel.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(logging.New(&buf, "debug"))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func findRecords(recs []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, r := range recs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

func randHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func TestRequestLogAuthFailuresNeverLogCredentials(t *testing.T) {
	pass := "p@ss-" + randHex(t)
	srv, err := New(&config.Config{AuthUser: "alice", AuthPass: pass}, nil, trail.TemplatesFS, trail.StaticFS)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		user, pass string
		want       int
	}{
		// alice's real password under another username: would be accepted if
		// only the password were checked.
		{"wrong user right password", "bob", pass, 401},
		{"right user wrong password", "alice", pass + "x", 401},
		{"valid credentials", "alice", pass, 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureSlog(t)
			req := httptest.NewRequest("GET", "/api/filters", nil)
			req.SetBasicAuth(tt.user, tt.pass)
			resp, err := srv.app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tt.want {
				t.Fatalf("response status = %d, want %d", resp.StatusCode, tt.want)
			}

			recs := findRecords(decodeRecords(t, buf), "http.request")
			if len(recs) != 1 || recs[0]["status"] != float64(tt.want) {
				t.Fatalf("want one http.request with status %d, got %v", tt.want, recs)
			}
			out := buf.String()
			header := req.Header.Get("Authorization")
			for _, secret := range []string{
				pass,
				base64.StdEncoding.EncodeToString([]byte(tt.user + ":" + tt.pass)),
				header,
			} {
				if strings.Contains(out, secret) {
					t.Errorf("log output contains credential material %q: %s", secret, out)
				}
			}
		})
	}
}

func TestRequestLogSkipsStatic(t *testing.T) {
	srv, err := New(&config.Config{}, nil, trail.TemplatesFS, trail.StaticFS)
	if err != nil {
		t.Fatal(err)
	}
	buf := captureSlog(t)
	resp, err := srv.app.Test(httptest.NewRequest("GET", "/static/favicon.svg", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("static status = %d", resp.StatusCode)
	}
	if buf.Len() != 0 {
		t.Errorf("static request was logged: %s", buf)
	}
}

// A handler failing mid-request must log with the same request_id as the
// http.request line. This exercises the real handlers.go call sites.
func TestRequestLogHandlerLinesShareRequestID(t *testing.T) {
	// A database with no schema makes every query fail.
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	srv, err := New(&config.Config{}, database, trail.TemplatesFS, trail.StaticFS)
	if err != nil {
		t.Fatal(err)
	}

	buf := captureSlog(t)
	resp, err := srv.app.Test(httptest.NewRequest("GET", "/api/panel/referrers", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}

	recs := decodeRecords(t, buf)
	reqs := findRecords(recs, "http.request")
	fails := findRecords(recs, "server.query_failed")
	if len(reqs) != 1 || len(fails) != 1 {
		t.Fatalf("want one http.request and one server.query_failed, got %v", recs)
	}
	if reqs[0]["status"] != float64(500) || reqs[0]["level"] != "ERROR" {
		t.Errorf("http.request = %v, want status 500 level ERROR", reqs[0])
	}
	if id := reqs[0]["request_id"]; id == nil || id == "" || id != fails[0]["request_id"] {
		t.Errorf("request_id mismatch: http.request=%v handler=%v", reqs[0]["request_id"], fails[0]["request_id"])
	}
}

// Panics must come out as a logged 500 whether or not Sentry is configured.
func TestRequestLogPanicIsLoggedAs500(t *testing.T) {
	for _, dsn := range []string{"", "https://public@localhost/1"} {
		t.Run("dsn="+dsn, func(t *testing.T) {
			t.Setenv("SENTRY_DSN", dsn)
			srv, err := New(&config.Config{}, nil, trail.TemplatesFS, trail.StaticFS)
			if err != nil {
				t.Fatal(err)
			}
			srv.app.Get("/boom", func(c *fiber.Ctx) error { panic(errors.New("kaboom")) })

			buf := captureSlog(t)
			resp, err := srv.app.Test(httptest.NewRequest("GET", "/boom", nil))
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("response status = %d, want 500", resp.StatusCode)
			}
			all := decodeRecords(t, buf)
			recs := findRecords(all, "http.request")
			if len(recs) != 1 || recs[0]["status"] != float64(500) || recs[0]["level"] != "ERROR" {
				t.Fatalf("want one ERROR http.request with status 500, got %v", recs)
			}
			if e, _ := recs[0]["error"].(string); !strings.Contains(e, "kaboom") {
				t.Errorf("http.request error = %q, want it to contain kaboom", e)
			}
			panics := findRecords(all, "server.panic")
			if len(panics) != 1 || panics[0]["request_id"] != recs[0]["request_id"] {
				t.Fatalf("want one server.panic with the request's request_id, got %v", panics)
			}
			if st, _ := panics[0]["stack"].(string); !strings.Contains(st, "goroutine") {
				t.Errorf("server.panic stack missing: %q", st)
			}
		})
	}
}
