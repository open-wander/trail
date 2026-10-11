package logging

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// captureDefault swaps the global slog default for a DEBUG JSON handler on a
// buffer. Tests using it must not run in parallel.
func captureDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(New(&buf, "debug"))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

func testApp() *fiber.App {
	app := fiber.New()
	app.Use(RequestLogger())
	app.Get("/ok", func(c *fiber.Ctx) error { return c.SendString("ok") })
	app.Get("/x", func(c *fiber.Ctx) error { return c.SendString("x") })
	app.Get("/static/x", func(c *fiber.Ctx) error { return c.SendString("asset") })
	app.Get("/staticfoo", func(c *fiber.Ctx) error { return c.SendString("foo") })
	app.Get("/fiberr", func(c *fiber.Ctx) error { return fiber.NewError(503, "down") })
	app.Get("/plainerr", func(c *fiber.Ctx) error { return errors.New("boom") })
	app.Get("/handlerlog", func(c *fiber.Ctx) error {
		From(c).Info("t.handler")
		return c.SendString("ok")
	})
	return app
}

func do(t *testing.T, app *fiber.App, target string) *http.Response {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("GET", target, nil))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func requestRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range decodeLines(t, buf) {
		if r["msg"] == "http.request" {
			out = append(out, r)
		}
	}
	return out
}

func TestRequestLoggerFields(t *testing.T) {
	buf := captureDefault(t)
	resp := do(t, testApp(), "/ok?a=1")

	recs := requestRecords(t, buf)
	if len(recs) != 1 {
		t.Fatalf("got %d http.request records, want 1: %s", len(recs), buf)
	}
	r := recs[0]
	for _, k := range []string{"request_id", "method", "path", "status", "duration_ms", "client_ip"} {
		if _, ok := r[k]; !ok {
			t.Errorf("missing field %q in %v", k, r)
		}
	}
	if r["path"] != "/ok" {
		t.Errorf("path = %v, want /ok (no query string)", r["path"])
	}
	if r["method"] != "GET" || r["status"] != float64(200) || r["level"] != "INFO" {
		t.Errorf("unexpected record %v", r)
	}
	if d, ok := r["duration_ms"].(float64); !ok || d < 0 {
		t.Errorf("duration_ms = %v, want number >= 0", r["duration_ms"])
	}
	if got := resp.Header.Get("X-Request-ID"); got == "" || got != r["request_id"] {
		t.Errorf("X-Request-ID header %q != request_id %v", got, r["request_id"])
	}
}

func TestRequestLoggerStatus(t *testing.T) {
	tests := []struct {
		path      string
		wantLevel string
		wantCode  float64
		wantErr   string // "error" field; empty means absent
	}{
		{"/ok", "INFO", 200, ""},
		{"/missing", "INFO", 404, ""},
		{"/fiberr", "ERROR", 503, "down"},
		{"/plainerr", "ERROR", 500, "boom"},
		{"/staticfoo", "INFO", 200, ""},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			buf := captureDefault(t)
			do(t, testApp(), tt.path)
			recs := requestRecords(t, buf)
			if len(recs) != 1 {
				t.Fatalf("got %d records, want 1", len(recs))
			}
			if recs[0]["status"] != tt.wantCode || recs[0]["level"] != tt.wantLevel {
				t.Errorf("got status=%v level=%v, want %v %s", recs[0]["status"], recs[0]["level"], tt.wantCode, tt.wantLevel)
			}
			if got, _ := recs[0]["error"].(string); got != tt.wantErr {
				t.Errorf("error field = %q, want %q", got, tt.wantErr)
			}
		})
	}
}

func TestRequestLoggerSkipsStatic(t *testing.T) {
	buf := captureDefault(t)
	resp := do(t, testApp(), "/static/x")
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if buf.Len() != 0 {
		t.Errorf("static request was logged: %s", buf)
	}
}

func TestRequestLoggerHandlerSharesRequestID(t *testing.T) {
	buf := captureDefault(t)
	do(t, testApp(), "/handlerlog")

	var reqID, handlerID any
	for _, r := range decodeLines(t, buf) {
		switch r["msg"] {
		case "http.request":
			reqID = r["request_id"]
		case "t.handler":
			handlerID = r["request_id"]
		}
	}
	if reqID == nil || reqID == "" || reqID != handlerID {
		t.Errorf("request_id mismatch: http.request=%v handler=%v", reqID, handlerID)
	}
}

func TestRequestLoggerIgnoresInboundRequestID(t *testing.T) {
	buf := captureDefault(t)
	req := httptest.NewRequest("GET", "/ok", nil)
	req.Header.Set("X-Request-ID", "attacker-value")
	resp, err := testApp().Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("X-Request-ID"); got == "attacker-value" || got == "" {
		t.Errorf("response X-Request-ID = %q, want a fresh generated id", got)
	}
	if strings.Contains(buf.String(), "attacker-value") {
		t.Errorf("inbound request id leaked into logs: %s", buf)
	}
}

func TestRequestLoggerNeverLogsQueryString(t *testing.T) {
	buf := captureDefault(t)
	do(t, testApp(), "/x?token=secret")
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("query string leaked into logs: %s", buf)
	}
}
