package server

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/config"
	"github.com/open-wander/trail/internal/parser"
)

const (
	sampleCombined = `192.168.1.1 - - [10/Jan/2026:13:55:36 +0000] "GET / HTTP/1.1" 200 100 "-" "Chrome"`
	sampleTraefik  = `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla" 1 "web@docker" "http://172.19.0.4:80" 1ms`
	sampleJSON     = `{"ClientHost":"192.168.65.1","DownstreamStatus":404,"RequestMethod":"GET","RequestPath":"/.env","RouterName":"echo@file","StartUTC":"2026-10-09T21:07:16.897593508Z","time":"2026-10-09T21:07:16Z"}`
)

// TestSecurityThreatPatternMode drives the security summary partial and checks
// which threat patterns each log format reports. Combined uses suspicious-path
// mode (status >= 400, any router); Traefik formats use unrouted mode
// (router = 'unrouted'). Each seeded row is visible to exactly one mode.
//
// With auto, the mode follows the format the parser detected from sample
// lines at startup. If detection never ran (empty or missing log), the view
// stays in unrouted mode.
func TestSecurityThreatPatternMode(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		sample      []string // when set, run real detection and pass the result to the server
		wantLabel   string
		wantPath    string
		absentLabel string
		absentPath  string
	}{
		{"combined", "combined", nil, "WordPress", "/wp-login.php", "Environment", "/.env"},
		{"combined upper case", "COMBINED", nil, "WordPress", "/wp-login.php", "Environment", "/.env"},
		{"traefik clf", "traefik", nil, "Environment", "/.env", "WordPress", "/wp-login.php"},
		{"traefik json", "traefik-json", nil, "Environment", "/.env", "WordPress", "/wp-login.php"},
		{"auto detected combined", "auto", []string{sampleCombined, sampleCombined}, "WordPress", "/wp-login.php", "Environment", "/.env"},
		{"auto detected traefik clf", "auto", []string{sampleTraefik, sampleTraefik}, "Environment", "/.env", "WordPress", "/wp-login.php"},
		{"auto detected traefik json", "auto", []string{sampleJSON, sampleJSON}, "Environment", "/.env", "WordPress", "/wp-login.php"},
		{"auto undetected", "auto", nil, "Environment", "/.env", "WordPress", "/wp-login.php"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Keep a developer's SENTRY_DSN from adding the Sentry middleware.
			t.Setenv("SENTRY_DSN", "")
			db := testDB(t)
			seedRequests(t, db,
				// Combined only: routed, status >= 400.
				requestRow{Hour: "2026-02-08T09:00:00Z", Router: "blog", Path: "/wp-login.php", Method: "GET", Status: 404, Count: 7},
				// Unrouted only: status below 400.
				requestRow{Hour: "2026-02-08T09:00:00Z", Router: "unrouted", Path: "/.env", Method: "GET", Status: 200, Count: 3},
			)

			srv, err := New(&config.Config{LogFormat: tt.format}, db, trail.TemplatesFS, trail.StaticFS)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if tt.sample != nil {
				p := parser.NewParser(tt.format)
				p.Detect(tt.sample)
				srv.SetLogFormat(p.Format())
			}

			req := httptest.NewRequest("GET", "/api/security?tab=summary&range=custom&custom_from=2026-02-01&custom_to=2026-02-28", nil)
			resp, err := srv.app.Test(req, -1)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			// Other sections list these paths regardless of mode, so only
			// inspect the threat card.
			body := string(raw)
			start := strings.Index(body, "Threat Pattern Classification")
			end := strings.Index(body, "Bot vs Human Traffic")
			if start < 0 || end < start {
				t.Fatalf("threat card not found in body (start=%d end=%d)", start, end)
			}
			card := body[start:end]

			if !strings.Contains(card, `chart-row-label">`+tt.wantLabel+`<`) {
				t.Errorf("threat card missing category %q:\n%s", tt.wantLabel, card)
			}
			if !strings.Contains(card, tt.wantPath) {
				t.Errorf("threat card missing example path %q", tt.wantPath)
			}
			if strings.Contains(card, `chart-row-label">`+tt.absentLabel+`<`) {
				t.Errorf("threat card unexpectedly has category %q", tt.absentLabel)
			}
			if strings.Contains(card, tt.absentPath) {
				t.Errorf("threat card unexpectedly has path %q", tt.absentPath)
			}
		})
	}
}
