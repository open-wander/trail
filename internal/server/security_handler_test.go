package server

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/config"
)

// TestSecurityThreatPatternMode drives the security summary partial and checks
// which threat patterns each log format reports. Combined uses suspicious-path
// mode (status >= 400, any router); Traefik formats use unrouted mode
// (router = 'unrouted'). Each seeded row is visible to exactly one mode.
//
// The auto format is deliberately not covered: the format source changes when
// detected-format handling lands (task 1.4), which owns that case.
func TestSecurityThreatPatternMode(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		wantLabel   string
		wantPath    string
		absentLabel string
		absentPath  string
	}{
		{"combined", "combined", "WordPress", "/wp-login.php", "Environment", "/.env"},
		{"combined upper case", "COMBINED", "WordPress", "/wp-login.php", "Environment", "/.env"},
		{"traefik clf", "traefik", "Environment", "/.env", "WordPress", "/wp-login.php"},
		{"traefik json", "traefik-json", "Environment", "/.env", "WordPress", "/wp-login.php"},
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
