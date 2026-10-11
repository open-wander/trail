package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/config"
)

// New is called directly (skipping config.Load) so this exercises the
// server's own fail-closed check for a half-set env credential pair.
func TestNewEnvAuthPair(t *testing.T) {
	pass := "p@ss-" + "4d8e2b61c0a7"
	tests := []struct {
		name     string
		cfg      config.Config
		wantErr  string // substring; empty means the server must start
		wantAuth bool
	}{
		{name: "user only", cfg: config.Config{AuthUser: "admin"}, wantErr: "TRAIL_AUTH_PASS"},
		{name: "pass only", cfg: config.Config{AuthPass: pass}, wantErr: "TRAIL_AUTH_USER"},
		{name: "blank pass", cfg: config.Config{AuthUser: "admin", AuthPass: "  "}, wantErr: "blank"},
		{name: "both set", cfg: config.Config{AuthUser: "admin", AuthPass: pass}, wantAuth: true},
		{name: "neither set", cfg: config.Config{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			srv, err := New(&cfg, nil, trail.TemplatesFS, trail.StaticFS)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q does not contain %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), pass) {
					t.Errorf("error leaks the password value: %q", err)
				}
				if srv != nil {
					t.Fatal("server must be nil on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// /static is mounted before auth, so probe an API route.
			status := func(user, pass string) int {
				req := httptest.NewRequest("GET", "/api/filters", nil)
				if user != "" {
					req.SetBasicAuth(user, pass)
				}
				r, err := srv.app.Test(req)
				if err != nil {
					return http.StatusInternalServerError
				}
				return r.StatusCode
			}
			got := status("", "")
			if tt.wantAuth {
				if got != http.StatusUnauthorized {
					t.Fatalf("no-credentials status = %d, want 401", got)
				}
				if c := status(tt.cfg.AuthUser, tt.cfg.AuthPass); c == http.StatusUnauthorized {
					t.Fatal("valid credentials still got 401")
				}
			} else if got == http.StatusUnauthorized {
				t.Fatal("no auth configured but request got 401")
			}
		})
	}
}
