package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func TestParseHtpasswd(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantUsers   int
		wantErr     bool
		errContains string
	}{
		{
			name: "valid bcrypt user",
			content: `testuser:$2y$05$abcdefghijklmnopqrstuv1234567890123456789012345678`,
			wantUsers: 1,
			wantErr:   false,
		},
		{
			name: "multiple valid users",
			content: `user1:$2y$05$abcdefghijklmnopqrstuv1234567890123456789012345678
user2:$2a$10$abcdefghijklmnopqrstuv1234567890123456789012345678`,
			wantUsers: 2,
			wantErr:   false,
		},
		{
			name: "skip empty lines and comments",
			content: `# This is a comment
user1:$2y$05$abcdefghijklmnopqrstuv1234567890123456789012345678

user2:$2a$10$abcdefghijklmnopqrstuv1234567890123456789012345678`,
			wantUsers: 2,
			wantErr:   false,
		},
		{
			name: "skip apr1 hash with warning",
			content: `user1:$apr1$abcdefgh$1234567890123456789012
user2:$2y$05$abcdefghijklmnopqrstuv1234567890123456789012345678`,
			wantUsers: 1,
			wantErr:   false,
		},
		{
			name:        "no valid users",
			content:     `user1:invalidhash`,
			wantUsers:   0,
			wantErr:     true,
			errContains: "no valid users",
		},
		{
			name:        "empty file",
			content:     "",
			wantUsers:   0,
			wantErr:     true,
			errContains: "no valid users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary htpasswd file
			tmpfile, err := os.CreateTemp("", "htpasswd-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(tt.content)); err != nil {
				t.Fatal(err)
			}
			if err := tmpfile.Close(); err != nil {
				t.Fatal(err)
			}

			// Parse the file
			users, err := parseHtpasswd(tmpfile.Name())

			// Check error expectations
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseHtpasswd() expected error but got none")
				}
				if tt.errContains != "" && err != nil {
					if !containsString(err.Error(), tt.errContains) {
						t.Errorf("parseHtpasswd() error = %v, want error containing %q", err, tt.errContains)
					}
				}
				return
			}

			if err != nil {
				t.Errorf("parseHtpasswd() unexpected error = %v", err)
				return
			}

			// Check user count
			if len(users) != tt.wantUsers {
				t.Errorf("parseHtpasswd() got %d users, want %d", len(users), tt.wantUsers)
			}
		})
	}
}

func TestVerifyPassword(t *testing.T) {
	// Generate a bcrypt hash for testing
	password := "testpassword"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		plaintext string
		hashed    string
		want      bool
	}{
		{
			name:      "correct password",
			plaintext: password,
			hashed:    string(hash),
			want:      true,
		},
		{
			name:      "incorrect password",
			plaintext: "wrongpassword",
			hashed:    string(hash),
			want:      false,
		},
		{
			name:      "unsupported hash format",
			plaintext: "anypassword",
			hashed:    "$apr1$abcd$1234567890",
			want:      false,
		},
		{
			name:      "invalid hash",
			plaintext: "anypassword",
			hashed:    "plaintext",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := verifyPassword(tt.plaintext, tt.hashed)
			if got != tt.want {
				t.Errorf("verifyPassword() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseHtpasswdFileNotFound(t *testing.T) {
	_, err := parseHtpasswd("/nonexistent/path/to/htpasswd")
	if err == nil {
		t.Error("parseHtpasswd() expected error for nonexistent file but got none")
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestNewHtpasswdStartup(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "nope")
	noUsers := write("nousers", "# comment\nnocolonline\nbob:$apr1$abc$def\n")
	valid := write("valid", "alice:"+string(hash)+"\n")

	tests := []struct {
		name     string
		cfg      config.Config
		wantErr  string // substring; empty means no error expected
		wantAuth bool   // when started: requests without credentials get 401
	}{
		{name: "missing file", cfg: config.Config{HtpasswdFile: missing}, wantErr: missing},
		{name: "unreadable (directory)", cfg: config.Config{HtpasswdFile: dir}, wantErr: dir},
		{name: "no valid users", cfg: config.Config{HtpasswdFile: noUsers}, wantErr: "no valid users"},
		{name: "no valid users path named", cfg: config.Config{HtpasswdFile: noUsers}, wantErr: noUsers},
		{
			name:    "broken htpasswd does not fall back to env creds",
			cfg:     config.Config{HtpasswdFile: missing, AuthUser: "u", AuthPass: "p"},
			wantErr: missing,
		},
		{name: "valid file", cfg: config.Config{HtpasswdFile: valid}, wantAuth: true},
		{name: "no auth configured", cfg: config.Config{}},
		{name: "env creds", cfg: config.Config{AuthUser: "u", AuthPass: "p"}, wantAuth: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			srv, err := New(&cfg, nil, trail.TemplatesFS, trail.StaticFS)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected startup error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tt.wantErr)
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
					t.Fatalf("no-credentials status = %d, want %d", got, http.StatusUnauthorized)
				}
				user, pass := "alice", "secret"
				if tt.cfg.AuthUser != "" {
					user, pass = tt.cfg.AuthUser, tt.cfg.AuthPass
				}
				if c := status(user, pass); c == http.StatusUnauthorized {
					t.Fatalf("valid credentials still got 401")
				}
			} else if got == http.StatusUnauthorized {
				t.Fatalf("no auth configured but request got 401")
			}
		})
	}
}
