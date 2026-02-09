package server

import (
	"os"
	"testing"

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
