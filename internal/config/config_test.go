package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		want    *Config
		wantErr bool
	}{
		{
			name:    "all defaults",
			envVars: map[string]string{},
			want: &Config{
				LogFile:       "/logs/access.log",
				DBPath:        "/data/trail.db",
				Listen:        ":8080",
				RetentionDays: 90,
				HtpasswdFile:  "",
				AuthUser:      "",
				AuthPass:      "",
			},
			wantErr: false,
		},
		{
			name: "all custom values",
			envVars: map[string]string{
				"TRAIL_LOG_FILE":       "/custom/access.log",
				"TRAIL_DB_PATH":        "/custom/trail.db",
				"TRAIL_LISTEN":         ":3000",
				"TRAIL_RETENTION_DAYS": "30",
				"TRAIL_HTPASSWD_FILE":  "/etc/htpasswd",
				"TRAIL_AUTH_USER":      "admin",
				"TRAIL_AUTH_PASS":      "secret",
			},
			want: &Config{
				LogFile:       "/custom/access.log",
				DBPath:        "/custom/trail.db",
				Listen:        ":3000",
				RetentionDays: 30,
				HtpasswdFile:  "/etc/htpasswd",
				AuthUser:      "admin",
				AuthPass:      "secret",
			},
			wantErr: false,
		},
		{
			name: "invalid retention days - not a number",
			envVars: map[string]string{
				"TRAIL_RETENTION_DAYS": "invalid",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid retention days - zero",
			envVars: map[string]string{
				"TRAIL_RETENTION_DAYS": "0",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "invalid retention days - negative",
			envVars: map[string]string{
				"TRAIL_RETENTION_DAYS": "-10",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "partial config with htpasswd only",
			envVars: map[string]string{
				"TRAIL_HTPASSWD_FILE": "/etc/htpasswd",
			},
			want: &Config{
				LogFile:       "/logs/access.log",
				DBPath:        "/data/trail.db",
				Listen:        ":8080",
				RetentionDays: 90,
				HtpasswdFile:  "/etc/htpasswd",
				AuthUser:      "",
				AuthPass:      "",
			},
			wantErr: false,
		},
		{
			name: "partial config with basic auth only",
			envVars: map[string]string{
				"TRAIL_AUTH_USER": "admin",
				"TRAIL_AUTH_PASS": "secret",
			},
			want: &Config{
				LogFile:       "/logs/access.log",
				DBPath:        "/data/trail.db",
				Listen:        ":8080",
				RetentionDays: 90,
				HtpasswdFile:  "",
				AuthUser:      "admin",
				AuthPass:      "secret",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all relevant environment variables before each test
			clearEnv := []string{
				"TRAIL_LOG_FILE",
				"TRAIL_DB_PATH",
				"TRAIL_LISTEN",
				"TRAIL_RETENTION_DAYS",
				"TRAIL_HTPASSWD_FILE",
				"TRAIL_AUTH_USER",
				"TRAIL_AUTH_PASS",
			}
			for _, key := range clearEnv {
				os.Unsetenv(key)
			}

			// Set test-specific environment variables
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			// Clean up after test
			defer func() {
				for _, key := range clearEnv {
					os.Unsetenv(key)
				}
			}()

			got, err := Load()
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Compare each field
			if got.LogFile != tt.want.LogFile {
				t.Errorf("LogFile = %v, want %v", got.LogFile, tt.want.LogFile)
			}
			if got.DBPath != tt.want.DBPath {
				t.Errorf("DBPath = %v, want %v", got.DBPath, tt.want.DBPath)
			}
			if got.Listen != tt.want.Listen {
				t.Errorf("Listen = %v, want %v", got.Listen, tt.want.Listen)
			}
			if got.RetentionDays != tt.want.RetentionDays {
				t.Errorf("RetentionDays = %v, want %v", got.RetentionDays, tt.want.RetentionDays)
			}
			if got.HtpasswdFile != tt.want.HtpasswdFile {
				t.Errorf("HtpasswdFile = %v, want %v", got.HtpasswdFile, tt.want.HtpasswdFile)
			}
			if got.AuthUser != tt.want.AuthUser {
				t.Errorf("AuthUser = %v, want %v", got.AuthUser, tt.want.AuthUser)
			}
			if got.AuthPass != tt.want.AuthPass {
				t.Errorf("AuthPass = %v, want %v", got.AuthPass, tt.want.AuthPass)
			}
		})
	}
}

func TestGetEnvOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		setEnv       bool
		want         string
	}{
		{
			name:         "env var not set - returns default",
			key:          "TEST_VAR",
			defaultValue: "default",
			setEnv:       false,
			want:         "default",
		},
		{
			name:         "env var set - returns env value",
			key:          "TEST_VAR",
			defaultValue: "default",
			envValue:     "custom",
			setEnv:       true,
			want:         "custom",
		},
		{
			name:         "env var set to empty string - returns default",
			key:          "TEST_VAR",
			defaultValue: "default",
			envValue:     "",
			setEnv:       true,
			want:         "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Unsetenv(tt.key)
			if tt.setEnv {
				os.Setenv(tt.key, tt.envValue)
			}
			defer os.Unsetenv(tt.key)

			got := getEnvOrDefault(tt.key, tt.defaultValue)
			if got != tt.want {
				t.Errorf("getEnvOrDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}
