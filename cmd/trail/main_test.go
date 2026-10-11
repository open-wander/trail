package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/open-wander/trail/internal/config"
)

func attrKeys(attrs []any) []string {
	var keys []string
	for i := 0; i < len(attrs); i += 2 {
		keys = append(keys, attrs[i].(string))
	}
	return keys
}

func attrMap(attrs []any) map[string]any {
	m := map[string]any{}
	for i := 0; i+1 < len(attrs); i += 2 {
		m[attrs[i].(string)] = attrs[i+1]
	}
	return m
}

func TestAuthMode(t *testing.T) {
	ht := filepath.Join(t.TempDir(), "htpasswd")
	if err := os.WriteFile(ht, []byte("alice:$2y$05$Zq9xHashNotLogged\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{"htpasswd", config.Config{HtpasswdFile: ht}, "htpasswd"},
		{"htpasswd wins over basic", config.Config{HtpasswdFile: ht, AuthUser: "u", AuthPass: "p"}, "htpasswd"},
		{"basic", config.Config{AuthUser: "u", AuthPass: "p"}, "basic"},
		{"none", config.Config{}, "none"},
		{"password without user", config.Config{AuthPass: "p"}, "none"},
		{"user without password", config.Config{AuthUser: "u"}, "none"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := authMode(&tt.cfg); got != tt.want {
				t.Errorf("authMode = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStartupAttrs(t *testing.T) {
	wantKeys := []string{"listen", "log_file", "db_path", "log_format", "log_level",
		"retention_days", "geoip", "auth"}

	t.Run("keys and values", func(t *testing.T) {
		tests := []struct {
			level, wantLevel string
			geoip            string
			wantGeoip        bool
		}{
			{"DEBUG", "DEBUG", "", false},
			{"garbage", "INFO", "/data/geo.mmdb", true},
		}
		for _, tt := range tests {
			cfg := &config.Config{Listen: ":8080", LogFile: "/l", DBPath: "/d", LogFormat: "auto",
				LogLevel: tt.level, RetentionDays: 30, GeoIPPath: tt.geoip}
			attrs := startupAttrs(cfg)
			if got := attrKeys(attrs); !slices.Equal(got, wantKeys) {
				t.Fatalf("keys = %v, want %v", got, wantKeys)
			}
			m := attrMap(attrs)
			if m["log_level"] != tt.wantLevel {
				t.Errorf("log_level = %v, want %v", m["log_level"], tt.wantLevel)
			}
			if g, ok := m["geoip"].(bool); !ok || g != tt.wantGeoip {
				t.Errorf("geoip = %v, want bool %v", m["geoip"], tt.wantGeoip)
			}
		}
	})

	t.Run("no secrets in output", func(t *testing.T) {
		const pass, user, hash = "Zq9xPassNotLogged", "Zq9xUserNotLogged", "Zq9xHashNotLogged"
		ht := filepath.Join(t.TempDir(), "htpasswd")
		if err := os.WriteFile(ht, []byte("alice:"+hash+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cfg := &config.Config{Listen: ":8080", LogFile: "/l", DBPath: "/d", LogFormat: "auto",
			LogLevel: "info", RetentionDays: 30, HtpasswdFile: ht, AuthUser: user, AuthPass: pass}
		attrs := startupAttrs(cfg)
		if got := attrKeys(attrs); !slices.Equal(got, wantKeys) {
			t.Fatalf("keys = %v, want %v", got, wantKeys)
		}
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).Info("trail.starting", attrs...)
		for _, secret := range []string{pass, user, hash} {
			if strings.Contains(buf.String(), secret) {
				t.Errorf("startup line leaks %q: %s", secret, buf.String())
			}
		}
	})
}

// Every value must come from its own config field, not just the right key.
func TestStartupAttrsValues(t *testing.T) {
	cfg := &config.Config{
		Listen:        ":9091",
		LogFile:       "/logs/a.log",
		DBPath:        "/data/t.db",
		LogFormat:     "traefik-json",
		LogLevel:      "warn",
		RetentionDays: 31,
		GeoIPPath:     "/geo.mmdb",
		AuthUser:      "u",
		AuthPass:      "p",
	}
	want := []any{
		"listen", ":9091", "log_file", "/logs/a.log", "db_path", "/data/t.db",
		"log_format", "traefik-json", "log_level", "WARN",
		"retention_days", 31, "geoip", true,
		"auth", "basic",
	}
	if got := startupAttrs(cfg); !slices.Equal(got, want) {
		t.Errorf("startupAttrs = %v, want %v", got, want)
	}
}
