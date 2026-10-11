package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all application configuration
type Config struct {
	LogFile       string // Path to Traefik access log file
	DBPath        string // Path to SQLite database file
	Listen        string // HTTP listen address
	RetentionDays int    // Days to retain analytics data
	LogFormat     string // Log format: "auto", "traefik", "traefik-json", or "combined"
	LogLevel      string // Level of Trail's own logs: "debug", "info", "warn" or "error"

	// Authentication settings (all optional)
	HtpasswdFile string // Path to htpasswd file for authentication
	AuthUser     string // Basic auth username (plaintext)
	AuthPass     string // Basic auth password (plaintext)

	// GeoIP settings (optional)
	GeoIPPath string // Path to MaxMind/DB-IP mmdb file for country lookup
}

// Load reads configuration from environment variables and applies defaults
func Load() (*Config, error) {
	cfg := &Config{
		LogFile:      getEnvOrDefault("TRAIL_LOG_FILE", "/logs/access.log"),
		DBPath:       getEnvOrDefault("TRAIL_DB_PATH", "/data/trail.db"),
		Listen:       getEnvOrDefault("TRAIL_LISTEN", ":8080"),
		LogFormat:    getEnvOrDefault("TRAIL_LOG_FORMAT", "auto"),
		LogLevel:     getEnvOrDefault("TRAIL_LOG_LEVEL", "info"),
		HtpasswdFile: os.Getenv("TRAIL_HTPASSWD_FILE"),
		AuthUser:     os.Getenv("TRAIL_AUTH_USER"),
		AuthPass:     os.Getenv("TRAIL_AUTH_PASS"),
		GeoIPPath:    os.Getenv("TRAIL_GEOIP_PATH"),
	}

	// Parse retention days with default
	retentionStr := getEnvOrDefault("TRAIL_RETENTION_DAYS", "90")
	retentionDays, err := strconv.Atoi(retentionStr)
	if err != nil {
		return nil, fmt.Errorf("invalid TRAIL_RETENTION_DAYS: %w", err)
	}
	if retentionDays <= 0 {
		return nil, fmt.Errorf("TRAIL_RETENTION_DAYS must be positive, got %d", retentionDays)
	}
	cfg.RetentionDays = retentionDays

	if err := cfg.ValidateEnvAuth(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ValidateEnvAuth rejects a half-set or blank TRAIL_AUTH_USER/TRAIL_AUTH_PASS
// pair, which would otherwise leave the dashboard open. "Set" means non-empty;
// values are never trimmed for use, so padded passwords still work. The check
// applies even when TRAIL_HTPASSWD_FILE is set: a half-set pair next to a valid
// htpasswd file signals a broken deployment. Errors never contain the values.
func (c *Config) ValidateEnvAuth() error {
	for _, v := range []struct{ name, val string }{
		{"TRAIL_AUTH_USER", c.AuthUser},
		{"TRAIL_AUTH_PASS", c.AuthPass},
	} {
		if v.val != "" && strings.TrimSpace(v.val) == "" {
			return fmt.Errorf("%s is set but blank", v.name)
		}
	}
	switch {
	case c.AuthUser != "" && c.AuthPass == "":
		return errors.New("TRAIL_AUTH_USER and TRAIL_AUTH_PASS must be set together (TRAIL_AUTH_PASS is empty)")
	case c.AuthUser == "" && c.AuthPass != "":
		return errors.New("TRAIL_AUTH_USER and TRAIL_AUTH_PASS must be set together (TRAIL_AUTH_USER is empty)")
	}
	return nil
}

// getEnvOrDefault returns the environment variable value or the default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
