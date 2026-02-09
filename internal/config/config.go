package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all application configuration
type Config struct {
	LogFile       string // Path to Traefik access log file
	DBPath        string // Path to SQLite database file
	Listen        string // HTTP listen address
	RetentionDays int    // Days to retain analytics data
	LogFormat     string // Log format: "auto", "traefik", or "combined"

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
		LogFile:       getEnvOrDefault("TRAIL_LOG_FILE", "/logs/access.log"),
		DBPath:        getEnvOrDefault("TRAIL_DB_PATH", "/data/trail.db"),
		Listen:        getEnvOrDefault("TRAIL_LISTEN", ":8080"),
		LogFormat:     getEnvOrDefault("TRAIL_LOG_FORMAT", "auto"),
		HtpasswdFile:  os.Getenv("TRAIL_HTPASSWD_FILE"),
		AuthUser:      os.Getenv("TRAIL_AUTH_USER"),
		AuthPass:      os.Getenv("TRAIL_AUTH_PASS"),
		GeoIPPath:     os.Getenv("TRAIL_GEOIP_PATH"),
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

	return cfg, nil
}

// getEnvOrDefault returns the environment variable value or the default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
