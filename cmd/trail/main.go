package main

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	sentry "github.com/getsentry/sentry-go"

	trail "github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/aggregator"
	"github.com/open-wander/trail/internal/backfill"
	"github.com/open-wander/trail/internal/config"
	"github.com/open-wander/trail/internal/db"
	"github.com/open-wander/trail/internal/logging"
	"github.com/open-wander/trail/internal/parser"
	"github.com/open-wander/trail/internal/retention"
	"github.com/open-wander/trail/internal/server"
	"github.com/open-wander/trail/internal/tailer"
)

func main() {
	// Logging comes first, straight from the environment, so a configuration
	// error is already emitted as JSON at the requested level.
	logging.Init(os.Getenv("TRAIL_LOG_LEVEL"))

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config.load_failed", "error", err)
		os.Exit(1)
	}

	// Crash reporting to GlitchTip (Sentry protocol) when SENTRY_DSN is set.
	if dsn := os.Getenv("SENTRY_DSN"); dsn != "" {
		env := os.Getenv("SENTRY_ENVIRONMENT")
		if env == "" {
			env = "production"
		}
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:              dsn,
			Environment:      env,
			Release:          os.Getenv("SENTRY_RELEASE"),
			AttachStacktrace: true,
		}); err != nil {
			slog.Warn("sentry.init_failed", "error", err)
		} else {
			slog.Info("sentry.enabled", "environment", env)
			defer sentry.Flush(2 * time.Second)
		}
	}

	// Open database
	database, err := db.Open(cfg.DBPath)
	if err != nil {
		slog.Error("db.open_failed", "path", cfg.DBPath, "error", err)
		os.Exit(1)
	}
	defer database.Close()

	// Build the server before any import work so a broken htpasswd file
	// fails startup immediately instead of after a long backfill.
	srv, err := server.New(cfg, database, trail.TemplatesFS, trail.StaticFS)
	if err != nil {
		slog.Error("server.config_failed", "error", err)
		os.Exit(1)
	}

	// Create parser with configured format
	p := parser.NewParser(cfg.LogFormat)

	// Auto-detect format from first 10 lines of the log file
	if p.Format() == parser.FormatAuto {
		if lines, err := readFirstLines(cfg.LogFile, 10); err == nil && len(lines) > 0 {
			detected := p.Detect(lines)
			// Anything other than combined is Traefik, which accepts both CLF and JSON lines.
			slog.Info("format.detected", "format", detected.String(), "mixed", detected != parser.FormatCombined)
		}
	}

	// Hand the effective format to the server before anything starts serving.
	srv.SetLogFormat(p.Format())

	// Import rotated log files before starting live tail
	if err := backfill.Run(context.Background(), database, cfg.LogFile, p); err != nil {
		slog.Error("backfill.failed", "error", err)
	}

	// Create shared lines channel (buffered, capacity 10000)
	lines := make(chan string, 10000)

	// Create components
	tail := tailer.New(cfg.LogFile, database)
	agg := aggregator.New(database, p, cfg.GeoIPPath)
	cleaner := retention.New(database, cfg.RetentionDays)

	// Create root context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup shutdown signal handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start goroutines for background services
	// The tailer gets its own context so it can be stopped before the
	// aggregator: once no sender exists, the aggregator's drain is complete.
	tailCtx, tailCancel := context.WithCancel(ctx)
	defer tailCancel()
	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		if err := tail.Run(tailCtx, lines); err != nil {
			if err != context.Canceled {
				slog.Error("tailer.failed", "error", err)
			}
		}
	}()

	aggDone := make(chan struct{})
	go func() {
		defer close(aggDone)
		if err := agg.Run(ctx, lines); err != nil {
			if err != context.Canceled {
				slog.Error("aggregator.failed", "error", err)
			}
		}
	}()

	go func() {
		if err := cleaner.Run(ctx); err != nil {
			if err != context.Canceled {
				slog.Error("retention.failed", "error", err)
			}
		}
	}()

	// Start server in goroutine (since it blocks)
	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("trail.starting", startupAttrs(cfg)...)
		if err := srv.Start(); err != nil {
			serverErrors <- err
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-sigCh:
		slog.Info("trail.shutting_down")
	case err := <-serverErrors:
		slog.Error("server.start_failed", "listen", cfg.Listen, "error", err)
		os.Exit(1)
	}

	// Stop the tailer first so its final position save happens before the
	// aggregator drains the channel and before the database is closed.
	tailCancel()
	select {
	case <-tailDone:
	case <-time.After(3 * time.Second):
		slog.Error("tailer.stop_timeout")
	}

	// Stop the aggregator and retention cleaner
	cancel()

	// Shutdown server with timeout
	if err := srv.Shutdown(); err != nil {
		slog.Error("server.shutdown_failed", "error", err)
	}

	// Wait for the aggregator final flush before the database is closed
	select {
	case <-aggDone:
	case <-time.After(10 * time.Second):
		slog.Error("aggregator.flush_timeout")
	}

	// Give goroutines a moment to finish cleanup
	time.Sleep(100 * time.Millisecond)

	slog.Info("trail.shutdown_complete")
}

// startupAttrs lists the fields of the trail.starting line. It is the one
// place that decides what the startup summary exposes; credentials never
// belong here.
func startupAttrs(cfg *config.Config) []any {
	return []any{
		"listen", cfg.Listen, "log_file", cfg.LogFile, "db_path", cfg.DBPath,
		"log_format", cfg.LogFormat, "log_level", logging.ParseLevel(cfg.LogLevel).String(),
		"retention_days", cfg.RetentionDays, "geoip", cfg.GeoIPPath != "",
		"auth", authMode(cfg),
	}
}

// authMode names the configured authentication method for the startup
// summary. It never exposes credentials.
func authMode(cfg *config.Config) string {
	switch {
	case cfg.HtpasswdFile != "":
		return "htpasswd"
	case cfg.AuthUser != "" && cfg.AuthPass != "":
		return "basic"
	default:
		return "none"
	}
}

// readFirstLines reads up to n non-empty lines from a file.
func readFirstLines(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() && len(lines) < n {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}
