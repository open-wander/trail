package main

import (
	"bufio"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	trail "github.com/open-wander/trail"
	"github.com/open-wander/trail/internal/aggregator"
	"github.com/open-wander/trail/internal/backfill"
	"github.com/open-wander/trail/internal/config"
	"github.com/open-wander/trail/internal/db"
	"github.com/open-wander/trail/internal/parser"
	"github.com/open-wander/trail/internal/retention"
	"github.com/open-wander/trail/internal/server"
	"github.com/open-wander/trail/internal/tailer"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Open database
	database, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Create parser with configured format
	p := parser.NewParser(cfg.LogFormat)

	// Auto-detect format from first 10 lines of the log file
	if p.Format() == parser.FormatAuto {
		if lines, err := readFirstLines(cfg.LogFile, 10); err == nil && len(lines) > 0 {
			detected := p.Detect(lines)
			formatName := "traefik"
			if detected == parser.FormatCombined {
				formatName = "combined"
			}
			log.Printf("Auto-detected log format: %s", formatName)
		}
	}

	// Import rotated log files before starting live tail
	if err := backfill.Run(context.Background(), database, cfg.LogFile, p); err != nil {
		log.Printf("Backfill failed: %v", err)
	}

	// Create shared lines channel (buffered, capacity 10000)
	lines := make(chan string, 10000)

	// Create components
	tail := tailer.New(cfg.LogFile, database)
	agg := aggregator.New(database, p, cfg.GeoIPPath)
	cleaner := retention.New(database, cfg.RetentionDays)
	srv := server.New(cfg, database, trail.TemplatesFS, trail.StaticFS)

	// Create root context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup shutdown signal handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start goroutines for background services
	go func() {
		if err := tail.Run(ctx, lines); err != nil {
			if err != context.Canceled {
				log.Printf("Tailer error: %v", err)
			}
		}
	}()

	go func() {
		if err := agg.Run(ctx, lines); err != nil {
			if err != context.Canceled {
				log.Printf("Aggregator error: %v", err)
			}
		}
	}()

	go func() {
		if err := cleaner.Run(ctx); err != nil {
			if err != context.Canceled {
				log.Printf("Retention cleaner error: %v", err)
			}
		}
	}()

	// Start server in goroutine (since it blocks)
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("Trail starting - listening on %s, watching %s", cfg.Listen, cfg.LogFile)
		if err := srv.Start(); err != nil {
			serverErrors <- err
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-sigCh:
		log.Println("Shutting down...")
	case err := <-serverErrors:
		log.Fatalf("Server failed to start: %v", err)
	}

	// Cancel context to stop all goroutines
	cancel()

	// Shutdown server with timeout
	if err := srv.Shutdown(); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	// Give goroutines a moment to finish cleanup
	time.Sleep(100 * time.Millisecond)

	log.Println("Shutdown complete")
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
