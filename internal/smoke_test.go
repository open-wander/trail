package smoke_test

import (
	"bufio"
	"context"
	"os"
	"testing"
	"time"

	"github.com/open-wander/trail/internal/aggregator"
	traildb "github.com/open-wander/trail/internal/db"
	"github.com/open-wander/trail/internal/server"
	_ "modernc.org/sqlite"
)

// TestSmokeImportSampleLog imports access.log.1, flushes through aggregator,
// and verifies data appears in dashboard queries.
func TestSmokeImportSampleLog(t *testing.T) {
	logFile := "../access.log.1"
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Skip("access.log.1 not found, skipping smoke test")
	}

	// Setup in-memory DB
	database, err := traildb.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer database.Close()

	// Create aggregator and lines channel
	agg := aggregator.New(database, nil, "")
	lines := make(chan string, 10000)

	// Start aggregator in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- agg.Run(ctx, lines)
	}()

	// Read log file and send lines
	f, err := os.Open(logFile)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer f.Close()

	lineCount := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		lines <- scanner.Text()
		lineCount++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("error reading log file: %v", err)
	}

	// Close channel to trigger final flush, wait for aggregator
	close(lines)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("aggregator error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("aggregator timed out")
	}

	t.Logf("imported %d lines from access.log.1", lineCount)

	// Verify data in dashboard queries
	q := server.NewQueries(database)

	// Use a wide date range to capture all data in the log
	filter := server.Filter{
		From:        "2026-01-01T00:00:00Z",
		To:          "2026-12-31T23:00:00Z",
		IncludeBots: true,
	}

	// Requests over time should have data
	rot, err := q.RequestsOverTime(filter)
	if err != nil {
		t.Fatalf("RequestsOverTime() error = %v", err)
	}
	if len(rot) == 0 {
		t.Error("RequestsOverTime() returned 0 rows after import")
	}
	t.Logf("requests over time: %d hourly buckets", len(rot))

	// Top paths should have data
	paths, err := q.TopPaths(filter, 10)
	if err != nil {
		t.Fatalf("TopPaths() error = %v", err)
	}
	if len(paths) == 0 {
		t.Error("TopPaths() returned 0 rows after import")
	}
	t.Logf("top paths: %d paths, top = %q (%d hits)", len(paths), paths[0].Path, paths[0].Count)

	// Status breakdown should have data
	statuses, err := q.StatusBreakdown(filter)
	if err != nil {
		t.Fatalf("StatusBreakdown() error = %v", err)
	}
	if len(statuses) == 0 {
		t.Error("StatusBreakdown() returned 0 rows after import")
	}
	for _, s := range statuses {
		t.Logf("status %s: %d", s.Class, s.Count)
	}

	// Total stats should be non-zero
	stats, err := q.TotalStats(filter)
	if err != nil {
		t.Fatalf("TotalStats() error = %v", err)
	}
	if stats.Requests == 0 {
		t.Error("TotalStats() returned 0 requests after import")
	}
	t.Logf("total: %d requests, %d visitors, %d bytes", stats.Requests, stats.Visitors, stats.Bytes)

	// Unique visitors (human filter)
	humanFilter := server.Filter{
		From:        "2026-01-01T00:00:00Z",
		To:          "2026-12-31T23:00:00Z",
		IncludeBots: false,
	}
	humanStats, err := q.TotalStats(humanFilter)
	if err != nil {
		t.Fatalf("TotalStats(human) error = %v", err)
	}
	if humanStats.Visitors == 0 {
		t.Error("TotalStats(human) returned 0 visitors after import")
	}
	t.Logf("human only: %d requests, %d visitors", humanStats.Requests, humanStats.Visitors)

	// Routers should have multiple entries
	routers, err := q.Routers()
	if err != nil {
		t.Fatalf("Routers() error = %v", err)
	}
	if len(routers) < 2 {
		t.Errorf("Routers() returned %d routers, want >= 2", len(routers))
	}
	t.Logf("routers: %v", routers)

	// Referrers should have data
	refs, err := q.TopReferrers(filter, 5)
	if err != nil {
		t.Fatalf("TopReferrers() error = %v", err)
	}
	t.Logf("top referrers: %d", len(refs))
	for _, r := range refs {
		t.Logf("  %s: %d", r.Referrer, r.Count)
	}

	// Security: probed paths should exist (sample has scanner traffic)
	probed, err := q.TopProbedPaths(5)
	if err != nil {
		t.Fatalf("TopProbedPaths() error = %v", err)
	}
	if len(probed) == 0 {
		t.Error("TopProbedPaths() returned 0 rows (expected scanner traffic in sample)")
	}
	t.Logf("probed paths: %d", len(probed))
	for _, p := range probed {
		t.Logf("  %s: %d", p.Path, p.Count)
	}
}
