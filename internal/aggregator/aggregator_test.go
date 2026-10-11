package aggregator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	traildb "github.com/open-wander/trail/internal/db"
	"github.com/open-wander/trail/internal/parser"
	_ "modernc.org/sqlite"
)

// testDB creates an in-memory SQLite database for testing
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	database, err := traildb.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

// humanEntry creates a sample human Chrome log entry
func humanEntry(ip string, timestamp time.Time, path string, referer string) *parser.LogEntry {
	return &parser.LogEntry{
		IP:         ip,
		Timestamp:  timestamp,
		Method:     "GET",
		Path:       path,
		Protocol:   "HTTP/1.1",
		Status:     200,
		Bytes:      1234,
		Referer:    referer,
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
		Router:     "web@docker",
		Backend:    "http://backend:8080",
		DurationMs: 10,
	}
}

// botEntry creates a sample bot log entry
func botEntry(ip string, timestamp time.Time, path string) *parser.LogEntry {
	return &parser.LogEntry{
		IP:         ip,
		Timestamp:  timestamp,
		Method:     "GET",
		Path:       path,
		Protocol:   "HTTP/1.1",
		Status:     200,
		Bytes:      500,
		Referer:    "",
		UserAgent:  "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		Router:     "web@docker",
		Backend:    "http://backend:8080",
		DurationMs: 5,
	}
}

// unroutedEntry creates a sample unrouted log entry (no router matched)
func unroutedEntry(ip string, timestamp time.Time, path string) *parser.LogEntry {
	return &parser.LogEntry{
		IP:         ip,
		Timestamp:  timestamp,
		Method:     "GET",
		Path:       path,
		Protocol:   "HTTP/1.1",
		Status:     404,
		Bytes:      0,
		Referer:    "",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		Router:     "", // No router
		Backend:    "",
		DurationMs: 1,
	}
}

func TestAccumulateAndFlush(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Accumulate some entries
	agg.accumulate(humanEntry("1.2.3.4", ts, "/", "https://example.com/page"))
	agg.accumulate(humanEntry("5.6.7.8", ts, "/about", ""))
	agg.accumulate(botEntry("9.10.11.12", ts, "/robots.txt"))

	// Flush to database
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify requests table
	var reqCount int
	err := db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&reqCount)
	if err != nil {
		t.Fatalf("failed to query requests: %v", err)
	}
	if reqCount != 3 {
		t.Errorf("expected 3 request rows, got %d", reqCount)
	}

	// Verify visitors table (only humans, so 2)
	var visCount int
	err = db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors: %v", err)
	}
	if visCount != 2 {
		t.Errorf("expected 2 visitor rows, got %d", visCount)
	}

	// Verify referrers table (only one entry had a referer)
	var refCount int
	err = db.QueryRow("SELECT COUNT(*) FROM referrers").Scan(&refCount)
	if err != nil {
		t.Fatalf("failed to query referrers: %v", err)
	}
	if refCount != 1 {
		t.Errorf("expected 1 referrer row, got %d", refCount)
	}

	// Verify user_agents table
	var uaCount int
	err = db.QueryRow("SELECT COUNT(*) FROM user_agents").Scan(&uaCount)
	if err != nil {
		t.Fatalf("failed to query user_agents: %v", err)
	}
	if uaCount != 2 {
		t.Errorf("expected 2 user_agent rows (Chrome and Googlebot), got %d", uaCount)
	}
}

func TestUpsertLogic(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Add same request multiple times
	for i := 0; i < 3; i++ {
		agg.accumulate(&parser.LogEntry{
			IP:         "1.2.3.4",
			Timestamp:  ts,
			Method:     "GET",
			Path:       "/",
			Status:     200,
			Bytes:      100,
			UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
			Router:     "web@docker",
			DurationMs: 10,
		})
	}

	// First flush
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("first flush failed: %v", err)
	}

	// Verify counts after first flush
	var count, bytes, duration int64
	err := db.QueryRow(`
		SELECT count, bytes, duration FROM requests
		WHERE hour = ? AND router = ? AND path = ? AND method = ? AND status = ?
	`, parser.HourBucket(ts), "web@docker", "/", "GET", 200).Scan(&count, &bytes, &duration)
	if err != nil {
		t.Fatalf("failed to query request: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count=3, got %d", count)
	}
	if bytes != 300 {
		t.Errorf("expected bytes=300, got %d", bytes)
	}
	if duration != 30 {
		t.Errorf("expected duration=30, got %d", duration)
	}

	// Add more requests with same key
	for i := 0; i < 2; i++ {
		agg.accumulate(&parser.LogEntry{
			IP:         "5.6.7.8",
			Timestamp:  ts,
			Method:     "GET",
			Path:       "/",
			Status:     200,
			Bytes:      50,
			UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
			Router:     "web@docker",
			DurationMs: 5,
		})
	}

	// Second flush
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}

	// Verify counts increased (upsert worked)
	err = db.QueryRow(`
		SELECT count, bytes, duration FROM requests
		WHERE hour = ? AND router = ? AND path = ? AND method = ? AND status = ?
	`, parser.HourBucket(ts), "web@docker", "/", "GET", 200).Scan(&count, &bytes, &duration)
	if err != nil {
		t.Fatalf("failed to query request after second flush: %v", err)
	}
	if count != 5 {
		t.Errorf("expected count=5 after upsert, got %d", count)
	}
	if bytes != 400 {
		t.Errorf("expected bytes=400 after upsert, got %d", bytes)
	}
	if duration != 40 {
		t.Errorf("expected duration=40 after upsert, got %d", duration)
	}
}

func TestVisitorDeduplication(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Same IP, same hour, same router - should only create one visitor
	for i := 0; i < 5; i++ {
		agg.accumulate(humanEntry("1.2.3.4", ts, "/page1", ""))
		agg.accumulate(humanEntry("1.2.3.4", ts, "/page2", ""))
		agg.accumulate(humanEntry("1.2.3.4", ts, "/page3", ""))
	}

	// Flush
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Should only be 1 unique visitor
	var visCount int
	err := db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors: %v", err)
	}
	if visCount != 1 {
		t.Errorf("expected 1 unique visitor, got %d", visCount)
	}

	// Different IP in same hour should create another visitor
	agg.accumulate(humanEntry("5.6.7.8", ts, "/", ""))
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors after second flush: %v", err)
	}
	if visCount != 2 {
		t.Errorf("expected 2 unique visitors, got %d", visCount)
	}

	// Same IP but different hour should create another visitor
	ts2 := time.Date(2026, 1, 7, 17, 0, 0, 0, time.UTC) // Next hour
	agg.accumulate(humanEntry("1.2.3.4", ts2, "/", ""))
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("third flush failed: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors after third flush: %v", err)
	}
	if visCount != 3 {
		t.Errorf("expected 3 unique visitors (different hour), got %d", visCount)
	}
}

func TestReferrerDomainExtraction(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	tests := []struct {
		referer        string
		expectedDomain string
	}{
		{"https://example.com/page", "example.com"},
		{"https://www.google.com/search?q=test", "www.google.com"},
		{"http://subdomain.example.org/path/to/page", "subdomain.example.org"},
		{"https://example.com:8080/page", "example.com:8080"},
	}

	for _, tt := range tests {
		agg.accumulate(humanEntry("1.2.3.4", ts, "/", tt.referer))
	}

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify each referrer domain was stored correctly
	for _, tt := range tests {
		var count int
		err := db.QueryRow(`
			SELECT count FROM referrers
			WHERE hour = ? AND router = ? AND referrer = ?
		`, parser.HourBucket(ts), "web@docker", tt.expectedDomain).Scan(&count)
		if err != nil {
			t.Errorf("expected referrer %q not found: %v", tt.expectedDomain, err)
			continue
		}
		if count != 1 {
			t.Errorf("referrer %q: expected count=1, got %d", tt.expectedDomain, count)
		}
	}
}

func TestBotTrafficExcluded(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Add bot traffic
	agg.accumulate(botEntry("1.2.3.4", ts, "/robots.txt"))
	agg.accumulate(botEntry("5.6.7.8", ts, "/sitemap.xml"))

	// Add human traffic
	agg.accumulate(humanEntry("9.10.11.12", ts, "/", ""))

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify only 1 visitor (the human)
	var visCount int
	err := db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors: %v", err)
	}
	if visCount != 1 {
		t.Errorf("expected 1 human visitor, got %d", visCount)
	}

	// But all requests should be recorded
	var reqCount int
	err = db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&reqCount)
	if err != nil {
		t.Fatalf("failed to query requests: %v", err)
	}
	if reqCount != 3 {
		t.Errorf("expected 3 requests (including bots), got %d", reqCount)
	}

	// User agents should include bot category
	var uaCategory string
	err = db.QueryRow(`
		SELECT category FROM user_agents
		WHERE router = ? AND category = ?
	`, "web@docker", "googlebot").Scan(&uaCategory)
	if err != nil {
		t.Errorf("expected googlebot user agent category: %v", err)
	}
}

func TestUnroutedTrafficExcluded(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Add unrouted traffic (no router matched)
	agg.accumulate(unroutedEntry("1.2.3.4", ts, "/random"))
	agg.accumulate(unroutedEntry("5.6.7.8", ts, "/notfound"))

	// Add routed human traffic
	agg.accumulate(humanEntry("9.10.11.12", ts, "/", ""))

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify only 1 visitor (the routed human, not unrouted)
	var visCount int
	err := db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors: %v", err)
	}
	if visCount != 1 {
		t.Errorf("expected 1 routed visitor, got %d", visCount)
	}

	// But all requests should be recorded (with "unrouted" as router)
	var reqCount int
	err = db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&reqCount)
	if err != nil {
		t.Fatalf("failed to query requests: %v", err)
	}
	if reqCount != 3 {
		t.Errorf("expected 3 requests (including unrouted), got %d", reqCount)
	}

	// Verify unrouted requests have "unrouted" as router
	var unroutedCount int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM requests WHERE router = ?
	`, "unrouted").Scan(&unroutedCount)
	if err != nil {
		t.Fatalf("failed to query unrouted requests: %v", err)
	}
	if unroutedCount != 2 {
		t.Errorf("expected 2 unrouted requests, got %d", unroutedCount)
	}
}

func TestEmptyFlush(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	// Flush without accumulating anything
	if err := agg.flush(ctx); err != nil {
		t.Fatalf("empty flush failed: %v", err)
	}

	// Verify no data was written
	var reqCount, visCount, refCount, uaCount int

	db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&reqCount)
	db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	db.QueryRow("SELECT COUNT(*) FROM referrers").Scan(&refCount)
	db.QueryRow("SELECT COUNT(*) FROM user_agents").Scan(&uaCount)

	if reqCount != 0 || visCount != 0 || refCount != 0 || uaCount != 0 {
		t.Errorf("expected all tables empty after empty flush, got req=%d vis=%d ref=%d ua=%d",
			reqCount, visCount, refCount, uaCount)
	}

	// Verify buffer is still empty after empty flush
	agg.mu.Lock()
	bufSize := agg.bufferSize
	agg.mu.Unlock()

	if bufSize != 0 {
		t.Errorf("expected bufferSize=0 after empty flush, got %d", bufSize)
	}
}

func TestMultipleRouters(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Same IP but different routers
	entry1 := humanEntry("1.2.3.4", ts, "/", "")
	entry1.Router = "web@docker"

	entry2 := humanEntry("1.2.3.4", ts, "/api", "")
	entry2.Router = "api@docker"

	agg.accumulate(entry1)
	agg.accumulate(entry2)

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Should create 2 visitors (same IP, same hour, but different routers)
	var visCount int
	err := db.QueryRow("SELECT COUNT(*) FROM visitors").Scan(&visCount)
	if err != nil {
		t.Fatalf("failed to query visitors: %v", err)
	}
	if visCount != 2 {
		t.Errorf("expected 2 visitors (different routers), got %d", visCount)
	}

	// Verify both routers are present
	var webCount, apiCount int
	db.QueryRow("SELECT COUNT(*) FROM visitors WHERE router = ?", "web@docker").Scan(&webCount)
	db.QueryRow("SELECT COUNT(*) FROM visitors WHERE router = ?", "api@docker").Scan(&apiCount)

	if webCount != 1 || apiCount != 1 {
		t.Errorf("expected 1 visitor per router, got web=%d api=%d", webCount, apiCount)
	}
}

func TestReferrerUpsert(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Add multiple entries with same referrer
	for i := 0; i < 3; i++ {
		agg.accumulate(humanEntry("1.2.3.4", ts, "/", "https://example.com/page1"))
		agg.accumulate(humanEntry("5.6.7.8", ts, "/", "https://example.com/page2"))
	}

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("first flush failed: %v", err)
	}

	// Verify count is 6 (both pages, 3 times each)
	var count int
	err := db.QueryRow(`
		SELECT count FROM referrers
		WHERE hour = ? AND router = ? AND referrer = ?
	`, parser.HourBucket(ts), "web@docker", "example.com").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query referrer: %v", err)
	}
	if count != 6 {
		t.Errorf("expected referrer count=6, got %d", count)
	}

	// Add more with same referrer
	for i := 0; i < 2; i++ {
		agg.accumulate(humanEntry("9.10.11.12", ts, "/", "https://example.com/page3"))
	}

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}

	// Verify count increased to 8
	err = db.QueryRow(`
		SELECT count FROM referrers
		WHERE hour = ? AND router = ? AND referrer = ?
	`, parser.HourBucket(ts), "web@docker", "example.com").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query referrer after upsert: %v", err)
	}
	if count != 8 {
		t.Errorf("expected referrer count=8 after upsert, got %d", count)
	}
}

func TestUserAgentUpsert(t *testing.T) {
	db := testDB(t)
	agg := New(db, nil, "")
	ctx := context.Background()

	ts := time.Date(2026, 1, 7, 16, 30, 0, 0, time.UTC)

	// Add multiple Chrome requests
	for i := 0; i < 5; i++ {
		agg.accumulate(humanEntry("1.2.3.4", ts, "/", ""))
	}

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("first flush failed: %v", err)
	}

	// Verify Chrome count is 5
	var count int
	err := db.QueryRow(`
		SELECT count FROM user_agents
		WHERE hour = ? AND router = ? AND category = ?
	`, parser.HourBucket(ts), "web@docker", "Chrome").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query user_agent: %v", err)
	}
	if count != 5 {
		t.Errorf("expected Chrome count=5, got %d", count)
	}

	// Add more Chrome requests
	for i := 0; i < 3; i++ {
		agg.accumulate(humanEntry("5.6.7.8", ts, "/", ""))
	}

	if err := agg.flush(ctx); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}

	// Verify count increased to 8
	err = db.QueryRow(`
		SELECT count FROM user_agents
		WHERE hour = ? AND router = ? AND category = ?
	`, parser.HourBucket(ts), "web@docker", "Chrome").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query user_agent after upsert: %v", err)
	}
	if count != 8 {
		t.Errorf("expected Chrome count=8 after upsert, got %d", count)
	}
}

func TestRunDrainsQueuedLinesOnCancel(t *testing.T) {
	const n = 1500 // above bufferSizeThreshold to cover the threshold path
	database := testDB(t)
	agg := New(database, nil, "")

	lines := make(chan string, n)
	for range n {
		lines <- clfCurlLine
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := agg.Run(ctx, lines); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	var total int
	if err := database.QueryRow(`SELECT COALESCE(SUM(count), 0) FROM requests`).Scan(&total); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if total != n {
		t.Errorf("expected %d requests in DB after shutdown, got %d", n, total)
	}
}

func TestNewNilParserAcceptsTraefikJSON(t *testing.T) {
	agg := New(testDB(t), nil, "")
	lines := []string{
		clfCurlLine,
		`{"ClientHost":"192.168.65.1","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":20916,"RequestMethod":"GET","RequestPath":"/.env","RequestProtocol":"HTTP/1.1","StartUTC":"2026-10-09T21:07:16.918467925Z","request_User-Agent":"curl/8.7.1"}`,
	}
	for _, line := range lines {
		if _, err := agg.parser.ParseLine(line); err != nil {
			t.Errorf("default parser rejected %q: %v", line, err)
		}
	}
}

func TestRunFlushesBufferedEntriesOnShutdown(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	// Keep the ticker from flushing before shutdown.
	a.flushInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unbuffered: each send completes only once Run has received the line, so
	// all entries are buffered before the cancellation can be observed.
	lines := make(chan string)
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx, lines) }()

	const n = 3
	for range n {
		lines <- `91.34.143.167 - - [07/Jan/2026:16:17:08 +0000] "GET / HTTP/1.1" 200 5 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" 1 "web@docker" "http://172.19.0.4:80" 1ms`
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	var total int
	if err := db.QueryRow(`SELECT COALESCE(SUM(count), 0) FROM requests`).Scan(&total); err != nil {
		t.Fatalf("query requests: %v", err)
	}
	if total != n {
		t.Errorf("requests count = %d, want %d", total, n)
	}
}

const clfCurlLine = `91.34.143.167 - - [07/Jan/2026:16:17:08 +0000] "GET / HTTP/1.1" 200 5 "-" "curl/8.7.1" 1 "web@docker" "http://172.19.0.4:80" 1ms`

// The post-cancel guards in Run's select cases cannot be reached
// deterministically through Run: with ctx.Done ready, select may pick any
// ready case. These tests call the extracted case bodies (onLine, onTick)
// directly with an already-cancelled ctx, then run Run for the shutdown flush.

// assertShutdownKeepsEntries runs Run with a cancelled ctx and an empty
// channel (ctx.Done is the only ready case) and checks all want entries land.
func assertShutdownKeepsEntries(t *testing.T, a *Aggregator, ctx context.Context, want int) {
	t.Helper()
	if err := a.Run(ctx, make(chan string)); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	var total int
	if err := a.db.QueryRow(`SELECT COALESCE(SUM(count), 0) FROM requests`).Scan(&total); err != nil {
		t.Fatalf("query requests: %v", err)
	}
	if total != want {
		t.Errorf("requests count = %d, want %d", total, want)
	}
}

func bufferedSize(a *Aggregator) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bufferSize
}

func TestOnLineThresholdAfterCancelKeepsEntries(t *testing.T) {
	a := New(testDB(t), nil, "")
	for range bufferSizeThreshold - 1 {
		a.handleLine(clfCurlLine)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// This line reaches the threshold after cancellation. A failed flush is
	// restored and not returned, so a nil error no longer proves the guard:
	// assert that no flush was attempted at all.
	logs := captureLog(t)
	if err := a.onLine(ctx, clfCurlLine); err != nil {
		t.Fatalf("onLine returned %v, want nil", err)
	}
	if got := countMsg(logRecords(t, logs), "aggregator.flush_failed"); got != 0 {
		t.Errorf("flush attempted after cancel: %s", logs.String())
	}
	if got := bufferedSize(a); got != bufferSizeThreshold {
		t.Errorf("bufferSize = %d, want %d (buffer must be kept for the shutdown flush)", got, bufferSizeThreshold)
	}
	assertShutdownKeepsEntries(t, a, ctx, bufferSizeThreshold)
}

func TestOnTickAfterCancelKeepsEntries(t *testing.T) {
	a := New(testDB(t), nil, "")
	// Entries must be buffered: flush returns early on an empty buffer, which
	// would hide a missing guard.
	const n = 3
	for range n {
		a.handleLine(clfCurlLine)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// No flush attempt is the guard's real contract (see the threshold test).
	logs := captureLog(t)
	if err := a.onTick(ctx); err != nil {
		t.Fatalf("onTick returned %v, want nil", err)
	}
	if got := countMsg(logRecords(t, logs), "aggregator.flush_failed"); got != 0 {
		t.Errorf("flush attempted after cancel: %s", logs.String())
	}
	if got := bufferedSize(a); got != n {
		t.Errorf("bufferSize = %d, want %d (buffer must be kept for the shutdown flush)", got, n)
	}
	assertShutdownKeepsEntries(t, a, ctx, n)
}

func TestRunFlushesOnChannelCloseWithCancelledContext(t *testing.T) {
	// With the context cancelled and the channel closed, both select cases are
	// ready and Go picks one at random. Repeat so the channel-closed branch is
	// exercised (a miss across all rounds has probability 2^-50).
	for round := range 50 {
		db := testDB(t)
		a := New(db, nil, "")
		a.flushInterval = time.Hour

		ctx, cancel := context.WithCancel(context.Background())
		lines := make(chan string)
		errCh := make(chan error, 1)
		go func() { errCh <- a.Run(ctx, lines) }()

		lines <- `91.34.143.167 - - [07/Jan/2026:16:17:08 +0000] "GET / HTTP/1.1" 200 5 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" 1 "web@docker" "http://172.19.0.4:80" 1ms`

		cancel()
		close(lines)

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("round %d: Run returned %v, want nil", round, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: Run did not return", round)
		}

		var total int
		if err := db.QueryRow(`SELECT COALESCE(SUM(count), 0) FROM requests`).Scan(&total); err != nil {
			t.Fatalf("round %d: query requests: %v", round, err)
		}
		if total != 1 {
			t.Fatalf("round %d: requests count = %d, want 1", round, total)
		}
	}
}

// captureLog points slog.Default at a JSON handler writing to a buffer, at
// DEBUG so routine records are captured too and an absence assertion cannot
// pass merely because the level filtered the record. Read it only once the
// code under test has stopped writing.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

// logRecords decodes the captured JSON log lines.
func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		recs = append(recs, m)
	}
	return recs
}

// countMsg counts records whose msg is key.
func countMsg(recs []map[string]any, key string) int {
	n := 0
	for _, r := range recs {
		if r["msg"] == key {
			n++
		}
	}
	return n
}

// assertDropped checks the aggregator.buffer_dropped records: none when want
// is 0, otherwise exactly one reporting want dropped entries.
func assertDropped(t *testing.T, buf *bytes.Buffer, want int) {
	t.Helper()
	recs := logRecords(t, buf)
	got := countMsg(recs, "aggregator.buffer_dropped")
	if want == 0 {
		if got != 0 {
			t.Errorf("unexpected aggregator.buffer_dropped: %s", buf.String())
		}
		return
	}
	if got != 1 {
		t.Fatalf("aggregator.buffer_dropped records = %d, want 1: %s", got, buf.String())
	}
	for _, r := range recs {
		if r["msg"] != "aggregator.buffer_dropped" {
			continue
		}
		if r["dropped"] != float64(want) {
			t.Errorf("dropped = %v, want %d", r["dropped"], want)
		}
		if r["level"] != "ERROR" {
			t.Errorf("aggregator.buffer_dropped level = %v, want ERROR", r["level"])
		}
	}
}

// chromeLine returns a CLF line from a human browser at ip requesting path.
func chromeLine(ip, path string) string {
	return fmt.Sprintf(`%s - - [07/Jan/2026:16:17:08 +0000] "GET %s HTTP/1.1" 200 5 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" 1 "web@docker" "http://172.19.0.4:80" 3ms`, ip, path)
}

// breakRequests makes every flush fail by renaming the requests table away.
// The DB is :memory: with one connection, so closing it is not an option.
// Only call it while no transaction is open.
func breakRequests(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE requests RENAME TO requests_off`); err != nil {
		t.Fatalf("break requests: %v", err)
	}
}

func fixRequests(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE requests_off RENAME TO requests`); err != nil {
		t.Fatalf("fix requests: %v", err)
	}
}

func queryInt(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

type reqTotals struct{ count, bytes, duration int }

func requestTotals(t *testing.T, db *sql.DB, path string) reqTotals {
	t.Helper()
	var r reqTotals
	err := db.QueryRow(`SELECT COALESCE(SUM(count),0), COALESCE(SUM(bytes),0), COALESCE(SUM(duration),0) FROM requests WHERE path = ?`, path).
		Scan(&r.count, &r.bytes, &r.duration)
	if err != nil {
		t.Fatalf("request totals: %v", err)
	}
	return r
}

func TestFailedFlushKeepsBatch(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	captureLog(t)

	const n = 7
	for i := range n {
		a.handleLine(chromeLine(fmt.Sprintf("10.0.0.%d", i), "/a"))
	}

	breakRequests(t, db)
	if err := a.flush(context.Background()); err == nil {
		t.Fatal("flush succeeded against a missing table")
	}
	if got := bufferedSize(a); got != n {
		t.Errorf("bufferSize after failed flush = %d, want %d", got, n)
	}
	fixRequests(t, db)
	if got := queryInt(t, db, `SELECT COUNT(*) FROM requests`); got != 0 {
		t.Errorf("rows written by failed flush = %d, want 0", got)
	}
	if got := queryInt(t, db, `SELECT COUNT(*) FROM visitors`); got != 0 {
		t.Errorf("visitor rows written by failed flush = %d, want 0", got)
	}

	if err := a.flush(context.Background()); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if got, want := requestTotals(t, db, "/a"), (reqTotals{n, n * 5, n * 3}); got != want {
		t.Errorf("request totals = %+v, want %+v", got, want)
	}
	if got := queryInt(t, db, `SELECT COUNT(*) FROM visitors`); got != n {
		t.Errorf("visitors = %d, want %d", got, n)
	}
	if got := queryInt(t, db, `SELECT SUM(count) FROM user_agents`); got != n {
		t.Errorf("user_agents total = %d, want %d", got, n)
	}
	if got := queryInt(t, db, `SELECT SUM(count) FROM duration_hist`); got != n {
		t.Errorf("duration_hist total = %d, want %d", got, n)
	}
	if got := bufferedSize(a); got != 0 {
		t.Errorf("bufferSize after good flush = %d, want 0", got)
	}
}

func TestFailedFlushMergesWithNewData(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	captureLog(t)

	for i := range 3 {
		a.handleLine(chromeLine(fmt.Sprintf("10.0.0.%d", i), "/a"))
	}
	breakRequests(t, db)
	if err := a.flush(context.Background()); err == nil {
		t.Fatal("flush succeeded against a missing table")
	}
	fixRequests(t, db)

	// Overlapping request key and visitor (10.0.0.0), plus a new key and visitor.
	a.handleLine(chromeLine("10.0.0.0", "/a"))
	a.handleLine(chromeLine("10.0.0.9", "/b"))
	if got := bufferedSize(a); got != 5 {
		t.Fatalf("bufferSize = %d, want 5", got)
	}

	if err := a.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got, want := requestTotals(t, db, "/a"), (reqTotals{4, 20, 12}); got != want {
		t.Errorf("/a totals = %+v, want %+v", got, want)
	}
	if got, want := requestTotals(t, db, "/b"), (reqTotals{1, 5, 3}); got != want {
		t.Errorf("/b totals = %+v, want %+v", got, want)
	}
	if got := queryInt(t, db, `SELECT COUNT(*) FROM visitors`); got != 4 {
		t.Errorf("visitors = %d, want 4 (shared visitor written once)", got)
	}
	if got := queryInt(t, db, `SELECT SUM(count) FROM user_agents`); got != 5 {
		t.Errorf("user_agents total = %d, want 5", got)
	}
}

func TestRunSurvivesThresholdFlushFailure(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	a.flushInterval = time.Hour
	captureLog(t)
	breakRequests(t, db)

	lines := make(chan string)
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(context.Background(), lines) }()

	for range bufferSizeThreshold {
		lines <- clfCurlLine
	}
	// The threshold flush has failed by the time Run takes this line. Under
	// the old behaviour Run has returned, so this send would never complete.
	select {
	case lines <- clfCurlLine:
	case err := <-errCh:
		t.Fatalf("Run returned %v after a failed threshold flush", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run stopped receiving after a failed threshold flush")
	}

	fixRequests(t, db)
	close(lines)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after channel close")
	}
	if got := queryInt(t, db, `SELECT COALESCE(SUM(count),0) FROM requests`); got != bufferSizeThreshold+1 {
		t.Errorf("requests total = %d, want %d", got, bufferSizeThreshold+1)
	}
}

func TestOnTickFailureKeepsRunningAndRetries(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	logs := captureLog(t)
	for range 3 {
		a.handleLine(clfCurlLine)
	}
	breakRequests(t, db)
	if err := a.onTick(context.Background()); err != nil {
		t.Fatalf("onTick returned %v, want nil", err)
	}
	var failed []map[string]any
	for _, r := range logRecords(t, logs) {
		if r["msg"] == "aggregator.flush_failed" {
			failed = append(failed, r)
		}
	}
	if len(failed) != 1 || failed[0]["trigger"] != "tick" || failed[0]["level"] != "ERROR" {
		t.Errorf("want one ERROR aggregator.flush_failed with trigger=tick, got %v", failed)
	}
	if got := bufferedSize(a); got != 3 {
		t.Errorf("bufferSize = %d, want 3", got)
	}
	fixRequests(t, db)
	if err := a.onTick(context.Background()); err != nil {
		t.Fatalf("retry onTick: %v", err)
	}
	if got := queryInt(t, db, `SELECT COALESCE(SUM(count),0) FROM requests`); got != 3 {
		t.Errorf("requests total = %d, want 3", got)
	}
}

func TestThresholdRetriesAreThrottledAfterFailure(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	logs := captureLog(t)
	breakRequests(t, db)

	const n = bufferSizeThreshold + 50
	for range n {
		if err := a.onLine(context.Background(), clfCurlLine); err != nil {
			t.Fatalf("onLine returned %v, want nil", err)
		}
	}
	if got := countMsg(logRecords(t, logs), "aggregator.flush_failed"); got != 1 {
		t.Errorf("flush attempts = %d, want 1 (only a tick may retry)", got)
	}
	if got := bufferedSize(a); got != n {
		t.Errorf("bufferSize = %d, want %d", got, n)
	}

	// A successful tick clears the throttle.
	fixRequests(t, db)
	if err := a.onTick(context.Background()); err != nil {
		t.Fatalf("onTick: %v", err)
	}
	if got := queryInt(t, db, `SELECT COALESCE(SUM(count),0) FROM requests`); got != n {
		t.Errorf("requests total = %d, want %d", got, n)
	}
	for range bufferSizeThreshold {
		a.handleLine(clfCurlLine)
	}
	if err := a.onLine(context.Background(), clfCurlLine); err != nil {
		t.Fatal(err)
	}
	if got := bufferedSize(a); got != 0 {
		t.Errorf("bufferSize = %d, want 0 (threshold flush works again after recovery)", got)
	}
}

func TestFailedFlushCap(t *testing.T) {
	tests := []struct {
		name        string
		lines       int
		wantSize    int
		wantDropped int // entries reported by aggregator.buffer_dropped; 0 means no record
	}{
		{"at cap is kept", 5, 5, 0},
		{"over cap is dropped", 6, 0, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			a := New(db, nil, "")
			a.maxBuffered = 5
			logs := captureLog(t)
			for range tt.lines {
				a.handleLine(clfCurlLine)
			}
			breakRequests(t, db)
			if err := a.flush(context.Background()); err == nil {
				t.Fatal("flush succeeded against a missing table")
			}
			if got := bufferedSize(a); got != tt.wantSize {
				t.Errorf("bufferSize = %d, want %d", got, tt.wantSize)
			}
			assertDropped(t, logs, tt.wantDropped)
		})
	}
}

func TestRestoreCapCountsLiveBuffer(t *testing.T) {
	a := New(testDB(t), nil, "")
	a.maxBuffered = 5
	logs := captureLog(t)

	// Snapshot of 3 lines, then 3 more arrive while the flush is in flight.
	for range 3 {
		a.handleLine(clfCurlLine)
	}
	a.mu.Lock()
	snap := a.takeSnapshotLocked()
	a.mu.Unlock()
	for range 3 {
		a.handleLine(clfCurlLine)
	}

	a.restore(snap)
	if got := bufferedSize(a); got != 3 {
		t.Errorf("bufferSize = %d, want 3 (3+3 exceeds the cap, snapshot dropped)", got)
	}
	assertDropped(t, logs, 3)
}

func TestRestoreMergesIntoLiveBuffer(t *testing.T) {
	db := testDB(t)
	a := New(db, nil, "")
	captureLog(t)

	for i := range 3 {
		a.handleLine(chromeLine(fmt.Sprintf("10.0.0.%d", i), "/a"))
	}
	a.mu.Lock()
	snap := a.takeSnapshotLocked()
	a.mu.Unlock()

	// Accumulated while the snapshot's write was in flight: one overlapping
	// request key and visitor, one new of each.
	a.handleLine(chromeLine("10.0.0.0", "/a"))
	a.handleLine(chromeLine("10.0.0.9", "/b"))

	a.restore(snap)
	if got := bufferedSize(a); got != 5 {
		t.Errorf("bufferSize = %d, want 5 (lines, not map entries)", got)
	}
	if err := a.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got, want := requestTotals(t, db, "/a"), (reqTotals{4, 20, 12}); got != want {
		t.Errorf("/a totals = %+v, want %+v", got, want)
	}
	if got, want := requestTotals(t, db, "/b"), (reqTotals{1, 5, 3}); got != want {
		t.Errorf("/b totals = %+v, want %+v", got, want)
	}
	if got := queryInt(t, db, `SELECT COUNT(*) FROM visitors`); got != 4 {
		t.Errorf("visitors = %d, want 4", got)
	}
	if got := queryInt(t, db, `SELECT SUM(count) FROM user_agents`); got != 5 {
		t.Errorf("user_agents total = %d, want 5", got)
	}
	if got := queryInt(t, db, `SELECT SUM(count) FROM duration_hist`); got != 5 {
		t.Errorf("duration_hist total = %d, want 5", got)
	}
}
