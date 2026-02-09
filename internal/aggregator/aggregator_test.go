package aggregator

import (
	"context"
	"database/sql"
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
	agg := New(db, nil)
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
