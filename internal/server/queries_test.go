package server

import (
	"database/sql"
	"testing"

	traildb "github.com/open-wander/trail/internal/db"
	_ "modernc.org/sqlite"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	database, err := traildb.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

type requestRow struct {
	Hour     string
	Router   string
	Path     string
	Method   string
	Status   int
	Count    int
	Bytes    int64
	Duration int64
}

type visitorRow struct {
	Hour   string
	Router string
	IPHash string
}

type referrerRow struct {
	Hour     string
	Router   string
	Referrer string
	Count    int
}

func seedRequests(t *testing.T, db *sql.DB, rows ...requestRow) {
	t.Helper()
	for _, r := range rows {
		_, err := db.Exec(
			"INSERT INTO requests (hour, router, path, method, status, count, bytes, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			r.Hour, r.Router, r.Path, r.Method, r.Status, r.Count, r.Bytes, r.Duration,
		)
		if err != nil {
			t.Fatalf("failed to seed request: %v", err)
		}
	}
}

func seedVisitors(t *testing.T, db *sql.DB, rows ...visitorRow) {
	t.Helper()
	for _, r := range rows {
		_, err := db.Exec(
			"INSERT INTO visitors (hour, router, ip_hash) VALUES (?, ?, ?)",
			r.Hour, r.Router, r.IPHash,
		)
		if err != nil {
			t.Fatalf("failed to seed visitor: %v", err)
		}
	}
}

type userAgentRow struct {
	Hour     string
	Router   string
	Category string
	Count    int
}

func seedUserAgents(t *testing.T, db *sql.DB, rows ...userAgentRow) {
	t.Helper()
	for _, r := range rows {
		_, err := db.Exec(
			"INSERT INTO user_agents (hour, router, category, count) VALUES (?, ?, ?, ?)",
			r.Hour, r.Router, r.Category, r.Count,
		)
		if err != nil {
			t.Fatalf("failed to seed user_agent: %v", err)
		}
	}
}

func seedReferrers(t *testing.T, db *sql.DB, rows ...referrerRow) {
	t.Helper()
	for _, r := range rows {
		_, err := db.Exec(
			"INSERT INTO referrers (hour, router, referrer, count) VALUES (?, ?, ?, ?)",
			r.Hour, r.Router, r.Referrer, r.Count,
		)
		if err != nil {
			t.Fatalf("failed to seed referrer: %v", err)
		}
	}
}

func TestRequestsOverTime(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 10, 5000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 200, 5, 2500, 50000},
		requestRow{"2026-02-08T01:00:00Z", "api", "/users", "GET", 200, 15, 7500, 150000},
		requestRow{"2026-02-08T01:00:00Z", "unrouted", "/scan", "GET", 404, 100, 10000, 1000},
	)

	tests := []struct {
		name   string
		filter Filter
		want   int // number of time series rows
	}{
		{
			name: "all hours including bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			want: 2, // 00:00 and 01:00
		},
		{
			name: "exclude bots filters unrouted",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			want: 2, // both hours still have api traffic
		},
		{
			name: "specific router",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				Router:      "api",
				IncludeBots: true,
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.RequestsOverTime(tt.filter)
			if err != nil {
				t.Fatalf("RequestsOverTime() error = %v", err)
			}
			if len(got) != tt.want {
				t.Errorf("RequestsOverTime() returned %d rows, want %d", len(got), tt.want)
			}
			for _, row := range got {
				if row.Label == "" {
					t.Error("returned row with empty label")
				}
				if row.Count <= 0 {
					t.Errorf("returned row with count %d, want > 0", row.Count)
				}
			}
		})
	}
}

func TestTopPaths(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 200, 50, 25000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/comments", "GET", 200, 25, 12500, 250000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/scan", "GET", 404, 1000, 100000, 10000},
	)

	tests := []struct {
		name        string
		filter      Filter
		limit       int
		wantLen     int
		wantTopPath string
	}{
		{
			name: "top 2 paths including bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			limit:       2,
			wantLen:     2,
			wantTopPath: "/scan",
		},
		{
			name: "exclude bots shows api paths",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			limit:       10,
			wantLen:     3,
			wantTopPath: "/users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.TopPaths(tt.filter, tt.limit)
			if err != nil {
				t.Fatalf("TopPaths() error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("TopPaths() returned %d rows, want %d", len(got), tt.wantLen)
			}
			if len(got) > 0 && got[0].Path != tt.wantTopPath {
				t.Errorf("TopPaths() top path = %q, want %q", got[0].Path, tt.wantTopPath)
			}
		})
	}
}

func TestTopReferrers(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedReferrers(t, db,
		referrerRow{"2026-02-08T00:00:00Z", "api", "google.com", 100},
		referrerRow{"2026-02-08T00:00:00Z", "api", "twitter.com", 50},
		referrerRow{"2026-02-08T00:00:00Z", "web", "facebook.com", 25},
		referrerRow{"2026-02-08T00:00:00Z", "unrouted", "scanner.com", 1000},
	)

	tests := []struct {
		name            string
		filter          Filter
		limit           int
		wantLen         int
		wantTopReferrer string
	}{
		{
			name: "top referrers with bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			limit:           2,
			wantLen:         2,
			wantTopReferrer: "scanner.com",
		},
		{
			name: "exclude bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			limit:           10,
			wantLen:         3,
			wantTopReferrer: "google.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.TopReferrers(tt.filter, tt.limit)
			if err != nil {
				t.Fatalf("TopReferrers() error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("TopReferrers() returned %d rows, want %d", len(got), tt.wantLen)
			}
			if len(got) > 0 && got[0].Referrer != tt.wantTopReferrer {
				t.Errorf("TopReferrers() top = %q, want %q", got[0].Referrer, tt.wantTopReferrer)
			}
		})
	}
}

func TestStatusBreakdown(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 201, 50, 25000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/redirect", "GET", 301, 25, 0, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/notfound", "GET", 404, 10, 5000, 50000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/error", "GET", 500, 5, 2500, 25000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/scan", "GET", 404, 1000, 10000, 1000},
	)

	tests := []struct {
		name   string
		filter Filter
		want   map[string]int64
	}{
		{
			name: "all with bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			want: map[string]int64{
				"2xx": 150,
				"3xx": 25,
				"4xx": 1010,
				"5xx": 5,
			},
		},
		{
			name: "exclude bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			want: map[string]int64{
				"2xx": 150,
				"3xx": 25,
				"4xx": 10,
				"5xx": 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.StatusBreakdown(tt.filter)
			if err != nil {
				t.Fatalf("StatusBreakdown() error = %v", err)
			}
			gotMap := make(map[string]int64)
			for _, row := range got {
				gotMap[row.Class] = row.Count
			}
			for class, wantCount := range tt.want {
				if gotMap[class] != wantCount {
					t.Errorf("class %q count = %d, want %d", class, gotMap[class], wantCount)
				}
			}
		})
	}
}

func TestUniqueVisitors(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedVisitors(t, db,
		visitorRow{"2026-02-08T00:00:00Z", "api", "hash1"},
		visitorRow{"2026-02-08T00:00:00Z", "api", "hash2"},
		visitorRow{"2026-02-08T01:00:00Z", "api", "hash1"},
		visitorRow{"2026-02-08T01:00:00Z", "api", "hash3"},
		visitorRow{"2026-02-08T00:00:00Z", "unrouted", "bot1"},
	)

	tests := []struct {
		name   string
		filter Filter
		want   int // number of time series rows
	}{
		{
			name: "all with bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			want: 2, // 2 hours with visitors
		},
		{
			name: "exclude bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			want: 2, // api traffic in 2 hours
		},
		{
			name: "specific router",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				Router:      "api",
				IncludeBots: true,
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.UniqueVisitors(tt.filter)
			if err != nil {
				t.Fatalf("UniqueVisitors() error = %v", err)
			}
			if len(got) != tt.want {
				t.Errorf("UniqueVisitors() returned %d rows, want %d", len(got), tt.want)
			}
			for _, row := range got {
				if row.Label == "" {
					t.Error("returned row with empty label")
				}
				if row.Count <= 0 {
					t.Errorf("returned row with count %d, want > 0", row.Count)
				}
			}
		})
	}
}

func TestTotalStats(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 10000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 200, 50, 25000, 5000000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/scan", "GET", 404, 1000, 100000, 1000000},
	)

	seedVisitors(t, db,
		visitorRow{"2026-02-08T00:00:00Z", "api", "hash1"},
		visitorRow{"2026-02-08T00:00:00Z", "api", "hash2"},
		visitorRow{"2026-02-08T00:00:00Z", "unrouted", "bot1"},
	)

	tests := []struct {
		name         string
		filter       Filter
		wantRequests int64
		wantVisitors int64
		wantBytes    int64
	}{
		{
			name: "all with bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			wantRequests: 1150,
			wantVisitors: 3,
			wantBytes:    175000,
		},
		{
			name: "exclude bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			wantRequests: 150,
			wantVisitors: 2,
			wantBytes:    75000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.TotalStats(tt.filter)
			if err != nil {
				t.Fatalf("TotalStats() error = %v", err)
			}
			if got.Requests != tt.wantRequests {
				t.Errorf("requests = %d, want %d", got.Requests, tt.wantRequests)
			}
			if got.Visitors != tt.wantVisitors {
				t.Errorf("visitors = %d, want %d", got.Visitors, tt.wantVisitors)
			}
			if got.Bytes != tt.wantBytes {
				t.Errorf("bytes = %d, want %d", got.Bytes, tt.wantBytes)
			}
			if got.Requests > 0 && got.AvgMs <= 0 {
				t.Error("avg_ms should be > 0 when requests > 0")
			}
		})
	}
}

func TestRouters(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 10, 5000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "web", "/home", "GET", 200, 5, 2500, 50000},
		requestRow{"2026-02-08T00:00:00Z", "admin", "/dashboard", "GET", 200, 3, 1500, 30000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/scan", "GET", 404, 100, 10000, 1000},
	)

	// Routers() takes no arguments - returns all distinct non-empty routers
	got, err := q.Routers()
	if err != nil {
		t.Fatalf("Routers() error = %v", err)
	}
	// Should return all 4 routers (including "unrouted" since it's a non-empty router value)
	if len(got) < 3 {
		t.Errorf("Routers() returned %d routers, want >= 3", len(got))
	}
	gotMap := make(map[string]bool)
	for _, r := range got {
		gotMap[r] = true
	}
	for _, want := range []string{"api", "web", "admin"} {
		if !gotMap[want] {
			t.Errorf("Routers() missing router %q", want)
		}
	}
}

func TestTopProbedPaths(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/wp-admin", "GET", 404, 500, 50000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/.env", "GET", 404, 300, 30000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/admin", "GET", 404, 200, 20000, 200000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 404, 10, 1000, 10000},
	)

	// TopProbedPaths only takes limit - hardcoded to unrouted router
	got, err := q.TopProbedPaths(2)
	if err != nil {
		t.Fatalf("TopProbedPaths() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("TopProbedPaths(2) returned %d rows, want 2", len(got))
	}
	if len(got) > 0 && got[0].Path != "/wp-admin" {
		t.Errorf("TopProbedPaths() top path = %q, want /wp-admin", got[0].Path)
	}

	// Verify non-unrouted paths are excluded
	allProbed, err := q.TopProbedPaths(10)
	if err != nil {
		t.Fatalf("TopProbedPaths() error = %v", err)
	}
	for _, row := range allProbed {
		if row.Path == "/users" {
			t.Error("TopProbedPaths() should not include non-unrouted paths")
		}
	}
}

func TestTopScannerIPs(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedVisitors(t, db,
		visitorRow{"2026-02-08T00:00:00Z", "unrouted", "scanner1"},
		visitorRow{"2026-02-08T01:00:00Z", "unrouted", "scanner1"},
		visitorRow{"2026-02-08T00:00:00Z", "unrouted", "scanner2"},
		visitorRow{"2026-02-08T00:00:00Z", "api", "user1"},
	)

	// TopScannerIPs only takes limit - hardcoded to unrouted router
	got, err := q.TopScannerIPs(10)
	if err != nil {
		t.Fatalf("TopScannerIPs() error = %v", err)
	}
	// Should only include unrouted visitors
	if len(got) != 2 {
		t.Errorf("TopScannerIPs() returned %d rows, want 2", len(got))
	}
	if len(got) > 0 && got[0].IPHash != "scanner1" {
		t.Errorf("TopScannerIPs() top IP = %q, want scanner1", got[0].IPHash)
	}
	// scanner1 appears in 2 hours, so count should be 2
	if len(got) > 0 && got[0].Count != 2 {
		t.Errorf("TopScannerIPs() top count = %d, want 2", got[0].Count)
	}
	// Verify api user is excluded
	for _, row := range got {
		if row.IPHash == "user1" {
			t.Error("TopScannerIPs() should not include non-unrouted visitors")
		}
	}
}

func TestBuildWhere(t *testing.T) {
	tests := []struct {
		name       string
		filter     Filter
		wantParts  int // number of conditions
		wantParams int // number of ? params
	}{
		{
			name: "basic filter",
			filter: Filter{
				From: "2026-02-08T00:00:00Z",
				To:   "2026-02-08T23:00:00Z",
			},
			wantParts:  3, // hour >= ?, hour <= ?, router != 'unrouted'
			wantParams: 2,
		},
		{
			name: "with router",
			filter: Filter{
				From:   "2026-02-08T00:00:00Z",
				To:     "2026-02-08T23:00:00Z",
				Router: "api",
			},
			wantParts:  4, // hour >= ?, hour <= ?, router = ?, router != 'unrouted'
			wantParams: 3,
		},
		{
			name: "include bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			wantParts:  2, // hour >= ?, hour <= ?
			wantParams: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			where, args := buildWhere(tt.filter)
			if where == "" {
				t.Error("buildWhere() returned empty string")
			}
			if len(args) != tt.wantParams {
				t.Errorf("buildWhere() returned %d params, want %d", len(args), tt.wantParams)
			}
		})
	}
}

func TestEmptyResults(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// All queries should return empty results without errors on empty DB
	rot, err := q.RequestsOverTime(f)
	if err != nil {
		t.Fatalf("RequestsOverTime() error = %v", err)
	}
	if len(rot) != 0 {
		t.Errorf("RequestsOverTime() on empty DB returned %d rows", len(rot))
	}

	paths, err := q.TopPaths(f, 10)
	if err != nil {
		t.Fatalf("TopPaths() error = %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("TopPaths() on empty DB returned %d rows", len(paths))
	}

	refs, err := q.TopReferrers(f, 10)
	if err != nil {
		t.Fatalf("TopReferrers() error = %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("TopReferrers() on empty DB returned %d rows", len(refs))
	}

	statuses, err := q.StatusBreakdown(f)
	if err != nil {
		t.Fatalf("StatusBreakdown() error = %v", err)
	}
	if len(statuses) != 0 {
		t.Errorf("StatusBreakdown() on empty DB returned %d rows", len(statuses))
	}

	visitors, err := q.UniqueVisitors(f)
	if err != nil {
		t.Fatalf("UniqueVisitors() error = %v", err)
	}
	if len(visitors) != 0 {
		t.Errorf("UniqueVisitors() on empty DB returned %d rows", len(visitors))
	}

	probed, err := q.TopProbedPaths(10)
	if err != nil {
		t.Fatalf("TopProbedPaths() error = %v", err)
	}
	if len(probed) != 0 {
		t.Errorf("TopProbedPaths() on empty DB returned %d rows", len(probed))
	}

	scanners, err := q.TopScannerIPs(10)
	if err != nil {
		t.Fatalf("TopScannerIPs() error = %v", err)
	}
	if len(scanners) != 0 {
		t.Errorf("TopScannerIPs() on empty DB returned %d rows", len(scanners))
	}

	notFound, err := q.TopNotFound(f, 10)
	if err != nil {
		t.Fatalf("TopNotFound() error = %v", err)
	}
	if len(notFound) != 0 {
		t.Errorf("TopNotFound() on empty DB returned %d rows", len(notFound))
	}

	agents, err := q.UserAgentBreakdown(f)
	if err != nil {
		t.Fatalf("UserAgentBreakdown() error = %v", err)
	}
	if len(agents) != 0 {
		t.Errorf("UserAgentBreakdown() on empty DB returned %d rows", len(agents))
	}

	methods, err := q.MethodBreakdown(f)
	if err != nil {
		t.Fatalf("MethodBreakdown() error = %v", err)
	}
	if len(methods) != 0 {
		t.Errorf("MethodBreakdown() on empty DB returned %d rows", len(methods))
	}

	specificStatuses, err := q.SpecificStatusCodes(f)
	if err != nil {
		t.Fatalf("SpecificStatusCodes() error = %v", err)
	}
	if len(specificStatuses) != 0 {
		t.Errorf("SpecificStatusCodes() on empty DB returned %d rows", len(specificStatuses))
	}

	hourDist, err := q.HourOfDayDistribution(f)
	if err != nil {
		t.Fatalf("HourOfDayDistribution() error = %v", err)
	}
	if len(hourDist) != 0 {
		t.Errorf("HourOfDayDistribution() on empty DB returned %d rows", len(hourDist))
	}

	drilldown, err := q.PathDrilldown(f, "/nonexistent")
	if err != nil {
		t.Fatalf("PathDrilldown() error = %v", err)
	}
	if len(drilldown) != 0 {
		t.Errorf("PathDrilldown() on empty DB returned %d rows", len(drilldown))
	}

	classDrilldown, err := q.StatusClassDrilldown(f, "4xx")
	if err != nil {
		t.Fatalf("StatusClassDrilldown() error = %v", err)
	}
	if len(classDrilldown) != 0 {
		t.Errorf("StatusClassDrilldown() on empty DB returned %d rows", len(classDrilldown))
	}
}

func TestTopNotFound(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/missing", "GET", 404, 50, 5000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/gone", "GET", 404, 30, 3000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/wp-admin", "GET", 404, 500, 50000, 500000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: false,
	}

	got, err := q.TopNotFound(f, 10)
	if err != nil {
		t.Fatalf("TopNotFound() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("TopNotFound() returned %d rows, want 2", len(got))
	}
	if len(got) > 0 && got[0].Path != "/missing" {
		t.Errorf("TopNotFound() top path = %q, want /missing", got[0].Path)
	}
	// Should not include 200 status paths
	for _, row := range got {
		if row.Path == "/users" {
			t.Error("TopNotFound() should not include non-404 paths")
		}
	}
}

func TestUserAgentBreakdown(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedUserAgents(t, db,
		userAgentRow{"2026-02-08T00:00:00Z", "api", "Chrome", 100},
		userAgentRow{"2026-02-08T00:00:00Z", "api", "Firefox", 50},
		userAgentRow{"2026-02-08T01:00:00Z", "api", "Chrome", 80},
		userAgentRow{"2026-02-08T00:00:00Z", "unrouted", "bot", 500},
	)

	tests := []struct {
		name        string
		filter      Filter
		wantLen     int
		wantTopCat  string
		wantTopPct  bool // whether top should have Pct > 0
	}{
		{
			name: "exclude bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: false,
			},
			wantLen:    2,
			wantTopCat: "Chrome",
			wantTopPct: true,
		},
		{
			name: "include bots",
			filter: Filter{
				From:        "2026-02-08T00:00:00Z",
				To:          "2026-02-08T23:00:00Z",
				IncludeBots: true,
			},
			wantLen:    3,
			wantTopCat: "bot",
			wantTopPct: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.UserAgentBreakdown(tt.filter)
			if err != nil {
				t.Fatalf("UserAgentBreakdown() error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("UserAgentBreakdown() returned %d rows, want %d", len(got), tt.wantLen)
			}
			if len(got) > 0 && got[0].Category != tt.wantTopCat {
				t.Errorf("UserAgentBreakdown() top = %q, want %q", got[0].Category, tt.wantTopCat)
			}
			if tt.wantTopPct && len(got) > 0 && got[0].Pct <= 0 {
				t.Error("UserAgentBreakdown() top Pct should be > 0")
			}
		})
	}
}

func TestMethodBreakdown(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "POST", 200, 30, 15000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users/1", "PUT", 200, 10, 5000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users/1", "DELETE", 200, 5, 2500, 50000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.MethodBreakdown(f)
	if err != nil {
		t.Fatalf("MethodBreakdown() error = %v", err)
	}
	if len(got) != 4 {
		t.Errorf("MethodBreakdown() returned %d rows, want 4", len(got))
	}
	if len(got) > 0 && got[0].Method != "GET" {
		t.Errorf("MethodBreakdown() top = %q, want GET", got[0].Method)
	}
	// Percentages should sum to ~100
	var totalPct float64
	for _, m := range got {
		totalPct += m.Pct
	}
	if totalPct < 99 || totalPct > 101 {
		t.Errorf("MethodBreakdown() pct sum = %.1f, want ~100", totalPct)
	}
}

func TestSpecificStatusCodes(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "POST", 201, 50, 25000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/redirect", "GET", 301, 25, 0, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/notfound", "GET", 404, 10, 5000, 50000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/error", "GET", 500, 5, 2500, 25000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.SpecificStatusCodes(f)
	if err != nil {
		t.Fatalf("SpecificStatusCodes() error = %v", err)
	}
	if len(got) != 5 {
		t.Errorf("SpecificStatusCodes() returned %d rows, want 5", len(got))
	}
	// Check that class is correctly assigned
	statusClassMap := map[int]string{200: "2xx", 201: "2xx", 301: "3xx", 404: "4xx", 500: "5xx"}
	for _, row := range got {
		if want, ok := statusClassMap[row.Status]; ok && row.Class != want {
			t.Errorf("status %d class = %q, want %q", row.Status, row.Class, want)
		}
	}
}

func TestHourOfDayDistribution(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 10, 5000, 100000},
		requestRow{"2026-02-08T08:00:00Z", "api", "/users", "GET", 200, 50, 25000, 500000},
		requestRow{"2026-02-08T12:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T18:00:00Z", "api", "/users", "GET", 200, 30, 15000, 300000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.HourOfDayDistribution(f)
	if err != nil {
		t.Fatalf("HourOfDayDistribution() error = %v", err)
	}
	if len(got) != 4 {
		t.Errorf("HourOfDayDistribution() returned %d rows, want 4", len(got))
	}
	// Hours should be in order
	for i := 1; i < len(got); i++ {
		if got[i].Hour <= got[i-1].Hour {
			t.Errorf("HourOfDayDistribution() hours not in order: %d <= %d", got[i].Hour, got[i-1].Hour)
		}
	}
	// Check specific hours
	hourMap := make(map[int]int64)
	for _, row := range got {
		hourMap[row.Hour] = row.Count
	}
	if hourMap[12] != 100 {
		t.Errorf("hour 12 count = %d, want 100", hourMap[12])
	}
}

func TestPathDrilldown(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 80, 40000, 800000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 304, 10, 0, 10000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "POST", 201, 20, 10000, 200000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 200, 50, 25000, 500000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.PathDrilldown(f, "/users")
	if err != nil {
		t.Fatalf("PathDrilldown() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("PathDrilldown() returned %d rows, want 3", len(got))
	}
	// Should not include /posts
	for _, row := range got {
		if row.Method == "GET" && row.Status == 200 && row.Count != 80 {
			t.Errorf("PathDrilldown() GET 200 count = %d, want 80", row.Count)
		}
	}

	// Drilling down on a non-existent path should return empty
	empty, err := q.PathDrilldown(f, "/nonexistent")
	if err != nil {
		t.Fatalf("PathDrilldown() error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("PathDrilldown() for nonexistent path returned %d rows", len(empty))
	}
}

func TestStatusClassDrilldown(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "POST", 201, 50, 25000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/gone", "GET", 404, 10, 5000, 50000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/bad", "POST", 400, 5, 2500, 25000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/error", "GET", 500, 3, 1500, 15000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// 2xx class should have 200 and 201
	got2xx, err := q.StatusClassDrilldown(f, "2xx")
	if err != nil {
		t.Fatalf("StatusClassDrilldown(2xx) error = %v", err)
	}
	if len(got2xx) != 2 {
		t.Errorf("StatusClassDrilldown(2xx) returned %d rows, want 2", len(got2xx))
	}

	// 4xx class should have 400 and 404
	got4xx, err := q.StatusClassDrilldown(f, "4xx")
	if err != nil {
		t.Fatalf("StatusClassDrilldown(4xx) error = %v", err)
	}
	if len(got4xx) != 2 {
		t.Errorf("StatusClassDrilldown(4xx) returned %d rows, want 2", len(got4xx))
	}

	// Invalid class should error
	_, err = q.StatusClassDrilldown(f, "invalid")
	if err == nil {
		t.Error("StatusClassDrilldown(invalid) should return error")
	}
}

func TestTopPathsPaginated(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/a", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/b", "GET", 200, 80, 40000, 800000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/c", "GET", 200, 60, 30000, 600000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/d", "GET", 200, 40, 20000, 400000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/e", "GET", 200, 20, 10000, 200000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// Page 1, limit 2
	result, err := q.TopPathsPaginated(f, 1, 2, "count", "desc")
	if err != nil {
		t.Fatalf("TopPathsPaginated() error = %v", err)
	}
	if result.TotalCount != 5 {
		t.Errorf("TotalCount = %d, want 5", result.TotalCount)
	}
	if result.TotalPages != 3 {
		t.Errorf("TotalPages = %d, want 3", result.TotalPages)
	}
	items := result.Items.([]PathStat)
	if len(items) != 2 {
		t.Errorf("page 1 returned %d items, want 2", len(items))
	}
	if len(items) > 0 && items[0].Path != "/a" {
		t.Errorf("page 1 first path = %q, want /a", items[0].Path)
	}

	// Page 2
	result2, err := q.TopPathsPaginated(f, 2, 2, "count", "desc")
	if err != nil {
		t.Fatalf("TopPathsPaginated() page 2 error = %v", err)
	}
	items2 := result2.Items.([]PathStat)
	if len(items2) != 2 {
		t.Errorf("page 2 returned %d items, want 2", len(items2))
	}
	if len(items2) > 0 && items2[0].Path != "/c" {
		t.Errorf("page 2 first path = %q, want /c", items2[0].Path)
	}

	// Sort by path ascending
	resultAsc, err := q.TopPathsPaginated(f, 1, 5, "path", "asc")
	if err != nil {
		t.Fatalf("TopPathsPaginated() sort asc error = %v", err)
	}
	itemsAsc := resultAsc.Items.([]PathStat)
	if len(itemsAsc) > 0 && itemsAsc[0].Path != "/a" {
		t.Errorf("sort asc first path = %q, want /a", itemsAsc[0].Path)
	}
}

func TestPathsSummary(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 200, 50, 25000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/comments", "GET", 200, 25, 12500, 250000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.PathsSummary(f)
	if err != nil {
		t.Fatalf("PathsSummary() error = %v", err)
	}
	if got.TotalHits != 175 {
		t.Errorf("TotalHits = %d, want 175", got.TotalHits)
	}
	if got.TotalBytes != 87500 {
		t.Errorf("TotalBytes = %d, want 87500", got.TotalBytes)
	}
	if got.AvgMs <= 0 {
		t.Error("AvgMs should be > 0")
	}
}

func TestThreatPatterns(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/wp-admin", "GET", 404, 500, 50000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/wp-login.php", "GET", 404, 300, 30000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/.env", "GET", 404, 200, 20000, 200000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/.git/config", "GET", 404, 100, 10000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/phpmyadmin", "GET", 404, 150, 15000, 150000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/shell.php", "GET", 404, 80, 8000, 80000},
		requestRow{"2026-02-08T00:00:00Z", "unrouted", "/random-path", "GET", 404, 50, 5000, 50000},
		// Non-unrouted should be excluded
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 1000, 100000, 1000000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.ThreatPatterns(f, false)
	if err != nil {
		t.Fatalf("ThreatPatterns() error = %v", err)
	}

	if len(got) == 0 {
		t.Fatal("ThreatPatterns() returned 0 rows")
	}

	// Verify categories exist and have counts
	catMap := make(map[string]int64)
	for _, tp := range got {
		catMap[tp.Category] = tp.Count
		if tp.Pct <= 0 {
			t.Errorf("category %q has Pct <= 0", tp.Category)
		}
		if len(tp.Examples) == 0 {
			t.Errorf("category %q has no examples", tp.Category)
		}
	}

	// WordPress category should have /wp-admin + /wp-login.php = 800
	if catMap["WordPress"] != 800 {
		t.Errorf("WordPress count = %d, want 800", catMap["WordPress"])
	}

	// Environment should have /.env + /.git/config = 300
	if catMap["Environment"] != 300 {
		t.Errorf("Environment count = %d, want 300", catMap["Environment"])
	}

	// Admin Panels should have /phpmyadmin = 150
	if catMap["Admin Panels"] != 150 {
		t.Errorf("Admin Panels count = %d, want 150", catMap["Admin Panels"])
	}

	// Scripts should have /shell.php = 80
	if catMap["Scripts"] != 80 {
		t.Errorf("Scripts count = %d, want 80", catMap["Scripts"])
	}

	// Other should have /random-path = 50
	if catMap["Other"] != 50 {
		t.Errorf("Other count = %d, want 50", catMap["Other"])
	}

	// Total should not include api traffic
	total := int64(0)
	for _, tp := range got {
		total += tp.Count
	}
	if total != 1380 {
		t.Errorf("total unrouted = %d, want 1380", total)
	}
}

func TestThreatPatternsEmpty(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.ThreatPatterns(f, false)
	if err != nil {
		t.Fatalf("ThreatPatterns() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ThreatPatterns() on empty DB returned %d rows", len(got))
	}
}

func TestBotVsHuman(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedUserAgents(t, db,
		userAgentRow{"2026-02-08T00:00:00Z", "api", "Chrome", 100},
		userAgentRow{"2026-02-08T00:00:00Z", "api", "Firefox", 50},
		userAgentRow{"2026-02-08T00:00:00Z", "api", "Safari", 30},
		userAgentRow{"2026-02-08T00:00:00Z", "api", "bot", 200},
		userAgentRow{"2026-02-08T00:00:00Z", "api", "crawler", 80},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	humanCount, botCount, botBreakdown, err := q.BotVsHuman(f)
	if err != nil {
		t.Fatalf("BotVsHuman() error = %v", err)
	}

	// Human = Chrome(100) + Firefox(50) + Safari(30) = 180
	if humanCount != 180 {
		t.Errorf("humanCount = %d, want 180", humanCount)
	}

	// Bot = bot(200) + crawler(80) = 280
	if botCount != 280 {
		t.Errorf("botCount = %d, want 280", botCount)
	}

	// Bot breakdown should have 2 entries
	if len(botBreakdown) != 2 {
		t.Errorf("botBreakdown length = %d, want 2", len(botBreakdown))
	}
}

func TestBotVsHumanEmpty(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	humanCount, botCount, botBreakdown, err := q.BotVsHuman(f)
	if err != nil {
		t.Fatalf("BotVsHuman() error = %v", err)
	}
	if humanCount != 0 || botCount != 0 {
		t.Errorf("expected 0/0, got %d/%d", humanCount, botCount)
	}
	if len(botBreakdown) != 0 {
		t.Errorf("expected empty breakdown, got %d entries", len(botBreakdown))
	}
}

func TestErrorTrends(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/error", "GET", 500, 10, 5000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/bad", "POST", 502, 5, 2500, 50000},
		requestRow{"2026-02-09T00:00:00Z", "api", "/error", "GET", 500, 20, 10000, 200000},
		// Non-5xx should be excluded
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/missing", "GET", 404, 50, 25000, 500000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-09T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.ErrorTrends(f)
	if err != nil {
		t.Fatalf("ErrorTrends() error = %v", err)
	}

	// Should have 2 days
	if len(got) != 2 {
		t.Errorf("ErrorTrends() returned %d rows, want 2", len(got))
	}

	// Day 1 should have 15 (10 + 5)
	if len(got) > 0 && got[0].Count != 15 {
		t.Errorf("day 1 count = %d, want 15", got[0].Count)
	}

	// Day 2 should have 20
	if len(got) > 1 && got[1].Count != 20 {
		t.Errorf("day 2 count = %d, want 20", got[1].Count)
	}
}

func TestErrorPaths(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/error", "GET", 500, 50, 25000, 5000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/bad-gateway", "GET", 502, 30, 15000, 3000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/timeout", "GET", 504, 10, 5000, 1000000},
		// Non-5xx should be excluded
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 1000, 500000, 10000000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.ErrorPaths(f, 10)
	if err != nil {
		t.Fatalf("ErrorPaths() error = %v", err)
	}

	if len(got) != 3 {
		t.Errorf("ErrorPaths() returned %d rows, want 3", len(got))
	}

	// Top should be /error with 50 hits
	if len(got) > 0 && got[0].Path != "/error" {
		t.Errorf("top error path = %q, want /error", got[0].Path)
	}
	if len(got) > 0 && got[0].Count != 50 {
		t.Errorf("top error count = %d, want 50", got[0].Count)
	}

	// Should not include /users (200 status)
	for _, p := range got {
		if p.Path == "/users" {
			t.Error("ErrorPaths() should not include non-5xx paths")
		}
	}
}

func TestStatusCodePaths(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 404, 50, 5000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/posts", "GET", 404, 30, 3000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/comments", "GET", 404, 10, 1000, 100000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.StatusCodePaths(f, 404, 10)
	if err != nil {
		t.Fatalf("StatusCodePaths() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("StatusCodePaths() returned %d rows, want 3", len(got))
	}
	// Top should be /users with 50 hits
	if len(got) > 0 && got[0].Path != "/users" {
		t.Errorf("StatusCodePaths() top path = %q, want /users", got[0].Path)
	}
	if len(got) > 0 && got[0].Count != 50 {
		t.Errorf("StatusCodePaths() top count = %d, want 50", got[0].Count)
	}

	// Limit should work
	limited, err := q.StatusCodePaths(f, 404, 2)
	if err != nil {
		t.Fatalf("StatusCodePaths(limit=2) error = %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("StatusCodePaths(limit=2) returned %d rows, want 2", len(limited))
	}

	// No results for status that doesn't exist
	empty, err := q.StatusCodePaths(f, 500, 10)
	if err != nil {
		t.Fatalf("StatusCodePaths(500) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("StatusCodePaths(500) returned %d rows, want 0", len(empty))
	}
}

func TestPathAlternateStatuses(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 304, 30, 0, 30000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 404, 10, 1000, 10000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "POST", 201, 20, 10000, 200000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// Exclude 200, should get 304, 201, 404
	got, err := q.PathAlternateStatuses(f, "/users", 200)
	if err != nil {
		t.Fatalf("PathAlternateStatuses() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("PathAlternateStatuses() returned %d rows, want 3", len(got))
	}
	// Should be ordered by count desc: 304(30) > 201(20) > 404(10)
	if len(got) > 0 && got[0].Status != 304 {
		t.Errorf("PathAlternateStatuses() top status = %d, want 304", got[0].Status)
	}
	// Excluded status should not appear
	for _, alt := range got {
		if alt.Status == 200 {
			t.Error("PathAlternateStatuses() should not include excluded status")
		}
	}

	// Non-existent path should return empty
	empty, err := q.PathAlternateStatuses(f, "/nonexistent", 200)
	if err != nil {
		t.Fatalf("PathAlternateStatuses(nonexistent) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("PathAlternateStatuses(nonexistent) returned %d rows, want 0", len(empty))
	}
}

func TestStatusCodeMethods(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 404, 50, 5000, 500000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "POST", 404, 30, 3000, 300000},
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "DELETE", 404, 20, 2000, 200000},
		// Different status, should be excluded
		requestRow{"2026-02-08T00:00:00Z", "api", "/users", "GET", 200, 100, 50000, 1000000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.StatusCodeMethods(f, 404)
	if err != nil {
		t.Fatalf("StatusCodeMethods() error = %v", err)
	}
	if len(got) != 3 {
		t.Errorf("StatusCodeMethods() returned %d rows, want 3", len(got))
	}
	// Top should be GET with 50 hits
	if len(got) > 0 && got[0].Method != "GET" {
		t.Errorf("StatusCodeMethods() top method = %q, want GET", got[0].Method)
	}
	// Percentages should sum to ~100
	var totalPct float64
	for _, m := range got {
		totalPct += m.Pct
	}
	if totalPct < 99 || totalPct > 101 {
		t.Errorf("StatusCodeMethods() pct sum = %.1f, want ~100", totalPct)
	}

	// No results for status that doesn't exist
	empty, err := q.StatusCodeMethods(f, 500)
	if err != nil {
		t.Fatalf("StatusCodeMethods(500) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("StatusCodeMethods(500) returned %d rows, want 0", len(empty))
	}
}

func TestSlowestPaths(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	seedRequests(t, db,
		// /slow: 10 requests, 10000000ms total = 1000000ms avg
		requestRow{"2026-02-08T00:00:00Z", "api", "/slow", "GET", 200, 10, 50000, 10000000},
		// /medium: 20 requests, 2000000ms total = 100000ms avg
		requestRow{"2026-02-08T00:00:00Z", "api", "/medium", "GET", 200, 20, 100000, 2000000},
		// /fast: 100 requests, 100000ms total = 1000ms avg
		requestRow{"2026-02-08T00:00:00Z", "api", "/fast", "GET", 200, 100, 500000, 100000},
		// /rare: only 2 requests, should be excluded by HAVING >= 5
		requestRow{"2026-02-08T00:00:00Z", "api", "/rare", "GET", 200, 2, 1000, 50000000},
	)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	got, err := q.SlowestPaths(f, 10)
	if err != nil {
		t.Fatalf("SlowestPaths() error = %v", err)
	}

	// Should have 3 paths (excluding /rare with only 2 requests)
	if len(got) != 3 {
		t.Errorf("SlowestPaths() returned %d rows, want 3", len(got))
	}

	// Top should be /slow
	if len(got) > 0 && got[0].Path != "/slow" {
		t.Errorf("slowest path = %q, want /slow", got[0].Path)
	}

	// Should be ordered by avg_ms desc
	for i := 1; i < len(got); i++ {
		if got[i].AvgMs > got[i-1].AvgMs {
			t.Errorf("SlowestPaths() not ordered by avg_ms: %d > %d", got[i].AvgMs, got[i-1].AvgMs)
		}
	}

	// /rare should be excluded
	for _, p := range got {
		if p.Path == "/rare" {
			t.Error("SlowestPaths() should not include paths with < 5 requests")
		}
	}
}

func TestComputeDonutPositions(t *testing.T) {
	segments := []DonutSegment{
		{Label: "2xx", Count: 800, Color: "green"},
		{Label: "4xx", Count: 150, Color: "yellow"},
		{Label: "5xx", Count: 50, Color: "red"},
	}

	computeDonutPositions(segments)

	if segments[0].Start != 0 {
		t.Errorf("segment 0 start = %f, want 0", segments[0].Start)
	}
	if segments[0].Pct != 80 {
		t.Errorf("segment 0 pct = %f, want 80", segments[0].Pct)
	}
	// Last segment should end at exactly 100
	if segments[2].End != 100 {
		t.Errorf("last segment end = %f, want 100", segments[2].End)
	}

	// Empty slice should not panic
	computeDonutPositions(nil)
	computeDonutPositions([]DonutSegment{})
}

func TestHourOfDayVisitors(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	f := Filter{
		From:        "2026-02-08T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// Seed visitors at different hours
	seedVisitors(t, db,
		visitorRow{Hour: "2026-02-08T09:00:00Z", Router: "web", IPHash: "aaa"},
		visitorRow{Hour: "2026-02-08T09:00:00Z", Router: "web", IPHash: "bbb"},
		visitorRow{Hour: "2026-02-08T09:00:00Z", Router: "web", IPHash: "ccc"},
		visitorRow{Hour: "2026-02-08T14:00:00Z", Router: "web", IPHash: "aaa"},
		visitorRow{Hour: "2026-02-08T14:00:00Z", Router: "web", IPHash: "ddd"},
		visitorRow{Hour: "2026-02-08T22:00:00Z", Router: "web", IPHash: "eee"},
	)

	got, err := q.HourOfDayVisitors(f)
	if err != nil {
		t.Fatalf("HourOfDayVisitors() error: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("HourOfDayVisitors() returned %d rows, want 3", len(got))
	}

	// Hour 9 should have 3 unique visitors
	if got[0].Hour != 9 || got[0].Count != 3 {
		t.Errorf("hour 9: got hour=%d count=%d, want hour=9 count=3", got[0].Hour, got[0].Count)
	}

	// Hour 14 should have 2 unique visitors (aaa is duplicate but different hour)
	if got[1].Hour != 14 || got[1].Count != 2 {
		t.Errorf("hour 14: got hour=%d count=%d, want hour=14 count=2", got[1].Hour, got[1].Count)
	}
}

func TestPathDailyTrends(t *testing.T) {
	db := testDB(t)
	q := NewQueries(db)

	f := Filter{
		From:        "2026-02-06T00:00:00Z",
		To:          "2026-02-08T23:00:00Z",
		IncludeBots: true,
	}

	// Seed requests across multiple days
	seedRequests(t, db,
		requestRow{Hour: "2026-02-06T10:00:00Z", Router: "web", Path: "/", Method: "GET", Status: 200, Count: 10, Bytes: 1000, Duration: 50},
		requestRow{Hour: "2026-02-06T14:00:00Z", Router: "web", Path: "/", Method: "GET", Status: 200, Count: 5, Bytes: 500, Duration: 25},
		requestRow{Hour: "2026-02-07T10:00:00Z", Router: "web", Path: "/", Method: "GET", Status: 200, Count: 20, Bytes: 2000, Duration: 100},
		requestRow{Hour: "2026-02-08T10:00:00Z", Router: "web", Path: "/", Method: "GET", Status: 200, Count: 8, Bytes: 800, Duration: 40},
		requestRow{Hour: "2026-02-06T10:00:00Z", Router: "web", Path: "/api", Method: "GET", Status: 200, Count: 3, Bytes: 300, Duration: 15},
		requestRow{Hour: "2026-02-08T10:00:00Z", Router: "web", Path: "/api", Method: "GET", Status: 200, Count: 7, Bytes: 700, Duration: 35},
	)

	// Empty paths should return nil
	result, err := q.PathDailyTrends(f, []string{})
	if err != nil {
		t.Fatalf("PathDailyTrends(empty) error: %v", err)
	}
	if result != nil {
		t.Errorf("PathDailyTrends(empty) = %v, want nil", result)
	}

	// Fetch trends for "/" and "/api"
	result, err = q.PathDailyTrends(f, []string{"/", "/api"})
	if err != nil {
		t.Fatalf("PathDailyTrends() error: %v", err)
	}

	// "/" should have 3 days of data
	rootTrend, ok := result["/"]
	if !ok {
		t.Fatal("PathDailyTrends() missing path /")
	}
	if len(rootTrend) != 3 {
		t.Fatalf("/ trend length = %d, want 3", len(rootTrend))
	}
	// Feb 6: 10+5=15, Feb 7: 20, Feb 8: 8
	if rootTrend[0] != 15 {
		t.Errorf("/ day 1 = %d, want 15", rootTrend[0])
	}
	if rootTrend[1] != 20 {
		t.Errorf("/ day 2 = %d, want 20", rootTrend[1])
	}
	if rootTrend[2] != 8 {
		t.Errorf("/ day 3 = %d, want 8", rootTrend[2])
	}

	// "/api" should have 2 days of data (skips Feb 7)
	apiTrend, ok := result["/api"]
	if !ok {
		t.Fatal("PathDailyTrends() missing path /api")
	}
	if len(apiTrend) != 2 {
		t.Fatalf("/api trend length = %d, want 2", len(apiTrend))
	}
}
