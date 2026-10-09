package backfill

import (
	"compress/gzip"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	traildb "github.com/open-wander/trail/internal/db"
	"github.com/open-wander/trail/internal/parser"
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

// Sample Traefik log line for testing
const sampleLogLine = `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36" 1 "web@docker" "http://172.19.0.4:80" 1ms`

const sampleLogLine2 = `10.0.0.1 - - [07/Jan/2026:17:00:00 +0000] "GET /about HTTP/1.1" 200 1234 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36" 2 "web@docker" "http://172.19.0.4:80" 5ms`

func TestFindRotatedFiles(t *testing.T) {
	dir := t.TempDir()

	// Create rotated files
	for _, name := range []string{
		"access.log",
		"access.log.1",
		"access.log.2",
		"access.log.3.gz",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
	}

	// Create non-matching files (should be ignored)
	for _, name := range []string{
		"access.log.bak",
		"access.log.old",
		"other.log.1",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
	}

	files, err := findRotatedFiles(dir, "access.log")
	if err != nil {
		t.Fatalf("findRotatedFiles failed: %v", err)
	}

	if len(files) != 3 {
		t.Fatalf("expected 3 rotated files, got %d", len(files))
	}

	// Should be sorted descending by N (oldest first: 3, 2, 1)
	expectedNums := []int{3, 2, 1}
	for i, f := range files {
		if f.num != expectedNums[i] {
			t.Errorf("file[%d]: expected num=%d, got %d", i, expectedNums[i], f.num)
		}
	}
}

func TestFindRotatedFiles_Empty(t *testing.T) {
	dir := t.TempDir()

	// Only the active log file, no rotated copies
	if err := os.WriteFile(filepath.Join(dir, "access.log"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	files, err := findRotatedFiles(dir, "access.log")
	if err != nil {
		t.Fatalf("findRotatedFiles failed: %v", err)
	}

	if len(files) != 0 {
		t.Errorf("expected 0 rotated files, got %d", len(files))
	}
}

func TestProcessFile_PlainText(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log.1")

	content := sampleLogLine + "\n" + sampleLogLine2 + "\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan string, 100)
	f := rotatedFile{path: path, num: 1}

	if err := processFile(context.Background(), f, lines); err != nil {
		t.Fatalf("processFile failed: %v", err)
	}
	close(lines)

	var received []string
	for line := range lines {
		received = append(received, line)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(received))
	}
	if received[0] != sampleLogLine {
		t.Errorf("line 0 mismatch:\n got: %s\nwant: %s", received[0], sampleLogLine)
	}
	if received[1] != sampleLogLine2 {
		t.Errorf("line 1 mismatch:\n got: %s\nwant: %s", received[1], sampleLogLine2)
	}
}

func TestProcessFile_Gzip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log.2.gz")

	// Write gzipped content
	gzFile, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(gzFile)
	content := sampleLogLine + "\n" + sampleLogLine2 + "\n"
	if _, err := gw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzFile.Close(); err != nil {
		t.Fatal(err)
	}

	lines := make(chan string, 100)
	f := rotatedFile{path: path, num: 2}

	if err := processFile(context.Background(), f, lines); err != nil {
		t.Fatalf("processFile failed: %v", err)
	}
	close(lines)

	var received []string
	for line := range lines {
		received = append(received, line)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(received))
	}
	if received[0] != sampleLogLine {
		t.Errorf("line 0 mismatch")
	}
}

func TestIsImported(t *testing.T) {
	db := testDB(t)

	// Not imported yet
	imported, err := isImported(db, "/logs/access.log.1")
	if err != nil {
		t.Fatalf("isImported failed: %v", err)
	}
	if imported {
		t.Error("expected not imported for unknown file")
	}

	// Partially imported (offset != size)
	_, err = db.Exec(
		"INSERT INTO log_position (file, offset, inode, size) VALUES (?, ?, 0, ?)",
		"/logs/access.log.1", 50, 100,
	)
	if err != nil {
		t.Fatal(err)
	}

	imported, err = isImported(db, "/logs/access.log.1")
	if err != nil {
		t.Fatal(err)
	}
	if imported {
		t.Error("expected not imported when offset != size")
	}
}

func TestMarkImported(t *testing.T) {
	db := testDB(t)

	if err := markImported(db, "/logs/access.log.1", 12345); err != nil {
		t.Fatalf("markImported failed: %v", err)
	}

	// Verify it's now considered imported
	imported, err := isImported(db, "/logs/access.log.1")
	if err != nil {
		t.Fatal(err)
	}
	if !imported {
		t.Error("expected file to be imported after markImported")
	}

	// Verify the actual values
	var offset, size int64
	err = db.QueryRow(
		"SELECT offset, size FROM log_position WHERE file = ?",
		"/logs/access.log.1",
	).Scan(&offset, &size)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 12345 || size != 12345 {
		t.Errorf("expected offset=size=12345, got offset=%d size=%d", offset, size)
	}
}

func TestRun_FullIntegration(t *testing.T) {
	dir := t.TempDir()
	db := testDB(t)

	logPath := filepath.Join(dir, "access.log")
	// Create active log (empty, just needs to exist for path derivation)
	if err := os.WriteFile(logPath, nil, 0644); err != nil {
		t.Fatal(err)
	}

	// Create rotated file with real Traefik log lines
	rotatedPath := filepath.Join(dir, "access.log.1")
	lines := sampleLogLine + "\n" + sampleLogLine2 + "\n"
	if err := os.WriteFile(rotatedPath, []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}

	// Create gzipped rotated file
	gzPath := filepath.Join(dir, "access.log.2.gz")
	gzFile, err := os.Create(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(gzFile)
	if _, err := gw.Write([]byte(sampleLogLine + "\n")); err != nil {
		t.Fatal(err)
	}
	gw.Close()
	gzFile.Close()

	// Run backfill
	if err := Run(context.Background(), db, logPath, nil); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify data was written to DB
	var reqCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&reqCount); err != nil {
		t.Fatal(err)
	}
	if reqCount == 0 {
		t.Error("expected requests in DB after backfill, got 0")
	}

	// Verify both files marked as imported
	for _, path := range []string{rotatedPath, gzPath} {
		imported, err := isImported(db, path)
		if err != nil {
			t.Fatal(err)
		}
		if !imported {
			t.Errorf("expected %s to be marked as imported", path)
		}
	}

	// Run again - should be a no-op
	var reqCountBefore int
	db.QueryRow("SELECT COALESCE(SUM(count), 0) FROM requests").Scan(&reqCountBefore)

	if err := Run(context.Background(), db, logPath, nil); err != nil {
		t.Fatalf("second Run failed: %v", err)
	}

	var reqCountAfter int
	db.QueryRow("SELECT COALESCE(SUM(count), 0) FROM requests").Scan(&reqCountAfter)

	if reqCountAfter != reqCountBefore {
		t.Errorf("second run should be no-op: count before=%d after=%d", reqCountBefore, reqCountAfter)
	}
}

func TestRun_NoRotatedFiles(t *testing.T) {
	dir := t.TempDir()
	db := testDB(t)

	logPath := filepath.Join(dir, "access.log")
	if err := os.WriteFile(logPath, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should return nil immediately when no rotated files exist
	if err := Run(context.Background(), db, logPath, nil); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestProcessFile_ContextCancelled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log.1")

	if err := os.WriteFile(path, []byte(sampleLogLine+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Unbuffered channel so send will block and hit ctx.Done case
	lines := make(chan string)
	f := rotatedFile{path: path, num: 1}

	err := processFile(ctx, f, lines)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

// TestRun_MixedCLFAndJSON covers the switchover from Traefik CLF to JSON:
// a rotated file holding CLF lines followed by JSON lines for the same
// requests must aggregate both halves identically.
func TestRun_MixedCLFAndJSON(t *testing.T) {
	const (
		clfRouted    = `192.168.65.1 - - [09/Oct/2026:21:07:16 +0000] "GET /some/path HTTP/1.1" 404 19 "https://ref.example/" "Mozilla/5.0 test" 1 "echo@file" "http://127.0.0.1:8080" 1ms`
		clfUnrouted  = `192.168.65.1 - - [09/Oct/2026:21:07:16 +0000] "GET /.env HTTP/1.1" 404 19 "-" "curl/8.7.1" 2 "-" "-" 0ms`
		jsonRouted   = `{"ClientHost":"192.168.65.1","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":1649750,"RequestMethod":"GET","RequestPath":"/some/path","RequestProtocol":"HTTP/1.1","RouterName":"echo@file","ServiceURL":"http://127.0.0.1:8080","StartUTC":"2026-10-09T21:07:16.897593508Z","request_Referer":"https://ref.example/","request_User-Agent":"Mozilla/5.0 test"}`
		jsonUnrouted = `{"ClientHost":"192.168.65.1","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":20916,"RequestMethod":"GET","RequestPath":"/.env","RequestProtocol":"HTTP/1.1","StartUTC":"2026-10-09T21:07:16.918467925Z","request_User-Agent":"curl/8.7.1"}`
	)

	for _, format := range []string{"", "auto", "traefik", "traefik-json"} {
		t.Run("format="+format, func(t *testing.T) {
			dir := t.TempDir()
			db := testDB(t)
			logPath := filepath.Join(dir, "access.log")
			if err := os.WriteFile(logPath, nil, 0644); err != nil {
				t.Fatal(err)
			}
			content := clfRouted + "\n" + clfUnrouted + "\n" + jsonRouted + "\n" + jsonUnrouted + "\n"
			if err := os.WriteFile(filepath.Join(dir, "access.log.1"), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}

			var p *parser.Parser
			if format != "" {
				p = parser.NewParser(format)
				p.Detect([]string{clfRouted, clfUnrouted})
			}
			if err := Run(context.Background(), db, logPath, p); err != nil {
				t.Fatalf("Run failed: %v", err)
			}

			rows, err := db.Query(`SELECT router, path, status, count, bytes, duration FROM requests ORDER BY router`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			type row struct {
				router, path            string
				status, count, bytes, d int64
			}
			var got []row
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.router, &r.path, &r.status, &r.count, &r.bytes, &r.d); err != nil {
					t.Fatal(err)
				}
				got = append(got, r)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			// Each request appears twice (CLF + JSON) and must merge into one
			// row with count 2; a JSON line that diverged would add a row.
			want := []row{
				{router: "echo@file", path: "/some/path", status: 404, count: 2, bytes: 38, d: 2},
				{router: "unrouted", path: "/.env", status: 404, count: 2, bytes: 38, d: 0},
			}
			if len(got) != len(want) {
				t.Fatalf("requests rows = %+v, want %+v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}
