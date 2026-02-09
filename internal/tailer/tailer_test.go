package tailer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/open-wander/trail/internal/db"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	database, err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	return database
}

func TestLoadPosition_NoSavedPosition(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	offset, inode, size, err := loadPosition(database, "/test/path")
	if err != nil {
		t.Fatalf("expected no error for missing position, got: %v", err)
	}

	if offset != 0 || inode != 0 || size != 0 {
		t.Errorf("expected zeros for missing position, got offset=%d inode=%d size=%d", offset, inode, size)
	}
}

func TestSaveAndLoadPosition(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	testPath := "/test/log/file.log"
	testOffset := int64(1234)
	testInode := int64(5678)
	testSize := int64(9012)

	// Save position
	err := savePosition(database, testPath, testOffset, testInode, testSize)
	if err != nil {
		t.Fatalf("failed to save position: %v", err)
	}

	// Load it back
	offset, inode, size, err := loadPosition(database, testPath)
	if err != nil {
		t.Fatalf("failed to load position: %v", err)
	}

	if offset != testOffset {
		t.Errorf("expected offset=%d, got %d", testOffset, offset)
	}
	if inode != testInode {
		t.Errorf("expected inode=%d, got %d", testInode, inode)
	}
	if size != testSize {
		t.Errorf("expected size=%d, got %d", testSize, size)
	}
}

func TestSavePosition_Upsert(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	testPath := "/test/log/file.log"

	// Save initial position
	err := savePosition(database, testPath, 100, 200, 300)
	if err != nil {
		t.Fatalf("failed to save initial position: %v", err)
	}

	// Update position (UPSERT)
	err = savePosition(database, testPath, 400, 500, 600)
	if err != nil {
		t.Fatalf("failed to update position: %v", err)
	}

	// Verify updated values
	offset, inode, size, err := loadPosition(database, testPath)
	if err != nil {
		t.Fatalf("failed to load position: %v", err)
	}

	if offset != 400 || inode != 500 || size != 600 {
		t.Errorf("expected updated values (400,500,600), got (%d,%d,%d)", offset, inode, size)
	}
}

func TestTailer_ReadNewLines(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	// Create test log file
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Write initial content
	err := os.WriteFile(logPath, []byte("line 1\nline 2\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write test log: %v", err)
	}

	// Create tailer
	tailer := New(logPath, database)
	tailer.interval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	lines := make(chan string, 10)
	errChan := make(chan error, 1)

	// Run tailer in background
	go func() {
		errChan <- tailer.Run(ctx, lines)
	}()

	// Collect lines
	var collected []string
	timeout := time.After(500 * time.Millisecond)

collectLoop:
	for {
		select {
		case line := <-lines:
			collected = append(collected, line)
			if len(collected) >= 2 {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	// Verify we got the lines
	if len(collected) < 2 {
		t.Errorf("expected at least 2 lines, got %d: %v", len(collected), collected)
	}
	if len(collected) >= 1 && collected[0] != "line 1" {
		t.Errorf("expected 'line 1', got '%s'", collected[0])
	}
	if len(collected) >= 2 && collected[1] != "line 2" {
		t.Errorf("expected 'line 2', got '%s'", collected[1])
	}

	cancel()
	<-errChan
}

func TestTailer_ResumeFromOffset(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Write initial content
	initialContent := "line 1\nline 2\n"
	err := os.WriteFile(logPath, []byte(initialContent), 0644)
	if err != nil {
		t.Fatalf("failed to write test log: %v", err)
	}

	// Get file inode
	stat, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	// Manually save position after first line
	offset := int64(len("line 1\n"))
	inode := getInode(t, stat)
	size := int64(len(initialContent))

	err = savePosition(database, logPath, offset, inode, size)
	if err != nil {
		t.Fatalf("failed to save position: %v", err)
	}

	// Create tailer
	tailer := New(logPath, database)
	tailer.interval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	lines := make(chan string, 10)
	errChan := make(chan error, 1)

	go func() {
		errChan <- tailer.Run(ctx, lines)
	}()

	// Should only read "line 2" since we resumed from offset
	timeout := time.After(500 * time.Millisecond)
	var collected []string

collectLoop:
	for {
		select {
		case line := <-lines:
			collected = append(collected, line)
		case <-timeout:
			break collectLoop
		}
	}

	// Should only get line 2
	if len(collected) != 1 {
		t.Errorf("expected 1 line (resumed from offset), got %d: %v", len(collected), collected)
	}
	if len(collected) >= 1 && collected[0] != "line 2" {
		t.Errorf("expected 'line 2', got '%s'", collected[0])
	}

	cancel()
	<-errChan
}

func TestTailer_AppendNewLines(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Write initial content
	err := os.WriteFile(logPath, []byte("line 1\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write test log: %v", err)
	}

	// Create tailer
	tailer := New(logPath, database)
	tailer.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lines := make(chan string, 10)
	errChan := make(chan error, 1)

	go func() {
		errChan <- tailer.Run(ctx, lines)
	}()

	// Wait for initial line
	timeout := time.After(500 * time.Millisecond)
	<-timeout

	// Append new lines
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for append: %v", err)
	}
	_, err = f.WriteString("line 2\nline 3\n")
	f.Close()
	if err != nil {
		t.Fatalf("failed to append lines: %v", err)
	}

	// Wait for new lines to be processed
	timeout = time.After(1 * time.Second)
	var collected []string

collectLoop:
	for {
		select {
		case line := <-lines:
			collected = append(collected, line)
			if len(collected) >= 3 {
				break collectLoop
			}
		case <-timeout:
			break collectLoop
		}
	}

	// Should get all 3 lines
	if len(collected) < 3 {
		t.Errorf("expected 3 lines, got %d: %v", len(collected), collected)
	}

	cancel()
	<-errChan
}

func TestTailer_CopytruncateDetection(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	// Write initial content
	initialContent := "line 1\nline 2\nline 3\n"
	err := os.WriteFile(logPath, []byte(initialContent), 0644)
	if err != nil {
		t.Fatalf("failed to write test log: %v", err)
	}

	// Get file inode
	stat, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	// Save position at end of file
	inode := getInode(t, stat)
	offset := int64(len(initialContent))
	size := int64(len(initialContent))

	err = savePosition(database, logPath, offset, inode, size)
	if err != nil {
		t.Fatalf("failed to save position: %v", err)
	}

	// Truncate file (copytruncate simulation)
	err = os.WriteFile(logPath, []byte("new line 1\n"), 0644)
	if err != nil {
		t.Fatalf("failed to truncate file: %v", err)
	}

	// Process one tick
	tailer := New(logPath, database)
	lines := make(chan string, 10)

	err = tailer.processTick(lines, offset, inode, size)
	if err != nil {
		t.Fatalf("processTick failed: %v", err)
	}

	// Should read from beginning after detecting truncation
	timeout := time.After(100 * time.Millisecond)
	var collected []string

collectLoop:
	for {
		select {
		case line := <-lines:
			collected = append(collected, line)
		case <-timeout:
			break collectLoop
		}
	}

	if len(collected) != 1 {
		t.Errorf("expected 1 line after truncate, got %d: %v", len(collected), collected)
	}
	if len(collected) >= 1 && collected[0] != "new line 1" {
		t.Errorf("expected 'new line 1', got '%s'", collected[0])
	}
}

// Helper to get inode from stat
func getInode(t *testing.T, stat os.FileInfo) int64 {
	t.Helper()
	sys, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("failed to get syscall.Stat_t")
	}
	return int64(sys.Ino)
}
