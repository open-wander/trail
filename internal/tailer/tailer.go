package tailer

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// Tailer implements a poll-based log file tailer with position tracking,
// copytruncate detection, and rotation handling via inode checks.
type Tailer struct {
	path     string
	db       *sql.DB
	interval time.Duration
}

// New creates a new Tailer for the given log file path.
// Default polling interval is 1 second.
func New(path string, db *sql.DB) *Tailer {
	return &Tailer{
		path:     path,
		db:       db,
		interval: 1 * time.Second,
	}
}

// Run starts the tailer loop. It polls the log file at regular intervals,
// detects rotations and truncations, and sends complete lines to the channel.
// Blocks until ctx is cancelled or a fatal error occurs.
func (t *Tailer) Run(ctx context.Context, lines chan<- string) error {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	log.Printf("tailer: starting for %s", t.path)

	// Load saved position from database
	savedOffset, savedInode, savedSize, err := loadPosition(t.db, t.path)
	if err != nil {
		return fmt.Errorf("failed to load position: %w", err)
	}

	log.Printf("tailer: loaded position offset=%d inode=%d size=%d", savedOffset, savedInode, savedSize)

	for {
		select {
		case <-ctx.Done():
			log.Printf("tailer: stopping (context cancelled)")
			return ctx.Err()
		case <-ticker.C:
			if err := t.processTick(lines, savedOffset, savedInode, savedSize); err != nil {
				// Non-fatal errors (file not found, etc.) - just log and retry
				log.Printf("tailer: tick error: %v", err)
				continue
			}

			// Update saved position for next tick
			savedOffset, savedInode, savedSize, _ = loadPosition(t.db, t.path)
		}
	}
}

// processTick handles a single poll iteration.
func (t *Tailer) processTick(lines chan<- string, savedOffset, savedInode, savedSize int64) error {
	// Stat the file to get current inode and size
	stat, err := os.Stat(t.path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet - wait for it to appear
			return fmt.Errorf("file does not exist yet: %w", err)
		}
		return fmt.Errorf("stat failed: %w", err)
	}

	// Get inode from stat
	sys, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get file system stats")
	}
	currentInode := int64(sys.Ino)
	currentSize := stat.Size()

	// Determine starting offset based on inode and size comparison
	var startOffset int64

	switch {
	case currentInode != savedInode:
		// Rotation detected: new file with different inode
		log.Printf("tailer: rotation detected (inode %d -> %d), starting from beginning", savedInode, currentInode)
		startOffset = 0

	case currentSize < savedOffset:
		// Copytruncate detected: file was truncated in place
		log.Printf("tailer: copytruncate detected (size %d < offset %d), starting from beginning", currentSize, savedOffset)
		startOffset = 0

	default:
		// Normal case: resume from saved offset
		startOffset = savedOffset
	}

	// If no new data, skip reading
	if startOffset >= currentSize {
		return nil
	}

	// Open and read the file
	f, err := os.Open(t.path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Seek to starting offset
	if _, err := f.Seek(startOffset, 0); err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", startOffset, err)
	}

	// Read lines using buffered scanner
	scanner := bufio.NewScanner(f)
	lineCount := 0
	newOffset := startOffset

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue // Skip empty lines
		}

		// Send line to processing channel
		select {
		case lines <- line:
			lineCount++
		case <-time.After(5 * time.Second):
			// Channel is blocked - log warning but don't fail
			log.Printf("tailer: warning - channel blocked, skipping line")
		}

		// Update offset (scanner.Bytes() doesn't include newline, add 1)
		newOffset += int64(len(scanner.Bytes())) + 1
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Save new position to database
	if lineCount > 0 {
		log.Printf("tailer: processed %d lines, new offset=%d", lineCount, newOffset)
		if err := savePosition(t.db, t.path, newOffset, currentInode, currentSize); err != nil {
			return fmt.Errorf("failed to save position: %w", err)
		}
	}

	return nil
}

// loadPosition retrieves the saved file position from the database.
// Returns zeros if no position is saved yet.
func loadPosition(db *sql.DB, path string) (offset, inode, size int64, err error) {
	query := `SELECT offset, inode, size FROM log_position WHERE file = ?`
	err = db.QueryRow(query, path).Scan(&offset, &inode, &size)
	if err == sql.ErrNoRows {
		// No saved position - start from beginning
		return 0, 0, 0, nil
	}
	if err != nil {
		return 0, 0, 0, fmt.Errorf("query failed: %w", err)
	}
	return offset, inode, size, nil
}

// savePosition persists the current file position to the database using UPSERT.
func savePosition(db *sql.DB, path string, offset, inode, size int64) error {
	query := `
		INSERT INTO log_position (file, offset, inode, size)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(file) DO UPDATE SET
			offset = excluded.offset,
			inode = excluded.inode,
			size = excluded.size
	`
	_, err := db.Exec(query, path, offset, inode, size)
	if err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}
	return nil
}
