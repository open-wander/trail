package backfill

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/open-wander/trail/internal/aggregator"
	"github.com/open-wander/trail/internal/parser"
)

// Run imports rotated log files (access.log.1, access.log.2.gz, etc.)
// that haven't been imported yet. It processes them oldest-first using
// a dedicated aggregator instance, then marks each as imported.
// If p is nil, defaults to a Traefik parser.
func Run(ctx context.Context, db *sql.DB, logPath string, p *parser.Parser) error {
	dir := filepath.Dir(logPath)
	baseName := filepath.Base(logPath)

	files, err := findRotatedFiles(dir, baseName)
	if err != nil {
		return fmt.Errorf("finding rotated files: %w", err)
	}

	// Filter out already-imported files
	var pending []rotatedFile
	for _, f := range files {
		imported, err := isImported(db, f.path)
		if err != nil {
			return fmt.Errorf("checking import status for %s: %w", f.path, err)
		}
		if !imported {
			pending = append(pending, f)
		}
	}

	if len(pending) == 0 {
		return nil
	}

	log.Printf("backfill: %d rotated file(s) to import", len(pending))

	// Create dedicated aggregator + channel for backfill
	lines := make(chan string, 10000)
	agg := aggregator.New(db, p, "")

	// Run aggregator in background
	aggDone := make(chan error, 1)
	go func() {
		aggDone <- agg.Run(ctx, lines)
	}()

	// Process each pending file
	for _, f := range pending {
		if err := ctx.Err(); err != nil {
			close(lines)
			<-aggDone
			return err
		}

		log.Printf("backfill: importing %s", f.path)
		if err := processFile(ctx, f, lines); err != nil {
			close(lines)
			<-aggDone
			return fmt.Errorf("processing %s: %w", f.path, err)
		}

		// Get file size for marking as imported
		info, err := os.Stat(f.path)
		if err != nil {
			close(lines)
			<-aggDone
			return fmt.Errorf("stat %s: %w", f.path, err)
		}

		if err := markImported(db, f.path, info.Size()); err != nil {
			close(lines)
			<-aggDone
			return fmt.Errorf("marking %s as imported: %w", f.path, err)
		}
	}

	// Close channel to signal aggregator to flush and exit
	close(lines)

	// Wait for aggregator to finish flushing
	if err := <-aggDone; err != nil {
		return fmt.Errorf("aggregator flush: %w", err)
	}

	log.Printf("backfill: complete")
	return nil
}

// rotatedFile represents a rotated log file with its numeric suffix for sorting.
type rotatedFile struct {
	path string
	num  int
}

// findRotatedFiles scans dir for files matching {baseName}.{N} and
// {baseName}.{N}.gz. Returns them sorted by N descending (oldest first,
// since higher N = older in logrotate convention).
func findRotatedFiles(dir, baseName string) ([]rotatedFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	prefix := baseName + "."
	var files []rotatedFile

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		// Extract the part after baseName.
		suffix := strings.TrimPrefix(name, prefix)

		// Strip .gz if present
		suffix = strings.TrimSuffix(suffix, ".gz")

		// Must be a pure integer
		n, err := strconv.Atoi(suffix)
		if err != nil {
			continue
		}

		files = append(files, rotatedFile{
			path: filepath.Join(dir, name),
			num:  n,
		})
	}

	// Sort descending by N (highest number = oldest file, process first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].num > files[j].num
	})

	return files, nil
}

// isImported checks if a rotated file has already been fully imported.
// A file is considered imported if a log_position row exists with offset == size > 0.
func isImported(db *sql.DB, path string) (bool, error) {
	var offset, size int64
	err := db.QueryRow(
		"SELECT offset, size FROM log_position WHERE file = ?", path,
	).Scan(&offset, &size)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return offset == size && size > 0, nil
}

// markImported records that a rotated file has been fully imported.
func markImported(db *sql.DB, path string, size int64) error {
	_, err := db.Exec(`
		INSERT INTO log_position (file, offset, inode, size)
		VALUES (?, ?, 0, ?)
		ON CONFLICT(file) DO UPDATE SET
			offset = excluded.offset,
			inode = excluded.inode,
			size = excluded.size
	`, path, size, size)
	return err
}

// processFile reads all lines from a rotated file and sends them to the channel.
// Handles both plain text and gzip-compressed files.
func processFile(ctx context.Context, f rotatedFile, lines chan<- string) error {
	file, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer file.Close()

	var scanner *bufio.Scanner

	if strings.HasSuffix(f.path, ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("opening gzip reader: %w", err)
		}
		defer gz.Close()
		scanner = bufio.NewScanner(gz)
	} else {
		scanner = bufio.NewScanner(file)
	}

	// Set 1MB buffer for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		select {
		case lines <- line:
			count++
		case <-ctx.Done():
			return ctx.Err()
		}

		// Check context every 10k lines to avoid tight loop
		if count%10000 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	log.Printf("backfill: read %d lines from %s", count, f.path)
	return nil
}
