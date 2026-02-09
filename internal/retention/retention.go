package retention

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Cleaner struct {
	db            *sql.DB
	retentionDays int
	interval      time.Duration
}

// New creates a new retention cleaner with a default interval of 1 hour.
func New(db *sql.DB, retentionDays int) *Cleaner {
	return &Cleaner{
		db:            db,
		retentionDays: retentionDays,
		interval:      time.Hour,
	}
}

// Run starts the retention cleanup job. It runs cleanup immediately on start,
// then repeats every interval. It respects context cancellation.
func (c *Cleaner) Run(ctx context.Context) error {
	// Run immediately on start
	if err := c.cleanup(); err != nil {
		log.Printf("retention: initial cleanup failed: %v", err)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.cleanup(); err != nil {
				log.Printf("retention: cleanup failed: %v", err)
			}
		}
	}
}

// cleanup deletes rows older than retentionDays from all time-based tables.
func (c *Cleaner) cleanup() error {
	cutoff := time.Now().UTC().AddDate(0, 0, -c.retentionDays).Truncate(time.Hour).Format(time.RFC3339)

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete from requests
	reqResult, err := tx.Exec("DELETE FROM requests WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete requests: %w", err)
	}
	reqCount, _ := reqResult.RowsAffected()

	// Delete from visitors
	visResult, err := tx.Exec("DELETE FROM visitors WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete visitors: %w", err)
	}
	visCount, _ := visResult.RowsAffected()

	// Delete from referrers
	refResult, err := tx.Exec("DELETE FROM referrers WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete referrers: %w", err)
	}
	refCount, _ := refResult.RowsAffected()

	// Delete from user_agents
	uaResult, err := tx.Exec("DELETE FROM user_agents WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete user_agents: %w", err)
	}
	uaCount, _ := uaResult.RowsAffected()

	// Delete from countries
	countryResult, err := tx.Exec("DELETE FROM countries WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete countries: %w", err)
	}
	countryCount, _ := countryResult.RowsAffected()

	// Delete from browsers
	browserResult, err := tx.Exec("DELETE FROM browsers WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete browsers: %w", err)
	}
	browserCount, _ := browserResult.RowsAffected()

	// Delete from os_stats
	osResult, err := tx.Exec("DELETE FROM os_stats WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete os_stats: %w", err)
	}
	osCount, _ := osResult.RowsAffected()

	// Delete from duration_hist
	dhResult, err := tx.Exec("DELETE FROM duration_hist WHERE hour < ?", cutoff)
	if err != nil {
		return fmt.Errorf("delete duration_hist: %w", err)
	}
	dhCount, _ := dhResult.RowsAffected()

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	// Parse cutoff for friendly logging
	cutoffDate := cutoff[:10] // Extract YYYY-MM-DD from RFC3339

	log.Printf("retention: deleted %d requests, %d visitors, %d referrers, %d user_agents, %d countries, %d browsers, %d os_stats, %d duration_hist older than %s",
		reqCount, visCount, refCount, uaCount, countryCount, browserCount, osCount, dhCount, cutoffDate)

	return nil
}
