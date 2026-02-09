package db

import (
	"database/sql"
	"fmt"
)

const (
	createRequestsTable = `
CREATE TABLE IF NOT EXISTS requests (
    hour     TEXT    NOT NULL,
    router   TEXT    NOT NULL,
    path     TEXT    NOT NULL,
    method   TEXT    NOT NULL,
    status   INTEGER NOT NULL,
    count    INTEGER NOT NULL DEFAULT 0,
    bytes    INTEGER NOT NULL DEFAULT 0,
    duration INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour, router, path, method, status)
)`

	createVisitorsTable = `
CREATE TABLE IF NOT EXISTS visitors (
    hour    TEXT NOT NULL,
    router  TEXT NOT NULL,
    ip_hash TEXT NOT NULL,
    PRIMARY KEY (hour, router, ip_hash)
)`

	createReferrersTable = `
CREATE TABLE IF NOT EXISTS referrers (
    hour     TEXT    NOT NULL,
    router   TEXT    NOT NULL,
    referrer TEXT    NOT NULL,
    count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour, router, referrer)
)`

	createUserAgentsTable = `
CREATE TABLE IF NOT EXISTS user_agents (
    hour     TEXT    NOT NULL,
    router   TEXT    NOT NULL,
    category TEXT    NOT NULL,
    count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour, router, category)
)`

	createLogPositionTable = `
CREATE TABLE IF NOT EXISTS log_position (
    file   TEXT    PRIMARY KEY,
    offset INTEGER NOT NULL DEFAULT 0,
    inode  INTEGER NOT NULL DEFAULT 0,
    size   INTEGER NOT NULL DEFAULT 0
)`

	createRequestsHourIndex    = `CREATE INDEX IF NOT EXISTS idx_requests_hour ON requests(hour)`
	createVisitorsHourIndex    = `CREATE INDEX IF NOT EXISTS idx_visitors_hour ON visitors(hour)`
	createReferrersHourIndex   = `CREATE INDEX IF NOT EXISTS idx_referrers_hour ON referrers(hour)`
	createUserAgentsHourIndex  = `CREATE INDEX IF NOT EXISTS idx_user_agents_hour ON user_agents(hour)`
)

// Migrate creates all tables and indexes if they don't exist.
func Migrate(db *sql.DB) error {
	statements := []string{
		createRequestsTable,
		createVisitorsTable,
		createReferrersTable,
		createUserAgentsTable,
		createLogPositionTable,
		createRequestsHourIndex,
		createVisitorsHourIndex,
		createReferrersHourIndex,
		createUserAgentsHourIndex,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	return nil
}
