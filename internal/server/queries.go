package server

import (
	"database/sql"
	"fmt"
	"strings"
)

// Queries wraps database access for dashboard metrics
type Queries struct {
	db *sql.DB
}

// NewQueries creates a new query handler
func NewQueries(db *sql.DB) *Queries {
	return &Queries{db: db}
}

// Filter defines common filtering parameters for queries
type Filter struct {
	From        string // hour start, e.g. "2026-02-08T00:00:00Z"
	To          string // hour end, e.g. "2026-02-08T23:00:00Z"
	Router      string // empty = all routers, or specific router name
	IncludeBots bool   // if false, exclude router="unrouted" and bot UA categories
}

// TimeSeriesPoint represents a single time-based data point
type TimeSeriesPoint struct {
	Label string // hour or date
	Count int64
}

// PathStat represents statistics for a single path
type PathStat struct {
	Path  string
	Count int64
	AvgMs int64 // average duration
	Bytes int64
	Pct   float64
	Trend []int64 // daily request counts for sparkline
}

// UserAgentStat represents statistics for a user agent category
type UserAgentStat struct {
	Category string
	Count    int64
	Pct      float64
}

// MethodStat represents statistics for an HTTP method
type MethodStat struct {
	Method string
	Count  int64
	Pct    float64
}

// SpecificStatusStat represents statistics for a specific HTTP status code
type SpecificStatusStat struct {
	Status int
	Class  string
	Count  int64
	Pct    float64
}

// HourOfDayStat represents request distribution for an hour of the day
type HourOfDayStat struct {
	Hour  int
	Count int64
	Pct   float64
}

// PathDetail represents method x status breakdown for a single path
type PathDetail struct {
	Method string
	Status int
	Count  int64
	Bytes  int64
	AvgMs  int64
}

// PaginatedResult wraps a paginated query result
type PaginatedResult struct {
	Items      interface{}
	TotalCount int64
	Page       int
	Limit      int
	TotalPages int
}

// PathsSummaryResult holds aggregate stats across all paths
type PathsSummaryResult struct {
	TotalHits  int64
	TotalBytes int64
	AvgMs      int64
	MinMs      int64
	MaxMs      int64
}

// ReferrerStat represents statistics for a single referrer
type ReferrerStat struct {
	Referrer string
	Count    int64
}

// StatusStat represents statistics for a status code class
type StatusStat struct {
	Class string // "2xx", "3xx", "4xx", "5xx"
	Count int64
}

// TotalStat represents summary statistics
type TotalStat struct {
	Requests int64
	Visitors int64
	Bytes    int64
	AvgMs    int64
}

// ScannerStat represents statistics for a scanner IP
type ScannerStat struct {
	IPHash string
	Count  int64
}

// ThreatPatternStat represents a threat category with count and example paths
type ThreatPatternStat struct {
	Category string
	Count    int64
	Examples []string
	Pct      float64
}

// buildWhere constructs WHERE clause with conditions based on filter
func buildWhere(f Filter) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "hour >= ?", "hour <= ?")
	args = append(args, f.From, f.To)

	if f.Router != "" {
		conditions = append(conditions, "router = ?")
		args = append(args, f.Router)
	}
	if !f.IncludeBots {
		conditions = append(conditions, "router != 'unrouted'")
	}

	return "WHERE " + strings.Join(conditions, " AND "), args
}

// RequestsOverTime returns hourly/daily request counts
func (q *Queries) RequestsOverTime(f Filter) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT hour, SUM(count) as total
		FROM requests
		%s
		GROUP BY hour
		ORDER BY hour
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// DailyRequestsOverTime returns daily request counts (for 7d/30d views)
func (q *Queries) DailyRequestsOverTime(f Filter) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT SUBSTR(hour, 1, 10) as day, SUM(count) as total
		FROM requests
		%s
		GROUP BY day
		ORDER BY day
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// DailyVisitors returns daily unique visitor counts (for 7d/30d views)
func (q *Queries) DailyVisitors(f Filter) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT SUBSTR(hour, 1, 10) as day, COUNT(DISTINCT ip_hash) as total
		FROM visitors
		%s
		GROUP BY day
		ORDER BY day
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// TopPaths returns top paths by request count
func (q *Queries) TopPaths(f Filter, limit int) ([]PathStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms,
			SUM(bytes) as total_bytes
		FROM requests
		%s
		GROUP BY path
		ORDER BY total_count DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs, &stat.Bytes); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// TopReferrers returns top referrers by count
func (q *Queries) TopReferrers(f Filter, limit int) ([]ReferrerStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT referrer, SUM(count) as total
		FROM referrers
		%s
		GROUP BY referrer
		ORDER BY total DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ReferrerStat
	for rows.Next() {
		var stat ReferrerStat
		if err := rows.Scan(&stat.Referrer, &stat.Count); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// StatusBreakdown returns status code class breakdown
func (q *Queries) StatusBreakdown(f Filter) ([]StatusStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			CASE
				WHEN status >= 200 AND status < 300 THEN '2xx'
				WHEN status >= 300 AND status < 400 THEN '3xx'
				WHEN status >= 400 AND status < 500 THEN '4xx'
				WHEN status >= 500 AND status < 600 THEN '5xx'
				ELSE 'other'
			END as class,
			SUM(count) as total
		FROM requests
		%s
		GROUP BY class
		ORDER BY class
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []StatusStat
	for rows.Next() {
		var stat StatusStat
		if err := rows.Scan(&stat.Class, &stat.Count); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// UniqueVisitors returns unique visitor counts per hour
func (q *Queries) UniqueVisitors(f Filter) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT hour, COUNT(DISTINCT ip_hash) as total
		FROM visitors
		%s
		GROUP BY hour
		ORDER BY hour
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// TotalStats returns summary statistics
func (q *Queries) TotalStats(f Filter) (*TotalStat, error) {
	where, args := buildWhere(f)

	// Get request stats
	requestQuery := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(count), 0) as total_requests,
			COALESCE(SUM(bytes), 0) as total_bytes,
			CASE
				WHEN COALESCE(SUM(count), 0) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms
		FROM requests
		%s
	`, where)

	var stat TotalStat
	err := q.db.QueryRow(requestQuery, args...).Scan(&stat.Requests, &stat.Bytes, &stat.AvgMs)
	if err != nil {
		return nil, err
	}

	// Get visitor count
	visitorQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT ip_hash)
		FROM visitors
		%s
	`, where)

	err = q.db.QueryRow(visitorQuery, args...).Scan(&stat.Visitors)
	if err != nil {
		return nil, err
	}

	return &stat, nil
}

// Routers returns list of all distinct routers
func (q *Queries) Routers() ([]string, error) {
	query := `
		SELECT DISTINCT router
		FROM requests
		WHERE router != ''
		ORDER BY router
	`

	rows, err := q.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var routers []string
	for rows.Next() {
		var router string
		if err := rows.Scan(&router); err != nil {
			return nil, err
		}
		routers = append(routers, router)
	}

	return routers, rows.Err()
}

// SecurityOverTime returns unrouted and bot traffic over time
func (q *Queries) SecurityOverTime(f Filter) ([]TimeSeriesPoint, error) {
	// Build custom where clause that includes unrouted traffic
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "hour >= ?", "hour <= ?")
	args = append(args, f.From, f.To)

	// For security view, we want unrouted traffic
	conditions = append(conditions, "router = 'unrouted'")

	where := "WHERE " + strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
		SELECT hour, SUM(count) as total
		FROM requests
		%s
		GROUP BY hour
		ORDER BY hour
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// TopProbedPaths returns most frequently probed paths from unrouted traffic
func (q *Queries) TopProbedPaths(limit int) ([]PathStat, error) {
	query := `
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms
		FROM requests
		WHERE router = 'unrouted'
		GROUP BY path
		ORDER BY total_count DESC
		LIMIT ?
	`

	rows, err := q.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// TopScannerIPs returns top scanner IPs from unrouted traffic
func (q *Queries) TopScannerIPs(limit int) ([]ScannerStat, error) {
	query := `
		SELECT ip_hash, COUNT(*) as total
		FROM visitors
		WHERE router = 'unrouted'
		GROUP BY ip_hash
		ORDER BY total DESC
		LIMIT ?
	`

	rows, err := q.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ScannerStat
	for rows.Next() {
		var stat ScannerStat
		if err := rows.Scan(&stat.IPHash, &stat.Count); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// TopNotFound returns top paths with 404 status
func (q *Queries) TopNotFound(f Filter, limit int) ([]PathStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms,
			SUM(bytes) as total_bytes
		FROM requests
		%s AND status = 404
		GROUP BY path
		ORDER BY total_count DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs, &stat.Bytes); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// UserAgentBreakdown returns user agent category distribution
func (q *Queries) UserAgentBreakdown(f Filter) ([]UserAgentStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT category, SUM(count) as total
		FROM user_agents
		%s
		GROUP BY category
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []UserAgentStat
	var grandTotal int64
	for rows.Next() {
		var stat UserAgentStat
		if err := rows.Scan(&stat.Category, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// MethodBreakdown returns HTTP method distribution
func (q *Queries) MethodBreakdown(f Filter) ([]MethodStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT method, SUM(count) as total
		FROM requests
		%s
		GROUP BY method
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []MethodStat
	var grandTotal int64
	for rows.Next() {
		var stat MethodStat
		if err := rows.Scan(&stat.Method, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// SpecificStatusCodes returns individual status code breakdown
func (q *Queries) SpecificStatusCodes(f Filter) ([]SpecificStatusStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			status,
			CASE
				WHEN status >= 200 AND status < 300 THEN '2xx'
				WHEN status >= 300 AND status < 400 THEN '3xx'
				WHEN status >= 400 AND status < 500 THEN '4xx'
				WHEN status >= 500 AND status < 600 THEN '5xx'
				ELSE 'other'
			END as class,
			SUM(count) as total
		FROM requests
		%s
		GROUP BY status
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SpecificStatusStat
	var grandTotal int64
	for rows.Next() {
		var stat SpecificStatusStat
		if err := rows.Scan(&stat.Status, &stat.Class, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// HourOfDayDistribution returns request distribution by hour of day (0-23)
func (q *Queries) HourOfDayDistribution(f Filter) ([]HourOfDayStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			CAST(SUBSTR(hour, 12, 2) AS INTEGER) as hod,
			SUM(count) as total
		FROM requests
		%s
		GROUP BY hod
		ORDER BY hod
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []HourOfDayStat
	var grandTotal int64
	for rows.Next() {
		var stat HourOfDayStat
		if err := rows.Scan(&stat.Hour, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// PathDrilldown returns method x status detail for a specific path
func (q *Queries) PathDrilldown(f Filter, path string) ([]PathDetail, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			method,
			status,
			SUM(count) as total_count,
			SUM(bytes) as total_bytes,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms
		FROM requests
		%s AND path = ?
		GROUP BY method, status
		ORDER BY total_count DESC
	`, where)

	args = append(args, path)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathDetail
	for rows.Next() {
		var detail PathDetail
		if err := rows.Scan(&detail.Method, &detail.Status, &detail.Count, &detail.Bytes, &detail.AvgMs); err != nil {
			return nil, err
		}
		results = append(results, detail)
	}

	return results, rows.Err()
}

// StatusClassDrilldown returns individual status codes within a class (e.g., all codes in 4xx)
func (q *Queries) StatusClassDrilldown(f Filter, class string) ([]SpecificStatusStat, error) {
	where, args := buildWhere(f)

	var statusMin, statusMax int
	switch class {
	case "2xx":
		statusMin, statusMax = 200, 299
	case "3xx":
		statusMin, statusMax = 300, 399
	case "4xx":
		statusMin, statusMax = 400, 499
	case "5xx":
		statusMin, statusMax = 500, 599
	default:
		return nil, fmt.Errorf("invalid status class: %s", class)
	}

	query := fmt.Sprintf(`
		SELECT
			status,
			'%s' as class,
			SUM(count) as total
		FROM requests
		%s AND status >= ? AND status <= ?
		GROUP BY status
		ORDER BY total DESC
	`, class, where)

	args = append(args, statusMin, statusMax)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SpecificStatusStat
	var grandTotal int64
	for rows.Next() {
		var stat SpecificStatusStat
		if err := rows.Scan(&stat.Status, &stat.Class, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// StatusCodePathStat represents a path's stats for a specific status code
type StatusCodePathStat struct {
	Path        string
	Count       int64
	Bytes       int64
	AvgMs       int64
	AltStatuses []AltStatus
}

// AltStatus represents an alternate status code returned for a path
type AltStatus struct {
	Status int
	Count  int64
}

// StatusCodeMethodStat represents a method's stats for a specific status code
type StatusCodeMethodStat struct {
	Method string
	Count  int64
	Pct    float64
}

// StatusCodePaths returns top paths that return a specific status code
func (q *Queries) StatusCodePaths(f Filter, code int, limit int) ([]StatusCodePathStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			SUM(bytes) as total_bytes,
			CASE WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count) ELSE 0 END as avg_ms
		FROM requests
		%s AND status = ?
		GROUP BY path
		ORDER BY total_count DESC
		LIMIT ?
	`, where)

	args = append(args, code, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []StatusCodePathStat
	for rows.Next() {
		var stat StatusCodePathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.Bytes, &stat.AvgMs); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// PathAlternateStatuses returns other status codes a path returns, excluding the given status
func (q *Queries) PathAlternateStatuses(f Filter, path string, excludeStatus int) ([]AltStatus, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT status, SUM(count) as total
		FROM requests
		%s AND path = ? AND status != ?
		GROUP BY status
		ORDER BY total DESC
		LIMIT 5
	`, where)

	args = append(args, path, excludeStatus)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []AltStatus
	for rows.Next() {
		var alt AltStatus
		if err := rows.Scan(&alt.Status, &alt.Count); err != nil {
			return nil, err
		}
		results = append(results, alt)
	}

	return results, rows.Err()
}

// StatusCodeMethods returns method breakdown for a specific status code
func (q *Queries) StatusCodeMethods(f Filter, code int) ([]StatusCodeMethodStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT method, SUM(count) as total
		FROM requests
		%s AND status = ?
		GROUP BY method
		ORDER BY total DESC
	`, where)

	args = append(args, code)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []StatusCodeMethodStat
	var grandTotal int64
	for rows.Next() {
		var stat StatusCodeMethodStat
		if err := rows.Scan(&stat.Method, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// TopPathsPaginated returns paginated top paths with sorting
func (q *Queries) TopPathsPaginated(f Filter, page, limit int, sort, order string) (*PaginatedResult, error) {
	where, args := buildWhere(f)

	// Validate sort column
	sortCol := "total_count"
	switch sort {
	case "path":
		sortCol = "path"
	case "bytes":
		sortCol = "total_bytes"
	case "avg_ms":
		sortCol = "avg_ms"
	case "count":
		sortCol = "total_count"
	}

	// Validate order
	orderDir := "DESC"
	if order == "asc" {
		orderDir = "ASC"
	}

	// Count total rows
	countQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT path)
		FROM requests
		%s
	`, where)

	var totalCount int64
	err := q.db.QueryRow(countQuery, args...).Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	totalPages := int(totalCount) / limit
	if int(totalCount)%limit > 0 {
		totalPages++
	}

	offset := (page - 1) * limit

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms,
			SUM(bytes) as total_bytes
		FROM requests
		%s
		GROUP BY path
		ORDER BY %s %s
		LIMIT ? OFFSET ?
	`, where, sortCol, orderDir)

	args = append(args, limit, offset)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Calculate grand total for percentages
	var grandTotal int64
	var items []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs, &stat.Bytes); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		items = append(items, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &PaginatedResult{
		Items:      items,
		TotalCount: totalCount,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	}, nil
}

// PathsSummary returns aggregate stats across all paths
func (q *Queries) PathsSummary(f Filter) (*PathsSummaryResult, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(total_count), 0) as total_hits,
			COALESCE(SUM(total_bytes), 0) as total_bytes,
			CASE WHEN COALESCE(SUM(total_count), 0) > 0 THEN SUM(total_duration) / SUM(total_count) ELSE 0 END as avg_ms,
			COALESCE(MIN(min_ms), 0) as min_ms,
			COALESCE(MAX(max_ms), 0) as max_ms
		FROM (
			SELECT
				SUM(count) as total_count,
				SUM(bytes) as total_bytes,
				SUM(duration) as total_duration,
				CASE WHEN SUM(count) > 0 THEN MIN(duration / CASE WHEN count > 0 THEN count ELSE 1 END) ELSE 0 END as min_ms,
				CASE WHEN SUM(count) > 0 THEN MAX(duration / CASE WHEN count > 0 THEN count ELSE 1 END) ELSE 0 END as max_ms
			FROM requests
			%s
			GROUP BY path
		)
	`, where)

	var result PathsSummaryResult
	err := q.db.QueryRow(query, args...).Scan(
		&result.TotalHits,
		&result.TotalBytes,
		&result.AvgMs,
		&result.MinMs,
		&result.MaxMs,
	)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// ThreatPatterns groups suspicious request paths into attack categories.
// When suspiciousPathMode is true (combined format), it uses status >= 400
// instead of router = 'unrouted' since combined logs have no router concept.
func (q *Queries) ThreatPatterns(f Filter, suspiciousPathMode bool) ([]ThreatPatternStat, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "hour >= ?", "hour <= ?")
	args = append(args, f.From, f.To)

	if suspiciousPathMode {
		conditions = append(conditions, "status >= 400")
	} else {
		conditions = append(conditions, "router = 'unrouted'")
	}

	where := "WHERE " + strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
		SELECT
			CASE
				WHEN path LIKE '%%wp-%%' OR path LIKE '%%wordpress%%' OR path LIKE '%%xmlrpc%%' THEN 'WordPress'
				WHEN path LIKE '%%.env%%' OR path LIKE '%%.git%%' OR path LIKE '%%.aws%%' OR path LIKE '%%config%%' THEN 'Environment'
				WHEN path LIKE '%%admin%%' OR path LIKE '%%phpmyadmin%%' OR path LIKE '%%manager%%' OR path LIKE '%%cpanel%%' THEN 'Admin Panels'
				WHEN path LIKE '%%.php%%' OR path LIKE '%%.asp%%' OR path LIKE '%%.cgi%%' THEN 'Scripts'
				ELSE 'Other'
			END as category,
			SUM(count) as total,
			GROUP_CONCAT(DISTINCT path) as examples
		FROM requests
		%s
		GROUP BY category
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ThreatPatternStat
	var grandTotal int64
	for rows.Next() {
		var stat ThreatPatternStat
		var examplesStr string
		if err := rows.Scan(&stat.Category, &stat.Count, &examplesStr); err != nil {
			return nil, err
		}
		// Take first 3 examples
		parts := strings.Split(examplesStr, ",")
		limit := 3
		if len(parts) < limit {
			limit = len(parts)
		}
		stat.Examples = parts[:limit]
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// BotVsHuman returns bot and human traffic counts from user_agents
func (q *Queries) BotVsHuman(f Filter) (humanCount, botCount int64, botBreakdown []UserAgentStat, err error) {
	where, args := buildWhere(Filter{
		From:        f.From,
		To:          f.To,
		IncludeBots: true, // we want all traffic for this comparison
	})

	query := fmt.Sprintf(`
		SELECT category, SUM(count) as total
		FROM user_agents
		%s
		GROUP BY category
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return 0, 0, nil, err
	}
	defer rows.Close()

	// Known bot categories (case-insensitive match)
	botCategories := map[string]bool{
		"bot":     true,
		"crawler": true,
		"spider":  true,
		"scraper": true,
	}

	for rows.Next() {
		var stat UserAgentStat
		if err := rows.Scan(&stat.Category, &stat.Count); err != nil {
			return 0, 0, nil, err
		}
		lower := strings.ToLower(stat.Category)
		isBot := botCategories[lower]
		if !isBot {
			// Also match categories containing "bot"
			isBot = strings.Contains(lower, "bot") || strings.Contains(lower, "crawl") || strings.Contains(lower, "spider")
		}
		if isBot {
			botCount += stat.Count
			botBreakdown = append(botBreakdown, stat)
		} else {
			humanCount += stat.Count
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, err
	}

	return humanCount, botCount, botBreakdown, nil
}

// ErrorTrends returns 5xx error counts over time (hourly or daily based on range)
func (q *Queries) ErrorTrends(f Filter) ([]TimeSeriesPoint, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "hour >= ?", "hour <= ?")
	args = append(args, f.From, f.To)
	conditions = append(conditions, "status >= 500 AND status < 600")

	where := "WHERE " + strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
		SELECT SUBSTR(hour, 1, 10) as day, SUM(count) as total
		FROM requests
		%s
		GROUP BY day
		ORDER BY day
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// ErrorPaths returns paths with the most 5xx errors
func (q *Queries) ErrorPaths(f Filter, limit int) ([]PathStat, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "hour >= ?", "hour <= ?")
	args = append(args, f.From, f.To)
	conditions = append(conditions, "status >= 500 AND status < 600")

	where := "WHERE " + strings.Join(conditions, " AND ")

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms,
			SUM(bytes) as total_bytes
		FROM requests
		%s
		GROUP BY path
		ORDER BY total_count DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs, &stat.Bytes); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// SlowestPaths returns paths with the highest average response time
func (q *Queries) SlowestPaths(f Filter, limit int) ([]PathStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			path,
			SUM(count) as total_count,
			CASE
				WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count)
				ELSE 0
			END as avg_ms,
			SUM(bytes) as total_bytes
		FROM requests
		%s AND count > 0
		GROUP BY path
		HAVING total_count >= 5
		ORDER BY avg_ms DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PathStat
	for rows.Next() {
		var stat PathStat
		if err := rows.Scan(&stat.Path, &stat.Count, &stat.AvgMs, &stat.Bytes); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// HourOfDayVisitors returns unique visitor distribution by hour of day (0-23)
func (q *Queries) HourOfDayVisitors(f Filter) ([]HourOfDayStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT
			CAST(SUBSTR(hour, 12, 2) AS INTEGER) as hod,
			COUNT(DISTINCT ip_hash) as total
		FROM visitors
		%s
		GROUP BY hod
		ORDER BY hod
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []HourOfDayStat
	for rows.Next() {
		var stat HourOfDayStat
		if err := rows.Scan(&stat.Hour, &stat.Count); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}

	return results, rows.Err()
}

// PathDailyTrends returns daily request counts for a set of paths
func (q *Queries) PathDailyTrends(f Filter, paths []string) (map[string][]int64, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	where, args := buildWhere(f)

	placeholders := make([]string, len(paths))
	for i, p := range paths {
		placeholders[i] = "?"
		args = append(args, p)
	}

	query := fmt.Sprintf(`
		SELECT path, SUBSTR(hour, 1, 10) as day, SUM(count) as total
		FROM requests
		%s AND path IN (%s)
		GROUP BY path, day
		ORDER BY path, day
	`, where, strings.Join(placeholders, ","))

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]int64)
	for rows.Next() {
		var path, day string
		var count int64
		if err := rows.Scan(&path, &day, &count); err != nil {
			return nil, err
		}
		result[path] = append(result[path], count)
	}

	return result, rows.Err()
}

// CountryStat represents statistics for a single country
type CountryStat struct {
	Country string
	Count   int64
	Pct     float64
}

// BrowserStat represents statistics for a browser
type BrowserStat struct {
	Browser string
	Count   int64
	Pct     float64
}

// OSStat represents statistics for an operating system
type OSStat struct {
	OS    string
	Count int64
	Pct   float64
}

// DurationBucketStat represents a histogram bucket for response times
type DurationBucketStat struct {
	Bucket string
	Count  int64
	Pct    float64
}

// PercentileResult holds computed response time percentiles
type PercentileResult struct {
	P50 int64
	P95 int64
	P99 int64
}

// CountryBreakdown returns country distribution from GeoIP data
func (q *Queries) CountryBreakdown(f Filter, limit int) ([]CountryStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT country, SUM(count) as total
		FROM countries
		%s
		GROUP BY country
		ORDER BY total DESC
		LIMIT ?
	`, where)

	args = append(args, limit)
	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CountryStat
	var grandTotal int64
	for rows.Next() {
		var stat CountryStat
		if err := rows.Scan(&stat.Country, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// BrowserBreakdown returns browser distribution
func (q *Queries) BrowserBreakdown(f Filter) ([]BrowserStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT browser, SUM(count) as total
		FROM browsers
		%s
		GROUP BY browser
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []BrowserStat
	var grandTotal int64
	for rows.Next() {
		var stat BrowserStat
		if err := rows.Scan(&stat.Browser, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// OSBreakdown returns operating system distribution
func (q *Queries) OSBreakdown(f Filter) ([]OSStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT os, SUM(count) as total
		FROM os_stats
		%s
		GROUP BY os
		ORDER BY total DESC
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []OSStat
	var grandTotal int64
	for rows.Next() {
		var stat OSStat
		if err := rows.Scan(&stat.OS, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// DurationHistogram returns the response time histogram
func (q *Queries) DurationHistogram(f Filter) ([]DurationBucketStat, error) {
	where, args := buildWhere(f)

	query := fmt.Sprintf(`
		SELECT bucket, SUM(count) as total
		FROM duration_hist
		%s
		GROUP BY bucket
		ORDER BY
			CASE bucket
				WHEN '0-10ms' THEN 1
				WHEN '10-50ms' THEN 2
				WHEN '50-100ms' THEN 3
				WHEN '100-500ms' THEN 4
				WHEN '500-1000ms' THEN 5
				WHEN '1000+ms' THEN 6
			END
	`, where)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []DurationBucketStat
	var grandTotal int64
	for rows.Next() {
		var stat DurationBucketStat
		if err := rows.Scan(&stat.Bucket, &stat.Count); err != nil {
			return nil, err
		}
		grandTotal += stat.Count
		results = append(results, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range results {
		if grandTotal > 0 {
			results[i].Pct = float64(results[i].Count) / float64(grandTotal) * 100
		}
	}

	return results, nil
}

// DurationPercentiles computes p50, p95, p99 from the duration histogram buckets.
// Uses bucket midpoints for interpolation: 5, 30, 75, 300, 750, 2000ms.
func (q *Queries) DurationPercentiles(f Filter) (*PercentileResult, error) {
	hist, err := q.DurationHistogram(f)
	if err != nil {
		return nil, err
	}

	if len(hist) == 0 {
		return &PercentileResult{}, nil
	}

	// Bucket midpoints in ms (ordered by bucket)
	midpoints := map[string]int64{
		"0-10ms":    5,
		"10-50ms":   30,
		"50-100ms":  75,
		"100-500ms": 300,
		"500-1000ms": 750,
		"1000+ms":   2000,
	}

	// Build cumulative distribution
	var total int64
	for _, h := range hist {
		total += h.Count
	}
	if total == 0 {
		return &PercentileResult{}, nil
	}

	// Compute percentiles from cumulative counts
	computePercentile := func(pct float64) int64 {
		threshold := int64(float64(total) * pct)
		cumulative := int64(0)
		for _, h := range hist {
			cumulative += h.Count
			if cumulative >= threshold {
				return midpoints[h.Bucket]
			}
		}
		// Fallback to last bucket midpoint
		if len(hist) > 0 {
			return midpoints[hist[len(hist)-1].Bucket]
		}
		return 0
	}

	return &PercentileResult{
		P50: computePercentile(0.50),
		P95: computePercentile(0.95),
		P99: computePercentile(0.99),
	}, nil
}

// BandwidthTimeSeries returns bytes transferred over time (hourly or daily)
func (q *Queries) BandwidthTimeSeries(f Filter, daily bool) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	var groupExpr, selectExpr string
	if daily {
		selectExpr = "SUBSTR(hour, 1, 10) as period"
		groupExpr = "period"
	} else {
		selectExpr = "hour as period"
		groupExpr = "period"
	}

	query := fmt.Sprintf(`
		SELECT %s, SUM(bytes) as total_bytes
		FROM requests
		%s
		GROUP BY %s
		ORDER BY %s
	`, selectExpr, where, groupExpr, groupExpr)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}

// ResponseTimeTimeSeries returns average response time over time (hourly or daily)
func (q *Queries) ResponseTimeTimeSeries(f Filter, daily bool) ([]TimeSeriesPoint, error) {
	where, args := buildWhere(f)

	var groupExpr, selectExpr string
	if daily {
		selectExpr = "SUBSTR(hour, 1, 10) as period"
		groupExpr = "period"
	} else {
		selectExpr = "hour as period"
		groupExpr = "period"
	}

	query := fmt.Sprintf(`
		SELECT %s,
			CASE WHEN SUM(count) > 0 THEN SUM(duration) / SUM(count) ELSE 0 END as avg_ms
		FROM requests
		%s
		GROUP BY %s
		ORDER BY %s
	`, selectExpr, where, groupExpr, groupExpr)

	rows, err := q.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []TimeSeriesPoint
	for rows.Next() {
		var point TimeSeriesPoint
		if err := rows.Scan(&point.Label, &point.Count); err != nil {
			return nil, err
		}
		results = append(results, point)
	}

	return results, rows.Err()
}
