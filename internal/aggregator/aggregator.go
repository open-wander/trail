package aggregator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log"
	"net/netip"
	"net/url"
	"sync"
	"time"

	"github.com/open-wander/trail/internal/bot"
	"github.com/open-wander/trail/internal/parser"
	"github.com/oschwald/geoip2-golang/v2"
)

const (
	defaultFlushInterval = 10 * time.Second
	bufferSizeThreshold  = 1000
)

// Aggregator batches log entries in memory and periodically flushes to SQLite
type Aggregator struct {
	db            *sql.DB
	parser        *parser.Parser
	flushInterval time.Duration
	ipSalt        string
	geoReader     *geoip2.Reader

	mu            sync.Mutex
	requests      map[requestKey]*requestVal
	visitors      map[visitorKey]struct{}
	referrers     map[referrerKey]int
	userAgents    map[userAgentKey]int
	countries     map[countryKey]int
	browsers      map[browserKey]int
	osStats       map[osKey]int
	durationHist  map[durationHistKey]int
	bufferSize    int
}

type requestKey struct {
	Hour   string
	Router string
	Path   string
	Method string
	Status int
}

type requestVal struct {
	Count    int
	Bytes    int64
	Duration int64
}

type visitorKey struct {
	Hour   string
	Router string
	IPHash string
}

type referrerKey struct {
	Hour     string
	Router   string
	Referrer string
}

type userAgentKey struct {
	Hour     string
	Router   string
	Category string
}

type countryKey struct {
	Hour    string
	Router  string
	Country string
}

type browserKey struct {
	Hour    string
	Router  string
	Browser string
}

type osKey struct {
	Hour   string
	Router string
	OS     string
}

type durationHistKey struct {
	Hour   string
	Router string
	Bucket string
}

// New creates a new Aggregator with a 10-second flush interval.
// If p is nil, defaults to a Traefik parser.
// geoDBPath is optional; if empty or the file can't be opened, GeoIP lookup is disabled.
func New(db *sql.DB, p *parser.Parser, geoDBPath string) *Aggregator {
	if p == nil {
		p = parser.NewParser("traefik")
	}

	// Generate random salt for IP hashing
	saltBytes := make([]byte, 16)
	if _, err := rand.Read(saltBytes); err != nil {
		log.Printf("warning: failed to generate salt, using empty: %v", err)
	}
	salt := hex.EncodeToString(saltBytes)

	var geoReader *geoip2.Reader
	if geoDBPath != "" {
		var err error
		geoReader, err = geoip2.Open(geoDBPath)
		if err != nil {
			log.Printf("warning: failed to open GeoIP database at %s: %v (country lookup disabled)", geoDBPath, err)
		} else {
			log.Printf("GeoIP database loaded: %s", geoDBPath)
		}
	}

	return &Aggregator{
		db:            db,
		parser:        p,
		flushInterval: defaultFlushInterval,
		ipSalt:        salt,
		geoReader:     geoReader,
		requests:      make(map[requestKey]*requestVal),
		visitors:      make(map[visitorKey]struct{}),
		referrers:     make(map[referrerKey]int),
		userAgents:    make(map[userAgentKey]int),
		countries:     make(map[countryKey]int),
		browsers:      make(map[browserKey]int),
		osStats:       make(map[osKey]int),
		durationHist:  make(map[durationHistKey]int),
	}
}

// Run processes log lines from the channel, accumulating in memory and flushing periodically
func (a *Aggregator) Run(ctx context.Context, lines <-chan string) error {
	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Flush remaining buffer before returning
			if err := a.flush(ctx); err != nil {
				log.Printf("error flushing on shutdown: %v", err)
				return err
			}
			return nil

		case <-ticker.C:
			if err := a.flush(ctx); err != nil {
				log.Printf("error during periodic flush: %v", err)
				return err
			}

		case line, ok := <-lines:
			if !ok {
				// Channel closed, flush and return
				if err := a.flush(ctx); err != nil {
					log.Printf("error flushing on channel close: %v", err)
					return err
				}
				return nil
			}

			// Parse the line
			entry, err := a.parser.ParseLine(line)
			if err != nil {
				log.Printf("warning: skipping unparseable line: %v", err)
				continue
			}

			// Accumulate in memory
			a.accumulate(entry)

			// Check if buffer size threshold is reached
			a.mu.Lock()
			size := a.bufferSize
			a.mu.Unlock()

			if size >= bufferSizeThreshold {
				if err := a.flush(ctx); err != nil {
					log.Printf("error during buffer threshold flush: %v", err)
					return err
				}
			}
		}
	}
}

// accumulate adds a log entry to the in-memory buffers
func (a *Aggregator) accumulate(entry *parser.LogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Determine router value (use "unrouted" if empty)
	router := entry.Router
	if router == "" {
		router = "unrouted"
	}

	// Get hour bucket
	hour := parser.HourBucket(entry.Timestamp)

	// Accumulate requests
	reqKey := requestKey{
		Hour:   hour,
		Router: router,
		Path:   entry.Path,
		Method: entry.Method,
		Status: entry.Status,
	}

	if val, exists := a.requests[reqKey]; exists {
		val.Count++
		val.Bytes += entry.Bytes
		val.Duration += int64(entry.DurationMs)
	} else {
		a.requests[reqKey] = &requestVal{
			Count:    1,
			Bytes:    entry.Bytes,
			Duration: int64(entry.DurationMs),
		}
	}

	// Accumulate visitors (unique IP per hour per router)
	// Only count non-bot, routed traffic
	if bot.Classify(entry) == bot.CategoryHuman {
		visKey := visitorKey{
			Hour:   hour,
			Router: router,
			IPHash: hashIP(entry.IP, a.ipSalt),
		}
		a.visitors[visKey] = struct{}{}
	}

	// Accumulate referrers
	if entry.Referer != "" {
		domain := extractDomain(entry.Referer)
		if domain != "" {
			refKey := referrerKey{
				Hour:     hour,
				Router:   router,
				Referrer: domain,
			}
			a.referrers[refKey]++
		}
	}

	// Accumulate user agents
	category := bot.ClassifyUA(entry.UserAgent)
	uaKey := userAgentKey{
		Hour:     hour,
		Router:   router,
		Category: category,
	}
	a.userAgents[uaKey]++

	// Accumulate browser breakdown
	browser := bot.ClassifyBrowser(entry.UserAgent)
	bKey := browserKey{
		Hour:    hour,
		Router:  router,
		Browser: browser,
	}
	a.browsers[bKey]++

	// Accumulate OS breakdown
	osName := bot.ClassifyOS(entry.UserAgent)
	oKey := osKey{
		Hour:   hour,
		Router: router,
		OS:     osName,
	}
	a.osStats[oKey]++

	// Accumulate duration histogram
	bucket := durationBucket(entry.DurationMs)
	dhKey := durationHistKey{
		Hour:   hour,
		Router: router,
		Bucket: bucket,
	}
	a.durationHist[dhKey]++

	// Accumulate country (GeoIP lookup)
	if a.geoReader != nil {
		if country := lookupCountry(a.geoReader, entry.IP); country != "" {
			cKey := countryKey{
				Hour:    hour,
				Router:  router,
				Country: country,
			}
			a.countries[cKey]++
		}
	}

	a.bufferSize++
}

// flush writes accumulated data to SQLite in a transaction
func (a *Aggregator) flush(ctx context.Context) error {
	a.mu.Lock()
	// Take snapshots of all buffers
	requests := a.requests
	visitors := a.visitors
	referrers := a.referrers
	userAgents := a.userAgents
	countries := a.countries
	browsers := a.browsers
	osStats := a.osStats
	durationHist := a.durationHist
	bufSize := a.bufferSize

	// Reset buffers
	a.requests = make(map[requestKey]*requestVal)
	a.visitors = make(map[visitorKey]struct{})
	a.referrers = make(map[referrerKey]int)
	a.userAgents = make(map[userAgentKey]int)
	a.countries = make(map[countryKey]int)
	a.browsers = make(map[browserKey]int)
	a.osStats = make(map[osKey]int)
	a.durationHist = make(map[durationHistKey]int)
	a.bufferSize = 0
	a.mu.Unlock()

	// Nothing to flush
	if bufSize == 0 {
		return nil
	}

	// Begin transaction
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Flush requests
	reqStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO requests (hour, router, path, method, status, count, bytes, duration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(hour, router, path, method, status) DO UPDATE SET
			count = count + excluded.count,
			bytes = bytes + excluded.bytes,
			duration = duration + excluded.duration
	`)
	if err != nil {
		return err
	}
	defer reqStmt.Close()

	for key, val := range requests {
		if _, err := reqStmt.ExecContext(ctx, key.Hour, key.Router, key.Path, key.Method, key.Status, val.Count, val.Bytes, val.Duration); err != nil {
			return err
		}
	}

	// Flush visitors
	visStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO visitors (hour, router, ip_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(hour, router, ip_hash) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer visStmt.Close()

	for key := range visitors {
		if _, err := visStmt.ExecContext(ctx, key.Hour, key.Router, key.IPHash); err != nil {
			return err
		}
	}

	// Flush referrers
	refStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO referrers (hour, router, referrer, count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(hour, router, referrer) DO UPDATE SET
			count = count + excluded.count
	`)
	if err != nil {
		return err
	}
	defer refStmt.Close()

	for key, count := range referrers {
		if _, err := refStmt.ExecContext(ctx, key.Hour, key.Router, key.Referrer, count); err != nil {
			return err
		}
	}

	// Flush user agents
	uaStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO user_agents (hour, router, category, count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(hour, router, category) DO UPDATE SET
			count = count + excluded.count
	`)
	if err != nil {
		return err
	}
	defer uaStmt.Close()

	for key, count := range userAgents {
		if _, err := uaStmt.ExecContext(ctx, key.Hour, key.Router, key.Category, count); err != nil {
			return err
		}
	}

	// Flush countries
	if len(countries) > 0 {
		countryStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO countries (hour, router, country, count)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(hour, router, country) DO UPDATE SET
				count = count + excluded.count
		`)
		if err != nil {
			return err
		}
		defer countryStmt.Close()

		for key, count := range countries {
			if _, err := countryStmt.ExecContext(ctx, key.Hour, key.Router, key.Country, count); err != nil {
				return err
			}
		}
	}

	// Flush browsers
	if len(browsers) > 0 {
		browserStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO browsers (hour, router, browser, count)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(hour, router, browser) DO UPDATE SET
				count = count + excluded.count
		`)
		if err != nil {
			return err
		}
		defer browserStmt.Close()

		for key, count := range browsers {
			if _, err := browserStmt.ExecContext(ctx, key.Hour, key.Router, key.Browser, count); err != nil {
				return err
			}
		}
	}

	// Flush OS stats
	if len(osStats) > 0 {
		osStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO os_stats (hour, router, os, count)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(hour, router, os) DO UPDATE SET
				count = count + excluded.count
		`)
		if err != nil {
			return err
		}
		defer osStmt.Close()

		for key, count := range osStats {
			if _, err := osStmt.ExecContext(ctx, key.Hour, key.Router, key.OS, count); err != nil {
				return err
			}
		}
	}

	// Flush duration histogram
	if len(durationHist) > 0 {
		dhStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO duration_hist (hour, router, bucket, count)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(hour, router, bucket) DO UPDATE SET
				count = count + excluded.count
		`)
		if err != nil {
			return err
		}
		defer dhStmt.Close()

		for key, count := range durationHist {
			if _, err := dhStmt.ExecContext(ctx, key.Hour, key.Router, key.Bucket, count); err != nil {
				return err
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("flushed %d entries to database", bufSize)
	return nil
}

// hashIP creates a SHA-256 hash of IP + salt, truncated to 16 hex characters
func hashIP(ip, salt string) string {
	h := sha256.Sum256([]byte(salt + ip))
	return hex.EncodeToString(h[:8])
}

// extractDomain extracts just the domain from a referrer URL
func extractDomain(referer string) string {
	u, err := url.Parse(referer)
	if err != nil {
		return ""
	}
	return u.Host
}

// durationBucket returns the histogram bucket label for a given duration in ms.
func durationBucket(ms int) string {
	switch {
	case ms <= 10:
		return "0-10ms"
	case ms <= 50:
		return "10-50ms"
	case ms <= 100:
		return "50-100ms"
	case ms <= 500:
		return "100-500ms"
	case ms <= 1000:
		return "500-1000ms"
	default:
		return "1000+ms"
	}
}

// lookupCountry returns the ISO country code for an IP address.
// Returns empty string on lookup failure.
func lookupCountry(reader *geoip2.Reader, ipStr string) string {
	addr, err := netip.ParseAddr(ipStr)
	if err != nil {
		return ""
	}
	record, err := reader.Country(addr)
	if err != nil {
		return ""
	}
	code := record.Country.ISOCode
	if code == "" {
		return ""
	}
	return code
}
