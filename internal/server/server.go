package server

import (
	"bufio"
	"database/sql"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/open-wander/trail/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// Server represents the HTTP server instance
type Server struct {
	app          *fiber.App
	db           *sql.DB
	config       *config.Config
	queries      *Queries
	tmpl         *template.Template
	overviewTmpl *template.Template
	securityTmpl *template.Template
	staticFS     fs.FS
}

// New creates a new Server instance with the given configuration and database.
// templatesFS and staticFS are embedded filesystems rooted at the project root
// (i.e. containing "templates/" and "static/" subdirectories).
func New(cfg *config.Config, database *sql.DB, templatesFS, staticFS fs.FS) *Server {
	app := fiber.New(fiber.Config{
		AppName:               "Trail Analytics",
		DisableStartupMessage: false,
	})

	// Initialize queries
	queries := NewQueries(database)

	// Sub into templates/ directory so patterns are just filenames
	tmplFS, err := fs.Sub(templatesFS, "templates")
	if err != nil {
		log.Fatalf("Failed to open embedded templates: %v", err)
	}

	// Load and parse templates with helper functions
	funcMap := template.FuncMap{
		"formatBytes":     formatBytes,
		"formatNumber":    formatNumber,
		"pct":             pct,
		"statusColor":     statusColor,
		"formatPct":       formatPct,
		"add":             func(a, b int) int { return a + b },
		"sub":             func(a, b int) int { return a - b },
		"statusCodeColor": statusCodeColor,
		"intRange":        intRange,
		"formatTimeLabel": formatTimeLabel,
		"formatDate":      formatDate,
		"conicGradient":   conicGradient,
		"sparklineSVG":    sparklineSVG,
	}

	// Parse overview templates (layout + overview + partial)
	overviewTmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(tmplFS,
		"layout.html",
		"overview.html",
		"overview_partial.html",
	))

	// Parse security templates (layout + security + partial)
	securityTmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(tmplFS,
		"layout.html",
		"security.html",
		"security_partial.html",
	))

	// Parse all templates for backward compatibility with partials
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(tmplFS, "*.html"))

	// Sub into static/ directory for file serving
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("Failed to open embedded static files: %v", err)
	}

	s := &Server{
		app:          app,
		db:           database,
		config:       cfg,
		queries:      queries,
		tmpl:         tmpl,
		overviewTmpl: overviewTmpl,
		securityTmpl: securityTmpl,
		staticFS:     staticSub,
	}

	// Configure middleware and routes
	s.setupMiddleware()
	s.setupRoutes()

	return s
}

// setupMiddleware configures middleware for the application
func (s *Server) setupMiddleware() {
	// Static file serving from embedded filesystem
	s.app.Use("/static", filesystem.New(filesystem.Config{
		Root: http.FS(s.staticFS),
	}))

	// Basic auth middleware (if configured)
	if authMiddleware := s.createAuthMiddleware(); authMiddleware != nil {
		s.app.Use(authMiddleware)
	}
}

// createAuthMiddleware creates basic auth middleware based on configuration
// Returns nil if no authentication is configured
func (s *Server) createAuthMiddleware() fiber.Handler {
	// Priority 1: htpasswd file
	if s.config.HtpasswdFile != "" {
		users, err := parseHtpasswd(s.config.HtpasswdFile)
		if err != nil {
			log.Printf("Warning: Failed to parse htpasswd file: %v", err)
			return nil
		}

		return basicauth.New(basicauth.Config{
			Authorizer: func(user, pass string) bool {
				hashedPass, exists := users[user]
				if !exists {
					return false
				}
				return verifyPassword(pass, hashedPass)
			},
		})
	}

	// Priority 2: environment variable credentials
	if s.config.AuthUser != "" && s.config.AuthPass != "" {
		return basicauth.New(basicauth.Config{
			Users: map[string]string{
				s.config.AuthUser: s.config.AuthPass,
			},
		})
	}

	// No authentication configured
	return nil
}

// parseHtpasswd reads and parses an htpasswd file
// Returns a map of username to hashed password
func parseHtpasswd(filepath string) (map[string]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open htpasswd file: %w", err)
	}
	defer file.Close()

	users := make(map[string]string)
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse line format: username:hash
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			log.Printf("Warning: Invalid htpasswd entry on line %d: missing colon", lineNum)
			continue
		}

		username := parts[0]
		hash := parts[1]

		// Validate hash format (we only support bcrypt)
		if !strings.HasPrefix(hash, "$2") {
			if strings.HasPrefix(hash, "$apr1$") {
				log.Printf("Warning: APR1 MD5 hash for user '%s' is not supported (line %d). Please use bcrypt.", username, lineNum)
			} else {
				log.Printf("Warning: Unsupported hash format for user '%s' on line %d. Only bcrypt is supported.", username, lineNum)
			}
			continue
		}

		users[username] = hash
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading htpasswd file: %w", err)
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("no valid users found in htpasswd file")
	}

	return users, nil
}

// verifyPassword checks if a plaintext password matches a hashed password
func verifyPassword(plaintext, hashed string) bool {
	// Only bcrypt is supported
	if strings.HasPrefix(hashed, "$2") {
		err := bcrypt.CompareHashAndPassword([]byte(hashed), []byte(plaintext))
		return err == nil
	}
	return false
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	// Dashboard pages
	s.app.Get("/", s.handleOverview)
	s.app.Get("/security", s.handleSecurity)

	// API endpoints (htmx partials)
	s.app.Get("/api/overview", s.handleAPIOverview)
	s.app.Get("/api/security", s.handleAPISecurity)
	s.app.Get("/api/filters", s.handleAPIFilters)

	// Drilldown endpoints
	s.app.Get("/api/drilldown/path", s.handlePathDrilldown)
	s.app.Get("/api/drilldown/status", s.handleStatusDrilldown)
	s.app.Get("/api/drilldown/status-code", s.handleStatusCodeDrilldown)

	// Paginated panel endpoints
	s.app.Get("/api/panel/paths", s.handlePanelPaths)
	s.app.Get("/api/panel/referrers", s.handlePanelReferrers)
	s.app.Get("/api/panel/not-found", s.handlePanelNotFound)
}

// Start begins listening for HTTP requests
func (s *Server) Start() error {
	log.Printf("Starting server on %s", s.config.Listen)
	return s.app.Listen(s.config.Listen)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() error {
	log.Println("Shutting down server...")
	return s.app.Shutdown()
}

// Template helper functions

// formatBytes formats bytes as human-readable string (KB, MB, GB)
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// formatNumber formats an integer with comma separators
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	str := fmt.Sprintf("%d", n)
	var result []rune
	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, digit)
	}
	return string(result)
}

// pct calculates percentage for bar chart width
// Returns at least 1% if value > 0 to ensure visibility
func pct(value, max int64) int {
	if max == 0 {
		return 0
	}
	if value == 0 {
		return 0
	}
	result := int((value * 100) / max)
	if result < 1 && value > 0 {
		return 1
	}
	return result
}

// statusColor returns CSS color variable for status code class
func statusColor(class string) string {
	switch class {
	case "2xx":
		return "var(--success)"
	case "3xx":
		return "var(--brand)"
	case "4xx":
		return "var(--warning)"
	case "5xx":
		return "var(--error)"
	default:
		return "var(--text-secondary)"
	}
}

// formatPct formats a float64 percentage to one decimal place
func formatPct(p float64) string {
	return fmt.Sprintf("%.1f%%", p)
}

// statusCodeColor returns CSS color for an individual status code
func statusCodeColor(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "var(--success)"
	case status >= 300 && status < 400:
		return "var(--accent)"
	case status >= 400 && status < 500:
		return "var(--warning)"
	case status >= 500 && status < 600:
		return "var(--error)"
	default:
		return "var(--text-secondary)"
	}
}

// intRange returns a slice of ints from 0 to n-1, useful for template loops
func intRange(n int) []int {
	result := make([]int, n)
	for i := range result {
		result[i] = i
	}
	return result
}

// formatTimeLabel formats time labels for display
// Handles both hourly ("2026-02-08T00:00:00Z" -> "Feb 08 00h")
// and daily ("2026-02-08" -> "Feb 08") formats
func formatTimeLabel(label string) string {
	// Try RFC3339 (hourly)
	if t, err := time.Parse(time.RFC3339, label); err == nil {
		return t.Format("Jan 02 15h")
	}
	// Try date-only (daily)
	if t, err := time.Parse("2006-01-02", label); err == nil {
		return t.Format("Jan 02 (Mon)")
	}
	return label
}

// formatDate returns today's date in YYYY-MM-DD format, or offsets by days
func formatDate(offsetDays int) string {
	return time.Now().UTC().AddDate(0, 0, offsetDays).Format("2006-01-02")
}

// conicGradient generates a CSS conic-gradient value from donut segments
func conicGradient(segments []DonutSegment) string {
	if len(segments) == 0 {
		return "var(--surface-2)"
	}
	var parts []string
	for _, s := range segments {
		parts = append(parts, fmt.Sprintf("%s %.1f%% %.1f%%", s.Color, s.Start, s.End))
	}
	return "conic-gradient(" + strings.Join(parts, ", ") + ")"
}

// sparklineSVG generates an inline SVG sparkline from data points
func sparklineSVG(points []int64) template.HTML {
	if len(points) < 2 {
		return ""
	}
	max := int64(1)
	for _, p := range points {
		if p > max {
			max = p
		}
	}
	width := 60
	height := 20
	n := len(points)
	var coords []string
	for i, p := range points {
		x := i * width / (n - 1)
		y := height - int(p*int64(height)/max)
		if y >= height {
			y = height - 1
		}
		coords = append(coords, fmt.Sprintf("%d,%d", x, y))
	}
	svg := fmt.Sprintf(
		`<svg class="sparkline" viewBox="0 0 %d %d" preserveAspectRatio="none"><polyline points="%s"/></svg>`,
		width, height, strings.Join(coords, " "),
	)
	return template.HTML(svg) // #nosec G203 -- generated from integer data only
}
