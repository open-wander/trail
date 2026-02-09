package bot

import (
	"strings"

	"github.com/open-wander/trail/internal/parser"
)

// Bot detection
// Identifies and filters bot traffic from analytics

// Category constants for request classification
const (
	CategoryHuman    = "human"
	CategoryBot      = "bot"
	CategoryUnrouted = "unrouted"
)

// Known bot signatures to check in User-Agent strings
var botSignatures = []string{
	"bot", "crawl", "spider", "slurp",
	"googlebot", "bingbot", "ahrefsbot", "censysinspect",
	"cms-checker", "facebookexternalhit", "go-http-client",
	"curl", "wget", "python-requests", "scrapy",
	"headlesschrome", "phantomjs", "selenium",
	"bot/", "+http",
}

// Known bot names for display categorization
var knownBots = []string{
	"ahrefsbot", "googlebot", "bingbot", "yandexbot",
	"baiduspider", "duckduckbot", "slurp", "facebookexternalhit",
	"twitterbot", "linkedinbot", "censysinspect", "cms-checker",
}

// Classify determines the category of a log entry based on router and User-Agent.
// Returns one of: CategoryHuman, CategoryBot, or CategoryUnrouted.
func Classify(entry *parser.LogEntry) string {
	// Unrouted requests (no router matched) are always categorized as unrouted
	if entry.Router == "" {
		return CategoryUnrouted
	}

	// Check User-Agent for bot patterns
	if isBot(entry.UserAgent) {
		return CategoryBot
	}

	return CategoryHuman
}

// isBot checks if a User-Agent string matches known bot patterns
func isBot(userAgent string) bool {
	ua := strings.ToLower(userAgent)

	// Empty or "-" User-Agent is suspicious
	if ua == "" || ua == "-" {
		return true
	}

	// Exactly "Mozilla/5.0" without more detail is suspicious
	if strings.TrimSpace(userAgent) == "Mozilla/5.0" {
		return true
	}

	// Check against known bot signatures
	for _, sig := range botSignatures {
		if strings.Contains(ua, sig) {
			return true
		}
	}

	return false
}

// ClassifyBrowser returns a high-level browser name from the User-Agent string.
// Returns one of: "Chrome", "Firefox", "Safari", "Edge", "Opera", "Bot", "Unknown", or "Other".
// Order matters: Edge before Chrome (Edge UA contains "chrome/"), Chrome before Safari.
func ClassifyBrowser(ua string) string {
	lower := strings.ToLower(ua)

	if lower == "" || lower == "-" {
		return "Unknown"
	}

	if isBot(ua) {
		return "Bot"
	}

	// Edge contains both "edg/" and "chrome/"
	if strings.Contains(lower, "edg/") {
		return "Edge"
	}
	// Opera (modern uses OPR/)
	if strings.Contains(lower, "opr/") || strings.Contains(lower, "opera/") {
		return "Opera"
	}
	// Chrome (must come before Safari since Chrome UA contains "safari/")
	if strings.Contains(lower, "chrome/") || strings.Contains(lower, "chromium/") {
		return "Chrome"
	}
	// Firefox
	if strings.Contains(lower, "firefox/") {
		return "Firefox"
	}
	// Safari (only if no chrome - already filtered above)
	if strings.Contains(lower, "safari/") {
		return "Safari"
	}

	return "Other"
}

// ClassifyOS returns the operating system from the User-Agent string.
// Returns one of: "Windows", "macOS", "Linux", "iOS", "Android", "ChromeOS", or "Other".
// Check mobile OS before desktop (iPhone before macOS).
func ClassifyOS(ua string) string {
	lower := strings.ToLower(ua)

	if lower == "" || lower == "-" {
		return "Other"
	}

	// Mobile OS first
	if strings.Contains(lower, "iphone") || strings.Contains(lower, "ipad") || strings.Contains(lower, "ipod") {
		return "iOS"
	}
	if strings.Contains(lower, "android") {
		return "Android"
	}

	// ChromeOS before Linux (CrOS UA also contains "linux")
	if strings.Contains(lower, "cros") {
		return "ChromeOS"
	}

	// Desktop OS
	if strings.Contains(lower, "windows") {
		return "Windows"
	}
	if strings.Contains(lower, "macintosh") || strings.Contains(lower, "mac os") {
		return "macOS"
	}
	if strings.Contains(lower, "linux") {
		return "Linux"
	}

	return "Other"
}

// ClassifyUA returns a display category for the User-Agent.
// Used for the user_agents table to group by browser/bot type.
func ClassifyUA(userAgent string) string {
	ua := strings.ToLower(userAgent)

	// Empty or unknown
	if ua == "" || ua == "-" {
		return "unknown"
	}

	// Check for known bot names first
	for _, botName := range knownBots {
		if strings.Contains(ua, botName) {
			return botName
		}
	}

	// Generic bot detection
	if isBot(userAgent) {
		return "bot"
	}

	// Browser detection (order matters)
	if strings.Contains(ua, "edg/") {
		return "Edge"
	}
	if strings.Contains(ua, "chrome/") && !strings.Contains(ua, "chromium") {
		return "Chrome"
	}
	if strings.Contains(ua, "firefox/") {
		return "Firefox"
	}
	if strings.Contains(ua, "safari/") && !strings.Contains(ua, "chrome/") {
		return "Safari"
	}

	return "unknown"
}
