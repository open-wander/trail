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
