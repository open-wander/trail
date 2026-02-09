package parser

import (
	"fmt"
	"strings"
	"time"
)

// LogEntry represents a parsed access log line
type LogEntry struct {
	IP         string
	Timestamp  time.Time
	Method     string
	Path       string
	Protocol   string
	Status     int
	Bytes      int64
	Referer    string
	UserAgent  string
	Router     string
	Backend    string
	DurationMs int
}

// CLF timestamp layout: [07/Jan/2026:16:17:16 +0000]
const clfTimeLayout = "02/Jan/2006:15:04:05 -0700"

// Format represents a log file format
type Format int

const (
	FormatAuto     Format = iota
	FormatTraefik         // Traefik extended CLF
	FormatCombined        // Apache/Nginx Combined
)

// Parser wraps format-aware line parsing
type Parser struct {
	format Format
}

// NewParser creates a Parser for the given format string.
// Valid values: "auto", "traefik", "combined".
func NewParser(format string) *Parser {
	switch strings.ToLower(format) {
	case "traefik":
		return &Parser{format: FormatTraefik}
	case "combined":
		return &Parser{format: FormatCombined}
	default:
		return &Parser{format: FormatAuto}
	}
}

// Format returns the current parser format
func (p *Parser) Format() Format {
	return p.format
}

// Detect examines sample lines to determine the log format.
// Only meaningful when format is FormatAuto; locks format for future calls.
func (p *Parser) Detect(lines []string) Format {
	if p.format != FormatAuto {
		return p.format
	}

	detected := DetectFormat(lines)
	p.format = detected
	return detected
}

// ParseLine parses a single log line using the configured format.
// For FormatAuto, tries Traefik first (more specific), then Combined.
func (p *Parser) ParseLine(line string) (*LogEntry, error) {
	switch p.format {
	case FormatTraefik:
		return ParseTraefik(line)
	case FormatCombined:
		return ParseCombined(line)
	default:
		// Auto: try Traefik first (more specific regex), fall back to Combined
		if entry, err := ParseTraefik(line); err == nil {
			return entry, nil
		}
		if entry, err := ParseCombined(line); err == nil {
			return entry, nil
		}
		return nil, fmt.Errorf("line does not match any known log format")
	}
}

// ParseLine is a backward-compatible standalone function that calls ParseTraefik.
func ParseLine(line string) (*LogEntry, error) {
	return ParseTraefik(line)
}

// HourBucket truncates a time to the hour and returns it as UTC ISO format
func HourBucket(t time.Time) string {
	return t.UTC().Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
}
