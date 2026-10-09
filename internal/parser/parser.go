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
	FormatAuto        Format = iota
	FormatTraefik            // Traefik extended CLF
	FormatCombined           // Apache/Nginx Combined
	FormatTraefikJSON        // Traefik JSON (--accesslog.format=json)
)

// String returns the configuration name of the format, as accepted by NewParser.
func (f Format) String() string {
	switch f {
	case FormatTraefik:
		return "traefik"
	case FormatCombined:
		return "combined"
	case FormatTraefikJSON:
		return "traefik-json"
	default:
		return "auto"
	}
}

// ParseFormat maps a configuration name ("auto", "traefik", "traefik-json",
// "combined", case-insensitive) to a Format. Unknown names map to FormatAuto.
func ParseFormat(name string) Format {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "traefik":
		return FormatTraefik
	case "traefik-json":
		return FormatTraefikJSON
	case "combined":
		return FormatCombined
	default:
		return FormatAuto
	}
}

// Parser wraps format-aware line parsing
type Parser struct {
	format Format
}

// NewParser creates a Parser for the given format string.
// Valid values: "auto", "traefik", "traefik-json", "combined".
func NewParser(format string) *Parser {
	return &Parser{format: ParseFormat(format)}
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
//
// Traefik's two formats are treated as one family so that a file switched
// from CLF to JSON mid-stream (and its CLF rotated predecessors) parses
// cleanly: unless the format is Combined, a line starting with '{' is parsed
// as Traefik JSON and any other line as Traefik CLF.
// For FormatAuto, non-JSON lines try Traefik first (more specific), then Combined.
func (p *Parser) ParseLine(line string) (*LogEntry, error) {
	if p.format != FormatCombined && isJSONLine(line) {
		return ParseTraefikJSON(line)
	}

	switch p.format {
	case FormatTraefik, FormatTraefikJSON:
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

// isJSONLine reports whether the first non-whitespace byte of line is '{'.
func isJSONLine(line string) bool {
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

// dashToEmpty maps the "-" placeholder used by log formats for absent values to "".
func dashToEmpty(s string) string {
	if s == "-" {
		return ""
	}
	return s
}

// HourBucket truncates a time to the hour and returns it as UTC ISO format
func HourBucket(t time.Time) string {
	return t.UTC().Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
}
