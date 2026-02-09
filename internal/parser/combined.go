package parser

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

// Compiled regex for Apache/Nginx Combined log format
// Format: IP - USER [TIMESTAMP] "METHOD PATH PROTOCOL" STATUS BYTES "REFERER" "UA" [optional: request_time]
var combinedRegex = regexp.MustCompile(
	`^(\S+) ` + // IP
		`\S+ ` + // ident (always -)
		`(\S+) ` + // auth user (- or username)
		`\[([^\]]+)\] ` + // timestamp
		`"(\S+) (\S+) ([^"]+)" ` + // method path protocol
		`(\d+) ` + // status
		`(\d+|-) ` + // bytes (can be - for 0)
		`"([^"]*)" ` + // referer
		`"([^"]*)"` + // user-agent
		`(?:\s+(\S+))?`, // optional: request_time in seconds (float, e.g. "0.003")
)

// ParseCombined parses a single Apache/Nginx Combined log line into a LogEntry.
// Sets Router to "server" as a synthetic default (no router concept in Combined format).
func ParseCombined(line string) (*LogEntry, error) {
	matches := combinedRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("line does not match Combined log format")
	}

	timestamp, err := time.Parse(clfTimeLayout, matches[3])
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	status, err := strconv.Atoi(matches[7])
	if err != nil {
		return nil, fmt.Errorf("failed to parse status code: %w", err)
	}

	var bytes int64
	if matches[8] != "-" {
		bytes, err = strconv.ParseInt(matches[8], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bytes: %w", err)
		}
	}

	// Parse optional request_time (float seconds -> ms)
	var durationMs int
	if matches[11] != "" {
		seconds, err := strconv.ParseFloat(matches[11], 64)
		if err == nil {
			durationMs = int(math.Round(seconds * 1000))
		}
	}

	unquote := func(s string) string {
		if s == "-" {
			return ""
		}
		return s
	}

	return &LogEntry{
		IP:         matches[1],
		Timestamp:  timestamp,
		Method:     matches[4],
		Path:       matches[5],
		Protocol:   matches[6],
		Status:     status,
		Bytes:      bytes,
		Referer:    unquote(matches[9]),
		UserAgent:  unquote(matches[10]),
		Router:     "server",
		Backend:    "",
		DurationMs: durationMs,
	}, nil
}
