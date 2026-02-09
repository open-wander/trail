package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Compiled regex for Traefik extended CLF format
// Format: IP - USER [TIMESTAMP] "METHOD PATH PROTOCOL" STATUS BYTES "REFERER" "UA" REQ# "ROUTER" "BACKEND" DURATIONms
var traefikRegex = regexp.MustCompile(
	`^(\S+) ` + // IP
		`\S+ ` + // ident (always -)
		`(\S+) ` + // auth user (- or username)
		`\[([^\]]+)\] ` + // timestamp
		`"(\S+) (\S+) ([^"]+)" ` + // method path protocol
		`(\d+) ` + // status
		`(\d+) ` + // bytes
		`"([^"]*)" ` + // referer
		`"([^"]*)" ` + // user-agent
		`\d+ ` + // request number (ignored)
		`"([^"]*)" ` + // router
		`"([^"]*)" ` + // backend
		`(\d+)ms`, // duration
)

// ParseTraefik parses a single Traefik access log line into a LogEntry
func ParseTraefik(line string) (*LogEntry, error) {
	matches := traefikRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("line does not match Traefik CLF format")
	}

	timestamp, err := time.Parse(clfTimeLayout, matches[3])
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	status, err := strconv.Atoi(matches[7])
	if err != nil {
		return nil, fmt.Errorf("failed to parse status code: %w", err)
	}

	bytes, err := strconv.ParseInt(matches[8], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bytes: %w", err)
	}

	durationMs, err := strconv.Atoi(matches[13])
	if err != nil {
		return nil, fmt.Errorf("failed to parse duration: %w", err)
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
		Router:     unquote(matches[11]),
		Backend:    unquote(matches[12]),
		DurationMs: durationMs,
	}, nil
}
