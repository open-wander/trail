package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// traefikJSONLine holds the fields Trail uses from a Traefik JSON access log
// line (--accesslog.format=json). Header fields are only present when kept via
// --accesslog.fields.headers.names.<Header>=keep.
type traefikJSONLine struct {
	ClientHost            string    `json:"ClientHost"`
	StartUTC              time.Time `json:"StartUTC"`
	RequestMethod         string    `json:"RequestMethod"`
	RequestPath           string    `json:"RequestPath"`
	RequestProtocol       string    `json:"RequestProtocol"`
	DownstreamStatus      int       `json:"DownstreamStatus"`
	DownstreamContentSize int64     `json:"DownstreamContentSize"`
	RouterName            string    `json:"RouterName"`
	ServiceURL            string    `json:"ServiceURL"`
	Duration              int64     `json:"Duration"` // nanoseconds
	Referer               string    `json:"request_Referer"`
	UserAgent             string    `json:"request_User-Agent"`
}

// errNotTraefikJSON is returned for valid JSON that lacks the core request fields.
var errNotTraefikJSON = errors.New("JSON line is not a Traefik access log entry")

// ParseTraefikJSON parses a single Traefik JSON access log line into a LogEntry.
// Absent or "-" router, service URL and headers map to "", exactly as the CLF
// parser maps Traefik's "-" placeholders, so both formats aggregate identically.
// Duration is truncated to whole milliseconds, as Traefik does in CLF output.
func ParseTraefikJSON(line string) (*LogEntry, error) {
	var raw traefikJSONLine
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse Traefik JSON line: %w", err)
	}

	if raw.ClientHost == "" || raw.StartUTC.IsZero() || raw.RequestMethod == "" || raw.RequestPath == "" {
		return nil, errNotTraefikJSON
	}

	return &LogEntry{
		IP:         raw.ClientHost,
		Timestamp:  raw.StartUTC,
		Method:     raw.RequestMethod,
		Path:       raw.RequestPath,
		Protocol:   raw.RequestProtocol,
		Status:     raw.DownstreamStatus,
		Bytes:      raw.DownstreamContentSize,
		Referer:    dashToEmpty(raw.Referer),
		UserAgent:  dashToEmpty(raw.UserAgent),
		Router:     dashToEmpty(raw.RouterName),
		Backend:    dashToEmpty(raw.ServiceURL),
		DurationMs: int(raw.Duration / int64(time.Millisecond)),
	}, nil
}
