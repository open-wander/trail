package parser

import (
	"testing"
	"time"
)

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    *LogEntry
		wantErr bool
	}{
		{
			name: "authenticated user with full fields",
			line: `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36" 1 "goaccess@docker" "http://172.19.0.4:80" 1ms`,
			want: &LogEntry{
				IP:         "91.34.143.167",
				Timestamp:  time.Date(2026, 1, 7, 16, 17, 8, 0, time.UTC),
				Method:     "GET",
				Path:       "/ws",
				Protocol:   "HTTP/1.1",
				Status:     404,
				Bytes:      555,
				Referer:    "",
				UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
				Router:     "goaccess@docker",
				Backend:    "http://172.19.0.4:80",
				DurationMs: 1,
			},
		},
		{
			name: "no router or backend",
			line: `37.186.248.50 - - [07/Jan/2026:16:26:58 +0000] "GET / HTTP/1.0" 404 19 "-" "-" 12 "-" "-" 0ms`,
			want: &LogEntry{
				IP:         "37.186.248.50",
				Timestamp:  time.Date(2026, 1, 7, 16, 26, 58, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				Protocol:   "HTTP/1.0",
				Status:     404,
				Bytes:      19,
				Referer:    "",
				UserAgent:  "",
				Router:     "",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "with user agent but no router",
			line: `78.153.140.179 - - [07/Jan/2026:16:28:56 +0000] "GET /.env HTTP/1.1" 404 19 "-" "Opera/8.01 (Macintosh; U; PPC Mac OS; en)" 13 "-" "-" 0ms`,
			want: &LogEntry{
				IP:         "78.153.140.179",
				Timestamp:  time.Date(2026, 1, 7, 16, 28, 56, 0, time.UTC),
				Method:     "GET",
				Path:       "/.env",
				Protocol:   "HTTP/1.1",
				Status:     404,
				Bytes:      19,
				Referer:    "",
				UserAgent:  "Opera/8.01 (Macintosh; U; PPC Mac OS; en)",
				Router:     "",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "HTTP/2.0 with redirect router",
			line: `173.252.70.28 - - [07/Jan/2026:16:46:34 +0000] "GET /robots.txt HTTP/2.0" 301 17 "-" "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)" 23 "rmbl-redirect@docker" "-" 0ms`,
			want: &LogEntry{
				IP:         "173.252.70.28",
				Timestamp:  time.Date(2026, 1, 7, 16, 46, 34, 0, time.UTC),
				Method:     "GET",
				Path:       "/robots.txt",
				Protocol:   "HTTP/2.0",
				Status:     301,
				Bytes:      17,
				Referer:    "",
				UserAgent:  "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
				Router:     "rmbl-redirect@docker",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "with referer URL",
			line: `91.34.143.167 - - [07/Jan/2026:16:17:16 +0000] "GET / HTTP/2.0" 200 24352 "https://ramble.openwander.org/jobs" "Mozilla/5.0 (Macintosh; ...)" 2 "ramble@docker" "http://172.19.0.2:3000" 7ms`,
			want: &LogEntry{
				IP:         "91.34.143.167",
				Timestamp:  time.Date(2026, 1, 7, 16, 17, 16, 0, time.UTC),
				Method:     "GET",
				Path:       "/",
				Protocol:   "HTTP/2.0",
				Status:     200,
				Bytes:      24352,
				Referer:    "https://ramble.openwander.org/jobs",
				UserAgent:  "Mozilla/5.0 (Macintosh; ...)",
				Router:     "ramble@docker",
				Backend:    "http://172.19.0.2:3000",
				DurationMs: 7,
			},
		},
		{
			name:    "invalid format",
			line:    "not a valid log line",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLine(tt.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.IP != tt.want.IP {
				t.Errorf("IP = %v, want %v", got.IP, tt.want.IP)
			}
			if !got.Timestamp.Equal(tt.want.Timestamp) {
				t.Errorf("Timestamp = %v, want %v", got.Timestamp, tt.want.Timestamp)
			}
			if got.Method != tt.want.Method {
				t.Errorf("Method = %v, want %v", got.Method, tt.want.Method)
			}
			if got.Path != tt.want.Path {
				t.Errorf("Path = %v, want %v", got.Path, tt.want.Path)
			}
			if got.Protocol != tt.want.Protocol {
				t.Errorf("Protocol = %v, want %v", got.Protocol, tt.want.Protocol)
			}
			if got.Status != tt.want.Status {
				t.Errorf("Status = %v, want %v", got.Status, tt.want.Status)
			}
			if got.Bytes != tt.want.Bytes {
				t.Errorf("Bytes = %v, want %v", got.Bytes, tt.want.Bytes)
			}
			if got.Referer != tt.want.Referer {
				t.Errorf("Referer = %v, want %v", got.Referer, tt.want.Referer)
			}
			if got.UserAgent != tt.want.UserAgent {
				t.Errorf("UserAgent = %v, want %v", got.UserAgent, tt.want.UserAgent)
			}
			if got.Router != tt.want.Router {
				t.Errorf("Router = %v, want %v", got.Router, tt.want.Router)
			}
			if got.Backend != tt.want.Backend {
				t.Errorf("Backend = %v, want %v", got.Backend, tt.want.Backend)
			}
			if got.DurationMs != tt.want.DurationMs {
				t.Errorf("DurationMs = %v, want %v", got.DurationMs, tt.want.DurationMs)
			}
		})
	}
}

func TestHourBucket(t *testing.T) {
	tests := []struct {
		name string
		time time.Time
		want string
	}{
		{
			name: "truncate to hour",
			time: time.Date(2026, 1, 7, 16, 17, 16, 0, time.UTC),
			want: "2026-01-07T16:00:00Z",
		},
		{
			name: "already at hour boundary",
			time: time.Date(2026, 1, 7, 16, 0, 0, 0, time.UTC),
			want: "2026-01-07T16:00:00Z",
		},
		{
			name: "non-UTC timezone gets converted",
			time: time.Date(2026, 1, 7, 16, 30, 0, 0, time.FixedZone("EST", -5*60*60)),
			want: "2026-01-07T21:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HourBucket(tt.time)
			if got != tt.want {
				t.Errorf("HourBucket() = %v, want %v", got, tt.want)
			}
		})
	}
}
