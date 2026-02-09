package parser

import (
	"testing"
	"time"
)

func TestParseCombined(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    *LogEntry
		wantErr bool
	}{
		{
			name: "standard Apache combined",
			line: `192.168.1.1 - frank [10/Jan/2026:13:55:36 -0800] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/start.html" "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"`,
			want: &LogEntry{
				IP:         "192.168.1.1",
				Timestamp:  time.Date(2026, 1, 10, 13, 55, 36, 0, time.FixedZone("", -8*60*60)),
				Method:     "GET",
				Path:       "/index.html",
				Protocol:   "HTTP/1.1",
				Status:     200,
				Bytes:      2326,
				Referer:    "http://www.example.com/start.html",
				UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
				Router:     "server",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "nginx with request_time",
			line: `10.0.0.1 - - [10/Jan/2026:14:00:00 +0000] "POST /api/data HTTP/1.1" 201 512 "-" "curl/7.68.0" 0.003`,
			want: &LogEntry{
				IP:         "10.0.0.1",
				Timestamp:  time.Date(2026, 1, 10, 14, 0, 0, 0, time.UTC),
				Method:     "POST",
				Path:       "/api/data",
				Protocol:   "HTTP/1.1",
				Status:     201,
				Bytes:      512,
				Referer:    "",
				UserAgent:  "curl/7.68.0",
				Router:     "server",
				Backend:    "",
				DurationMs: 3,
			},
		},
		{
			name: "dash referer and UA",
			line: `1.2.3.4 - - [10/Jan/2026:15:00:00 +0000] "GET /robots.txt HTTP/1.0" 404 0 "-" "-"`,
			want: &LogEntry{
				IP:         "1.2.3.4",
				Timestamp:  time.Date(2026, 1, 10, 15, 0, 0, 0, time.UTC),
				Method:     "GET",
				Path:       "/robots.txt",
				Protocol:   "HTTP/1.0",
				Status:     404,
				Bytes:      0,
				Referer:    "",
				UserAgent:  "",
				Router:     "server",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "bytes as dash",
			line: `5.6.7.8 - - [10/Jan/2026:16:00:00 +0000] "HEAD /health HTTP/1.1" 304 - "-" "health-checker/1.0"`,
			want: &LogEntry{
				IP:         "5.6.7.8",
				Timestamp:  time.Date(2026, 1, 10, 16, 0, 0, 0, time.UTC),
				Method:     "HEAD",
				Path:       "/health",
				Protocol:   "HTTP/1.1",
				Status:     304,
				Bytes:      0,
				Referer:    "",
				UserAgent:  "health-checker/1.0",
				Router:     "server",
				Backend:    "",
				DurationMs: 0,
			},
		},
		{
			name: "nginx with longer request time",
			line: `10.0.0.2 - - [10/Jan/2026:17:00:00 +0000] "GET /slow HTTP/2.0" 200 8192 "https://example.com" "Mozilla/5.0" 2.456`,
			want: &LogEntry{
				IP:         "10.0.0.2",
				Timestamp:  time.Date(2026, 1, 10, 17, 0, 0, 0, time.UTC),
				Method:     "GET",
				Path:       "/slow",
				Protocol:   "HTTP/2.0",
				Status:     200,
				Bytes:      8192,
				Referer:    "https://example.com",
				UserAgent:  "Mozilla/5.0",
				Router:     "server",
				Backend:    "",
				DurationMs: 2456,
			},
		},
		{
			name:    "invalid format",
			line:    "not a valid log line at all",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCombined(tt.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCombined() error = %v, wantErr %v", err, tt.wantErr)
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
			if got.DurationMs != tt.want.DurationMs {
				t.Errorf("DurationMs = %v, want %v", got.DurationMs, tt.want.DurationMs)
			}
		})
	}
}

func TestParseCombined_RouterAlwaysServer(t *testing.T) {
	lines := []string{
		`1.2.3.4 - - [10/Jan/2026:15:00:00 +0000] "GET / HTTP/1.1" 200 100 "-" "Chrome"`,
		`5.6.7.8 - admin [10/Jan/2026:16:00:00 +0000] "POST /login HTTP/1.1" 302 0 "-" "Firefox"`,
	}

	for _, line := range lines {
		entry, err := ParseCombined(line)
		if err != nil {
			t.Fatalf("ParseCombined failed: %v", err)
		}
		if entry.Router != "server" {
			t.Errorf("Router = %q, want \"server\"", entry.Router)
		}
	}
}
