package bot

import (
	"testing"
	"time"

	"github.com/open-wander/trail/internal/parser"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name      string
		entry     *parser.LogEntry
		wantCategory string
	}{
		{
			name: "unrouted request always returns unrouted",
			entry: &parser.LogEntry{
				Router:    "",
				UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
			},
			wantCategory: CategoryUnrouted,
		},
		{
			name: "unrouted with bot UA still returns unrouted",
			entry: &parser.LogEntry{
				Router:    "",
				UserAgent: "AhrefsBot/7.0",
			},
			wantCategory: CategoryUnrouted,
		},
		{
			name: "routed bot request returns bot",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "CensysInspect bot",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Mozilla/5.0 (compatible; CensysInspect/1.1; +https://about.censys.io/)",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "facebookexternalhit",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "Go-http-client",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Go-http-client/1.1",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "curl",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "curl/7.68.0",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "empty user agent",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "-",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "suspicious short Mozilla/5.0",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Mozilla/5.0",
			},
			wantCategory: CategoryBot,
		},
		{
			name: "real Chrome browser",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
			},
			wantCategory: CategoryHuman,
		},
		{
			name: "real mobile Chrome",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
			},
			wantCategory: CategoryHuman,
		},
		{
			name: "old Opera likely scanner",
			entry: &parser.LogEntry{
				Router:    "web@docker",
				UserAgent: "Opera/8.01 (Macintosh; U; PPC Mac OS; en)",
			},
			wantCategory: CategoryHuman, // not matching bot patterns
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.entry)
			if got != tt.wantCategory {
				t.Errorf("Classify() = %v, want %v", got, tt.wantCategory)
			}
		})
	}
}

func TestClassifyUA(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		want      string
	}{
		{
			name:      "Chrome browser",
			userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
			want:      "Chrome",
		},
		{
			name:      "Firefox browser",
			userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
			want:      "Firefox",
		},
		{
			name:      "Safari browser",
			userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
			want:      "Safari",
		},
		{
			name:      "Edge browser",
			userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
			want:      "Edge",
		},
		{
			name:      "AhrefsBot",
			userAgent: "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
			want:      "ahrefsbot",
		},
		{
			name:      "Googlebot",
			userAgent: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
			want:      "googlebot",
		},
		{
			name:      "CensysInspect",
			userAgent: "Mozilla/5.0 (compatible; CensysInspect/1.1; +https://about.censys.io/)",
			want:      "censysinspect",
		},
		{
			name:      "Generic bot",
			userAgent: "Go-http-client/1.1",
			want:      "bot",
		},
		{
			name:      "Empty user agent",
			userAgent: "-",
			want:      "unknown",
		},
		{
			name:      "Unknown browser",
			userAgent: "SomethingWeird/1.0",
			want:      "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyUA(tt.userAgent)
			if got != tt.want {
				t.Errorf("ClassifyUA() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsBot(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		wantBot   bool
	}{
		{"empty", "", true},
		{"dash", "-", true},
		{"short Mozilla", "Mozilla/5.0", true},
		{"contains bot", "SomeBot/1.0", true},
		{"contains crawl", "MyCrawler/1.0", true},
		{"contains spider", "spider-tool", true},
		{"curl", "curl/7.68.0", true},
		{"wget", "Wget/1.20.3", true},
		{"real Chrome", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", false},
		{"real Firefox", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBot(tt.userAgent)
			if got != tt.wantBot {
				t.Errorf("isBot() = %v, want %v", got, tt.wantBot)
			}
		})
	}
}

// Benchmark for performance verification
func BenchmarkClassify(b *testing.B) {
	entry := &parser.LogEntry{
		IP:         "192.168.1.1",
		Timestamp:  time.Now(),
		Method:     "GET",
		Path:       "/",
		Router:     "web@docker",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
		Status:     200,
		Bytes:      1234,
		DurationMs: 10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Classify(entry)
	}
}

func BenchmarkClassifyUA(b *testing.B) {
	userAgent := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ClassifyUA(userAgent)
	}
}
