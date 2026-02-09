package parser

import "testing"

func TestDetectFormat(t *testing.T) {
	traefikLine := `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36" 1 "goaccess@docker" "http://172.19.0.4:80" 1ms`
	combinedLine := `192.168.1.1 - frank [10/Jan/2026:13:55:36 -0800] "GET /index.html HTTP/1.1" 200 2326 "http://www.example.com/start.html" "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"`
	combinedNginx := `10.0.0.1 - - [10/Jan/2026:14:00:00 +0000] "POST /api/data HTTP/1.1" 201 512 "-" "curl/7.68.0" 0.003`

	tests := []struct {
		name string
		lines []string
		want  Format
	}{
		{
			name:  "pure traefik",
			lines: []string{traefikLine, traefikLine, traefikLine},
			want:  FormatTraefik,
		},
		{
			name:  "pure combined",
			lines: []string{combinedLine, combinedLine, combinedNginx},
			want:  FormatCombined,
		},
		{
			name:  "mixed - traefik majority",
			lines: []string{traefikLine, traefikLine, combinedLine},
			want:  FormatTraefik,
		},
		{
			name:  "mixed - combined majority",
			lines: []string{combinedLine, combinedNginx, traefikLine},
			want:  FormatCombined,
		},
		{
			name:  "tie goes to traefik",
			lines: []string{traefikLine, combinedLine},
			want:  FormatTraefik,
		},
		{
			name:  "empty input defaults to traefik",
			lines: []string{},
			want:  FormatTraefik,
		},
		{
			name:  "all empty lines defaults to traefik",
			lines: []string{"", "", ""},
			want:  FormatTraefik,
		},
		{
			name:  "unrecognized lines default to traefik",
			lines: []string{"not a log line", "another random string"},
			want:  FormatTraefik,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectFormat(tt.lines)
			if got != tt.want {
				t.Errorf("DetectFormat() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParserDetect(t *testing.T) {
	combinedLine := `192.168.1.1 - - [10/Jan/2026:13:55:36 +0000] "GET / HTTP/1.1" 200 100 "-" "Chrome"`

	p := NewParser("auto")
	if p.Format() != FormatAuto {
		t.Fatalf("initial format = %d, want FormatAuto", p.Format())
	}

	p.Detect([]string{combinedLine, combinedLine})
	if p.Format() != FormatCombined {
		t.Errorf("after detect format = %d, want FormatCombined", p.Format())
	}

	// Subsequent Detect calls should not change locked format
	traefikLine := `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla" 1 "web@docker" "http://172.19.0.4:80" 1ms`
	p.Detect([]string{traefikLine, traefikLine, traefikLine})
	if p.Format() != FormatCombined {
		t.Errorf("format should stay locked as Combined, got %d", p.Format())
	}
}

func TestParserParseLine(t *testing.T) {
	traefikLine := `91.34.143.167 - admin [07/Jan/2026:16:17:08 +0000] "GET /ws HTTP/1.1" 404 555 "-" "Mozilla/5.0" 1 "web@docker" "http://172.19.0.4:80" 1ms`
	combinedLine := `192.168.1.1 - - [10/Jan/2026:13:55:36 +0000] "GET / HTTP/1.1" 200 100 "-" "Chrome"`

	// Traefik parser
	tp := NewParser("traefik")
	entry, err := tp.ParseLine(traefikLine)
	if err != nil {
		t.Fatalf("traefik parser failed: %v", err)
	}
	if entry.Router != "web@docker" {
		t.Errorf("traefik Router = %q, want web@docker", entry.Router)
	}

	// Combined parser
	cp := NewParser("combined")
	entry, err = cp.ParseLine(combinedLine)
	if err != nil {
		t.Fatalf("combined parser failed: %v", err)
	}
	if entry.Router != "server" {
		t.Errorf("combined Router = %q, want server", entry.Router)
	}

	// Auto parser should handle both
	ap := NewParser("auto")
	entry, err = ap.ParseLine(traefikLine)
	if err != nil {
		t.Fatalf("auto parser failed on traefik: %v", err)
	}
	if entry.Router != "web@docker" {
		t.Errorf("auto Router = %q for traefik line, want web@docker", entry.Router)
	}

	entry, err = ap.ParseLine(combinedLine)
	if err != nil {
		t.Fatalf("auto parser failed on combined: %v", err)
	}
	// Combined line also matches the combined regex in auto mode
	if entry.Router == "" {
		t.Error("auto parser should set Router for combined line")
	}
}
