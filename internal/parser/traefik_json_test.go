package parser

import (
	"testing"
	"time"
)

// Real Traefik v3.7.7 JSON access log lines (--accesslog.format=json with
// headers kept): a routed request and an unrouted one (no RouterName,
// ServiceURL or Referer).
const (
	jsonRoutedLine   = `{"ClientAddr":"192.168.65.1:19739","ClientHost":"192.168.65.1","ClientPort":"19739","ClientUsername":"-","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":1649750,"OriginContentSize":19,"OriginDuration":1583042,"OriginStatus":404,"Overhead":66708,"RequestAddr":"example.test","RequestContentSize":0,"RequestCount":1,"RequestHost":"example.test","RequestMethod":"GET","RequestPath":"/some/path?q=1","RequestPort":"-","RequestProtocol":"HTTP/1.1","RequestScheme":"http","RetryAttempts":0,"RouterName":"echo@file","ServiceAddr":"127.0.0.1:8080","ServiceName":"echo@file","ServiceURL":"http://127.0.0.1:8080","StartLocal":"2026-10-09T21:07:16.897593508Z","StartUTC":"2026-10-09T21:07:16.897593508Z","downstream_Content-Length":"19","downstream_Content-Security-Policy":"frame-src 'self' https://traefik.io https://*.traefik.io;","downstream_Content-Type":"text/plain; charset=utf-8","downstream_Date":"Fri, 09 Oct 2026 21:07:16 GMT","downstream_X-Content-Type-Options":"nosniff","entryPointName":"web","level":"info","msg":"","origin_Content-Length":"19","origin_Content-Security-Policy":"frame-src 'self' https://traefik.io https://*.traefik.io;","origin_Content-Type":"text/plain; charset=utf-8","origin_Date":"Fri, 09 Oct 2026 21:07:16 GMT","origin_X-Content-Type-Options":"nosniff","request_Accept":"*/*","request_Referer":"https://ref.example/","request_User-Agent":"Mozilla/5.0 test","request_X-Forwarded-Host":"example.test","request_X-Forwarded-Port":"80","request_X-Forwarded-Proto":"http","request_X-Forwarded-Server":"aa50d584eb13","request_X-Real-Ip":"192.168.65.1","time":"2026-10-09T21:07:16Z"}`
	jsonUnroutedLine = `{"ClientAddr":"192.168.65.1:23014","ClientHost":"192.168.65.1","ClientPort":"23014","ClientUsername":"-","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":20916,"GzipRatio":0,"OriginContentSize":0,"OriginDuration":0,"OriginStatus":0,"Overhead":20916,"RequestAddr":"nomatch.test","RequestContentSize":0,"RequestCount":2,"RequestHost":"nomatch.test","RequestMethod":"GET","RequestPath":"/.env","RequestPort":"-","RequestProtocol":"HTTP/1.1","RequestScheme":"http","RetryAttempts":0,"StartLocal":"2026-10-09T21:07:16.918467925Z","StartUTC":"2026-10-09T21:07:16.918467925Z","downstream_Content-Type":"text/plain; charset=utf-8","downstream_X-Content-Type-Options":"nosniff","entryPointName":"web","level":"info","msg":"","request_Accept":"*/*","request_User-Agent":"curl/8.7.1","request_X-Forwarded-Host":"nomatch.test","request_X-Forwarded-Port":"80","request_X-Forwarded-Proto":"http","request_X-Forwarded-Server":"aa50d584eb13","request_X-Real-Ip":"192.168.65.1","time":"2026-10-09T21:07:16Z"}`

	// The same two requests as Traefik CLF would log them. CLF has second
	// precision, so the JSON twins below use whole-second StartUTC values.
	clfRoutedTwin    = `192.168.65.1 - - [09/Oct/2026:21:07:16 +0000] "GET /some/path?q=1 HTTP/1.1" 404 19 "https://ref.example/" "Mozilla/5.0 test" 1 "echo@file" "http://127.0.0.1:8080" 1ms`
	jsonRoutedTwin   = `{"ClientHost":"192.168.65.1","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":1649750,"RequestMethod":"GET","RequestPath":"/some/path?q=1","RequestProtocol":"HTTP/1.1","RouterName":"echo@file","ServiceURL":"http://127.0.0.1:8080","StartUTC":"2026-10-09T21:07:16Z","request_Referer":"https://ref.example/","request_User-Agent":"Mozilla/5.0 test"}`
	clfUnroutedTwin  = `192.168.65.1 - - [09/Oct/2026:21:07:16 +0000] "GET /.env HTTP/1.1" 404 19 "-" "curl/8.7.1" 2 "-" "-" 0ms`
	jsonUnroutedTwin = `{"ClientHost":"192.168.65.1","DownstreamContentSize":19,"DownstreamStatus":404,"Duration":20916,"RequestMethod":"GET","RequestPath":"/.env","RequestProtocol":"HTTP/1.1","StartUTC":"2026-10-09T21:07:16Z","request_User-Agent":"curl/8.7.1"}`
)

// assertEntry compares every LogEntry field, using Equal for the timestamp.
func assertEntry(t *testing.T, got, want *LogEntry) {
	t.Helper()
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, want.Timestamp)
	}
	g, w := *got, *want
	g.Timestamp, w.Timestamp = time.Time{}, time.Time{}
	if g != w {
		t.Errorf("entry mismatch:\n got  %+v\n want %+v", g, w)
	}
}

func TestParseTraefikJSON(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    *LogEntry
		wantErr bool
	}{
		{
			name: "routed request with headers",
			line: jsonRoutedLine,
			want: &LogEntry{
				IP:         "192.168.65.1",
				Timestamp:  time.Date(2026, 10, 9, 21, 7, 16, 897593508, time.UTC),
				Method:     "GET",
				Path:       "/some/path?q=1",
				Protocol:   "HTTP/1.1",
				Status:     404,
				Bytes:      19,
				Referer:    "https://ref.example/",
				UserAgent:  "Mozilla/5.0 test",
				Router:     "echo@file",
				Backend:    "http://127.0.0.1:8080",
				DurationMs: 1, // 1649750ns
			},
		},
		{
			name: "unrouted request has empty router, backend and referer",
			line: jsonUnroutedLine,
			want: &LogEntry{
				IP:         "192.168.65.1",
				Timestamp:  time.Date(2026, 10, 9, 21, 7, 16, 918467925, time.UTC),
				Method:     "GET",
				Path:       "/.env",
				Protocol:   "HTTP/1.1",
				Status:     404,
				Bytes:      19,
				Referer:    "",
				UserAgent:  "curl/8.7.1",
				Router:     "",
				Backend:    "",
				DurationMs: 0, // 20916ns
			},
		},
		{
			name: "duration truncates to whole milliseconds",
			line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"POST","RequestPath":"/api","RequestProtocol":"HTTP/2.0","DownstreamStatus":201,"DownstreamContentSize":7,"Duration":2999999,"RouterName":"api@docker"}`,
			want: &LogEntry{
				IP:         "10.0.0.1",
				Timestamp:  time.Date(2026, 10, 9, 21, 7, 16, 0, time.UTC),
				Method:     "POST",
				Path:       "/api",
				Protocol:   "HTTP/2.0",
				Status:     201,
				Bytes:      7,
				Router:     "api@docker",
				DurationMs: 2,
			},
		},
		{
			name: "dash placeholders map to empty like CLF",
			line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"GET","RequestPath":"/","RequestProtocol":"HTTP/1.1","DownstreamStatus":404,"RouterName":"-","ServiceURL":"-","request_Referer":"-","request_User-Agent":"-"}`,
			want: &LogEntry{
				IP:        "10.0.0.1",
				Timestamp: time.Date(2026, 10, 9, 21, 7, 16, 0, time.UTC),
				Method:    "GET",
				Path:      "/",
				Protocol:  "HTTP/1.1",
				Status:    404,
			},
		},
		{
			name: "missing optional numeric fields default to zero",
			line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"GET","RequestPath":"/"}`,
			want: &LogEntry{
				IP:        "10.0.0.1",
				Timestamp: time.Date(2026, 10, 9, 21, 7, 16, 0, time.UTC),
				Method:    "GET",
				Path:      "/",
			},
		},
		{name: "truncated JSON", line: `{"ClientHost":"10.0.0.1","StartUTC":`, wantErr: true},
		{name: "trailing garbage", line: `{"ClientHost":"10.0.0.1"} extra`, wantErr: true},
		{name: "empty object", line: `{}`, wantErr: true},
		{name: "status as string", line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"GET","RequestPath":"/","DownstreamStatus":"404"}`, wantErr: true},
		{name: "bad timestamp", line: `{"ClientHost":"10.0.0.1","StartUTC":"09/Oct/2026","RequestMethod":"GET","RequestPath":"/"}`, wantErr: true},
		{name: "missing StartUTC", line: `{"ClientHost":"10.0.0.1","RequestMethod":"GET","RequestPath":"/"}`, wantErr: true},
		{name: "missing ClientHost", line: `{"StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"GET","RequestPath":"/"}`, wantErr: true},
		{name: "missing RequestMethod", line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestPath":"/"}`, wantErr: true},
		{name: "missing RequestPath", line: `{"ClientHost":"10.0.0.1","StartUTC":"2026-10-09T21:07:16Z","RequestMethod":"GET"}`, wantErr: true},
		{name: "traefik application log line", line: `{"level":"error","msg":"something failed","time":"2026-10-09T21:07:16Z"}`, wantErr: true},
		{name: "CLF line", line: clfRoutedTwin, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTraefikJSON(tt.line)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseTraefikJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			assertEntry(t, got, tt.want)
		})
	}
}

// TestTraefikJSONMatchesCLF checks that one request logged as CLF and as JSON
// yields the same LogEntry, so dashboards do not change across the switch.
func TestTraefikJSONMatchesCLF(t *testing.T) {
	tests := []struct {
		name     string
		clf      string
		json     string
		wantRtr  string
		wantBack string
	}{
		{name: "routed", clf: clfRoutedTwin, json: jsonRoutedTwin, wantRtr: "echo@file", wantBack: "http://127.0.0.1:8080"},
		{name: "unrouted", clf: clfUnroutedTwin, json: jsonUnroutedTwin, wantRtr: "", wantBack: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromCLF, err := ParseTraefik(tt.clf)
			if err != nil {
				t.Fatalf("ParseTraefik: %v", err)
			}
			fromJSON, err := ParseTraefikJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseTraefikJSON: %v", err)
			}
			assertEntry(t, fromJSON, fromCLF)
			if fromJSON.Router != tt.wantRtr || fromJSON.Backend != tt.wantBack {
				t.Errorf("Router/Backend = %q/%q, want %q/%q", fromJSON.Router, fromJSON.Backend, tt.wantRtr, tt.wantBack)
			}
		})
	}

	// The real (nanosecond) sample lands in the same hour bucket as its CLF twin.
	real, err := ParseTraefikJSON(jsonUnroutedLine)
	if err != nil {
		t.Fatal(err)
	}
	twin, err := ParseTraefik(clfUnroutedTwin)
	if err != nil {
		t.Fatal(err)
	}
	if HourBucket(real.Timestamp) != HourBucket(twin.Timestamp) {
		t.Errorf("HourBucket = %s, CLF twin %s", HourBucket(real.Timestamp), HourBucket(twin.Timestamp))
	}
}

// TestParserMixedFormats feeds a switchover-day sequence (CLF then JSON lines,
// interleaved) through parsers in each Traefik-compatible mode.
func TestParserMixedFormats(t *testing.T) {
	sequence := []string{clfRoutedTwin, clfUnroutedTwin, jsonRoutedTwin, "  " + jsonUnroutedTwin, clfRoutedTwin, jsonUnroutedTwin}
	want := []*LogEntry{}
	for _, line := range []string{clfRoutedTwin, clfUnroutedTwin, clfRoutedTwin, clfUnroutedTwin, clfRoutedTwin, clfUnroutedTwin} {
		e, err := ParseTraefik(line)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, e)
	}

	newLockedAuto := func() *Parser {
		p := NewParser("auto")
		p.Detect([]string{clfRoutedTwin, clfUnroutedTwin})
		return p
	}

	tests := []struct {
		name       string
		parser     func() *Parser
		wantFormat Format
	}{
		{name: "auto unlocked", parser: func() *Parser { return NewParser("auto") }, wantFormat: FormatAuto},
		{name: "auto locked to traefik", parser: newLockedAuto, wantFormat: FormatTraefik},
		{name: "traefik", parser: func() *Parser { return NewParser("traefik") }, wantFormat: FormatTraefik},
		{name: "traefik-json", parser: func() *Parser { return NewParser("traefik-json") }, wantFormat: FormatTraefikJSON},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.parser()
			if p.Format() != tt.wantFormat {
				t.Fatalf("Format() = %v, want %v", p.Format(), tt.wantFormat)
			}
			for i, line := range sequence {
				got, err := p.ParseLine(line)
				if err != nil {
					t.Fatalf("line %d: ParseLine error: %v", i, err)
				}
				assertEntry(t, got, want[i])
			}
		})
	}
}

func TestParserCombinedRejectsJSON(t *testing.T) {
	p := NewParser("combined")
	if _, err := p.ParseLine(jsonRoutedLine); err == nil {
		t.Error("combined parser accepted a Traefik JSON line, want error")
	}
}

func TestParserTraefikRejectsMalformedJSON(t *testing.T) {
	// A '{' line is committed to the JSON parser and must not slip through
	// to the CLF or Combined regexes.
	for _, format := range []string{"auto", "traefik", "traefik-json"} {
		t.Run(format, func(t *testing.T) {
			if _, err := NewParser(format).ParseLine(`{"ClientHost":`); err == nil {
				t.Error("ParseLine accepted malformed JSON, want error")
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		in   string
		want Format
	}{
		{"auto", FormatAuto},
		{"traefik", FormatTraefik},
		{"traefik-json", FormatTraefikJSON},
		{"Traefik-JSON", FormatTraefikJSON},
		{"combined", FormatCombined},
		{"Combined", FormatCombined},
		{" combined ", FormatCombined},
		{"", FormatAuto},
		{"nonsense", FormatAuto},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := ParseFormat(tt.in)
			if got != tt.want {
				t.Errorf("ParseFormat(%q) = %v, want %v", tt.in, got, tt.want)
			}
			if got != FormatAuto && ParseFormat(got.String()) != got {
				t.Errorf("String() %q does not round-trip", got.String())
			}
		})
	}
}

func TestIsJSONLine(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{`{"a":1}`, true},
		{" \t{\"a\":1}", true},
		{"", false},
		{"   ", false},
		{clfRoutedTwin, false},
		{`[{"a":1}]`, false},
	}
	for _, tt := range tests {
		if got := isJSONLine(tt.line); got != tt.want {
			t.Errorf("isJSONLine(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}
