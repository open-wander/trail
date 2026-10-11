package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"garbage", slog.LevelInfo},
		{"DEBUG", slog.LevelDebug},
		{"Warn", slog.LevelWarn},
		{"ERROR", slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := ParseLevel(tt.in); got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line is not a JSON object: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestNewEmitsJSONAndFiltersByLevel(t *testing.T) {
	tests := []struct {
		level    string
		wantMsgs []string
	}{
		{"info", []string{"b.info", "c.warn"}},
		{"debug", []string{"a.debug", "b.info", "c.warn"}},
		{"warn", []string{"c.warn"}},
		{"garbage", []string{"b.info", "c.warn"}},
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(&buf, tt.level)
			l.Debug("a.debug", "n", 1)
			l.Info("b.info", "n", 2)
			l.Warn("c.warn", "n", 3)

			recs := decodeLines(t, &buf)
			if len(recs) != len(tt.wantMsgs) {
				t.Fatalf("got %d records, want %d: %s", len(recs), len(tt.wantMsgs), buf.String())
			}
			for i, rec := range recs {
				for _, k := range []string{"time", "level", "msg"} {
					if _, ok := rec[k]; !ok {
						t.Errorf("record %d missing key %q: %v", i, k, rec)
					}
				}
				if rec["msg"] != tt.wantMsgs[i] {
					t.Errorf("record %d msg = %v, want %s", i, rec["msg"], tt.wantMsgs[i])
				}
			}
		})
	}
}

// TestInitWritesToStdout proves the default logger goes to os.Stdout (the
// logging standard), not stderr. It swaps global state, so no t.Parallel.
func TestInitWritesToStdout(t *testing.T) {
	origOut, origLog := os.Stdout, slog.Default()
	restore := func() {
		os.Stdout = origOut
		slog.SetDefault(origLog)
	}
	t.Cleanup(restore)

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	os.Stdout = w
	Init("info")
	slog.Info("logging.stdout_probe", "k", "v")
	restore()
	w.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	recs := decodeLines(t, bytes.NewBuffer(data))
	if len(recs) != 1 {
		t.Fatalf("got %d records on stdout, want 1: %q", len(recs), data)
	}
	if recs[0]["msg"] != "logging.stdout_probe" || recs[0]["level"] != "INFO" {
		t.Errorf("unexpected record: %v", recs[0])
	}
}
