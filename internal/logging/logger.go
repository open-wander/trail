// Package logging configures Trail's own structured logs: one JSON object
// per line on stdout, as the TydeWhatMay logging standard requires.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Init installs the JSON logger on stdout as the slog default. Call it first
// in main so even configuration errors are emitted as JSON.
func Init(level string) {
	slog.SetDefault(New(os.Stdout, level))
}

// New returns a JSON logger writing to w at the given level.
func New(w io.Writer, level string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: ParseLevel(level)}))
}

// ParseLevel maps debug, warn and error (case-insensitive) to their levels;
// anything else, including empty, is info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
