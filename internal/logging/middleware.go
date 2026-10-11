package logging

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// RequestIDKey is both the Fiber Locals key and the log field holding the
// per-request ID.
const RequestIDKey = "request_id"

// From returns the default logger annotated with the request ID, so domain log
// lines correlate to the http.request line of the request that produced them.
// Handlers use it instead of the global slog for request-scoped logs.
func From(c *fiber.Ctx) *slog.Logger {
	if rid, ok := c.Locals(RequestIDKey).(string); ok && rid != "" {
		return slog.Default().With(slog.String(RequestIDKey, rid))
	}
	return slog.Default()
}

// isStatic reports whether path is an embedded asset. The trailing slash keeps
// a route such as /staticfoo logged.
func isStatic(path string) bool {
	return path == "/static" || strings.HasPrefix(path, "/static/")
}

// RequestLogger assigns every request a fresh ID (also sent as X-Request-ID)
// and logs one http.request line when it completes. Register it first so it
// also sees 401s from auth and errors from recovered panics.
//
// An inbound X-Request-ID is ignored, so clients cannot inject correlation
// values. client_ip is the immediate peer: behind a reverse proxy that is the
// proxy's address, not the end user's. The query string, headers and body are
// never logged.
func RequestLogger() fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := uuid.New().String()
		c.Locals(RequestIDKey, requestID)
		c.Set("X-Request-ID", requestID)

		start := time.Now()
		err := c.Next()

		if isStatic(c.Path()) {
			return err
		}

		// A returned error only becomes a response in Fiber's ErrorHandler,
		// after this middleware returns, so the status still reads 200 here.
		status := c.Response().StatusCode()
		if err != nil {
			status = fiber.StatusInternalServerError
			var fe *fiber.Error
			if errors.As(err, &fe) {
				status = fe.Code
			}
		}

		attrs := []slog.Attr{
			slog.String(RequestIDKey, requestID),
			slog.String("method", c.Method()),
			slog.String("path", c.Path()),
			slog.Int("status", status),
			slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000),
			slog.String("client_ip", c.IP()),
		}
		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
			// Fiber's default ErrorHandler does not log, so this is the only
			// record of why the request failed.
			if err != nil {
				attrs = append(attrs, slog.String("error", err.Error()))
			}
		}
		slog.LogAttrs(context.Background(), level, "http.request", attrs...)

		return err
	}
}
