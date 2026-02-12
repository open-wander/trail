package server

import (
	"math"
	"testing"
	"time"
)

func TestPreviousPeriodFilter(t *testing.T) {
	tests := []struct {
		name       string
		filter     Filter
		rangeParam string
		wantFrom   string
		wantTo     string
	}{
		{
			name: "today shifts to yesterday",
			filter: Filter{
				From:   "2026-02-12T00:00:00Z",
				To:     "2026-02-12T14:00:00Z",
				Router: "web",
			},
			rangeParam: "today",
			wantFrom:   "2026-02-11T10:00:00Z",
			wantTo:     "2026-02-12T00:00:00Z",
		},
		{
			name: "7d shifts back 7 days",
			filter: Filter{
				From: "2026-02-05T00:00:00Z",
				To:   "2026-02-12T00:00:00Z",
			},
			rangeParam: "7d",
			wantFrom:   "2026-01-29T00:00:00Z",
			wantTo:     "2026-02-05T00:00:00Z",
		},
		{
			name: "30d shifts back 30 days",
			filter: Filter{
				From: "2026-01-13T00:00:00Z",
				To:   "2026-02-12T00:00:00Z",
			},
			rangeParam: "30d",
			wantFrom:   "2025-12-14T00:00:00Z",
			wantTo:     "2026-01-13T00:00:00Z",
		},
		{
			name: "custom range shifts by same duration",
			filter: Filter{
				From: "2026-02-01T00:00:00Z",
				To:   "2026-02-06T23:59:00Z",
			},
			rangeParam: "custom",
			wantFrom:   "2026-01-26T00:01:00Z",
			wantTo:     "2026-02-01T00:00:00Z",
		},
		{
			name: "preserves router and bots",
			filter: Filter{
				From:        "2026-02-05T00:00:00Z",
				To:          "2026-02-12T00:00:00Z",
				Router:      "api",
				IncludeBots: true,
			},
			rangeParam: "7d",
			wantFrom:   "2026-01-29T00:00:00Z",
			wantTo:     "2026-02-05T00:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := previousPeriodFilter(tt.filter, tt.rangeParam)

			if got.From != tt.wantFrom {
				t.Errorf("From = %s, want %s", got.From, tt.wantFrom)
			}
			if got.To != tt.wantTo {
				t.Errorf("To = %s, want %s", got.To, tt.wantTo)
			}
			if got.Router != tt.filter.Router {
				t.Errorf("Router = %s, want %s", got.Router, tt.filter.Router)
			}
			if got.IncludeBots != tt.filter.IncludeBots {
				t.Errorf("IncludeBots = %v, want %v", got.IncludeBots, tt.filter.IncludeBots)
			}
		})
	}
}

func TestPreviousPeriodFilterDuration(t *testing.T) {
	// Verify previous period has the same duration as current
	filter := Filter{
		From: "2026-02-05T00:00:00Z",
		To:   "2026-02-12T00:00:00Z",
	}
	prev := previousPeriodFilter(filter, "7d")

	fromOrig, _ := time.Parse(time.RFC3339, filter.From)
	toOrig, _ := time.Parse(time.RFC3339, filter.To)
	origDuration := toOrig.Sub(fromOrig)

	fromPrev, _ := time.Parse(time.RFC3339, prev.From)
	toPrev, _ := time.Parse(time.RFC3339, prev.To)
	prevDuration := toPrev.Sub(fromPrev)

	if origDuration != prevDuration {
		t.Errorf("duration mismatch: original=%v previous=%v", origDuration, prevDuration)
	}
}

func TestPreviousPeriodFilterInvalidDates(t *testing.T) {
	filter := Filter{
		From: "invalid-date",
		To:   "also-invalid",
	}
	got := previousPeriodFilter(filter, "today")
	if got.From != filter.From || got.To != filter.To {
		t.Errorf("expected unchanged filter for invalid dates")
	}
}

func TestPctChange(t *testing.T) {
	tests := []struct {
		name     string
		newVal   int64
		oldVal   int64
		expected float64
	}{
		{"equal values", 100, 100, 0.0},
		{"double", 200, 100, 100.0},
		{"half", 50, 100, -50.0},
		{"zero previous positive new", 100, 0, 100.0},
		{"zero previous zero new", 0, 0, 0.0},
		{"zero new nonzero previous", 0, 100, -100.0},
		{"small increase", 105, 100, 5.0},
		{"small decrease", 95, 100, -5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pctChange(tt.newVal, tt.oldVal)
			if math.Abs(got-tt.expected) > 0.01 {
				t.Errorf("pctChange(%d, %d) = %.2f, want %.2f", tt.newVal, tt.oldVal, got, tt.expected)
			}
		})
	}
}

func TestComputeComparison(t *testing.T) {
	t.Run("both periods have data", func(t *testing.T) {
		current := &TotalStat{Requests: 200, Visitors: 50, Bytes: 1024, AvgMs: 150}
		previous := &TotalStat{Requests: 100, Visitors: 40, Bytes: 512, AvgMs: 100}
		c := computeComparison(current, previous)

		if math.Abs(c.RequestsDelta-100.0) > 0.01 {
			t.Errorf("RequestsDelta = %.2f, want 100.0", c.RequestsDelta)
		}
		if math.Abs(c.VisitorsDelta-25.0) > 0.01 {
			t.Errorf("VisitorsDelta = %.2f, want 25.0", c.VisitorsDelta)
		}
		if math.Abs(c.BytesDelta-100.0) > 0.01 {
			t.Errorf("BytesDelta = %.2f, want 100.0", c.BytesDelta)
		}
		if math.Abs(c.AvgMsDelta-50.0) > 0.01 {
			t.Errorf("AvgMsDelta = %.2f, want 50.0", c.AvgMsDelta)
		}
	})

	t.Run("nil previous returns zero deltas", func(t *testing.T) {
		current := &TotalStat{Requests: 200}
		c := computeComparison(current, nil)
		if c.RequestsDelta != 0 {
			t.Errorf("expected zero delta with nil previous, got %.2f", c.RequestsDelta)
		}
	})

	t.Run("nil current returns zero deltas", func(t *testing.T) {
		previous := &TotalStat{Requests: 100}
		c := computeComparison(nil, previous)
		if c.RequestsDelta != 0 {
			t.Errorf("expected zero delta with nil current, got %.2f", c.RequestsDelta)
		}
	})
}

func TestFormatDelta(t *testing.T) {
	tests := []struct {
		name     string
		delta    float64
		expected string
	}{
		{"positive", 12.5, "+12.5%"},
		{"negative", -3.2, "-3.2%"},
		{"zero", 0.0, "0.0%"},
		{"large positive", 150.0, "+150.0%"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDelta(tt.delta)
			if got != tt.expected {
				t.Errorf("formatDelta(%.1f) = %s, want %s", tt.delta, got, tt.expected)
			}
		})
	}
}

func TestDeltaClass(t *testing.T) {
	tests := []struct {
		name     string
		delta    float64
		expected string
	}{
		{"up", 5.0, "delta-up"},
		{"down", -5.0, "delta-down"},
		{"neutral zero", 0.0, "delta-neutral"},
		{"neutral small positive", 0.3, "delta-neutral"},
		{"neutral small negative", -0.3, "delta-neutral"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deltaClass(tt.delta)
			if got != tt.expected {
				t.Errorf("deltaClass(%.1f) = %s, want %s", tt.delta, got, tt.expected)
			}
		})
	}
}

func TestDeltaArrow(t *testing.T) {
	tests := []struct {
		name     string
		delta    float64
		expected string
	}{
		{"up", 5.0, "\u2191"},
		{"down", -5.0, "\u2193"},
		{"neutral", 0.0, "\u2192"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deltaArrow(tt.delta)
			if got != tt.expected {
				t.Errorf("deltaArrow(%.1f) = %s, want %s", tt.delta, got, tt.expected)
			}
		})
	}
}
