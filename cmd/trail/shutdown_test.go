package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-wander/trail/internal/aggregator"
	"github.com/open-wander/trail/internal/db"
	_ "modernc.org/sqlite"
)

const curlLine = `91.34.143.167 - - [07/Jan/2026:16:17:08 +0000] "GET / HTTP/1.1" 200 5 "-" "curl/8.7.1" 1 "web@docker" "http://172.19.0.4:80" 1ms`

type producerFunc func(context.Context, chan<- string) error

func (f producerFunc) Run(ctx context.Context, ch chan<- string) error { return f(ctx, ch) }

type consumerFunc func(context.Context, <-chan string) error

func (f consumerFunc) Run(ctx context.Context, ch <-chan string) error { return f(ctx, ch) }

// eventLog is a mutex-guarded ordered record of what the fakes did.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *eventLog) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, s)
}

func (e *eventLog) index(s string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, v := range e.events {
		if v == s {
			return i
		}
	}
	return -1
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs swaps the default logger for the test. Tests using it must not
// run in parallel.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// drainConsumer consumes until the channel closes or ctx is cancelled, then
// records "agg.flushed" and which of the two ended it.
func drainConsumer(ev *eventLog) consumerFunc {
	return func(ctx context.Context, ch <-chan string) error {
		defer ev.add("agg.flushed")
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					ev.add("agg.closed")
					return nil
				}
			case <-ctx.Done():
				ev.add("agg.cancelled")
				return ctx.Err()
			}
		}
	}
}

func TestPipelineStopOrdering(t *testing.T) {
	captureLogs(t)
	ev := &eventLog{}
	producer := producerFunc(func(ctx context.Context, _ chan<- string) error {
		<-ctx.Done()
		time.Sleep(30 * time.Millisecond) // a stop-order bug would flush the aggregator in this window
		ev.add("tail.returned")
		return ctx.Err()
	})

	pl := startPipeline(context.Background(), producer, drainConsumer(ev), make(chan string, 10))
	pl.stop(time.Second, time.Second)
	ev.add("stop.returned") // stands in for the deferred database Close

	tail, flushed, stopped := ev.index("tail.returned"), ev.index("agg.flushed"), ev.index("stop.returned")
	if tail < 0 || flushed < 0 || stopped < 0 {
		t.Fatalf("missing events: %v", ev.events)
	}
	if !(tail < flushed && flushed < stopped) {
		t.Errorf("want tail.returned < agg.flushed < stop.returned, got %v", ev.events)
	}
	if ev.index("agg.closed") < 0 || ev.index("agg.cancelled") >= 0 {
		t.Errorf("normal path must leave through channel close, not cancel: %v", ev.events)
	}
}

func TestPipelineStopLosesNoLines(t *testing.T) {
	const n = 1500 // above the aggregator buffer threshold
	captureLogs(t)
	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })

	sent := make(chan struct{})
	producer := producerFunc(func(ctx context.Context, ch chan<- string) error {
		for range n {
			ch <- curlLine
		}
		close(sent)
		<-ctx.Done()
		return ctx.Err()
	})

	pl := startPipeline(context.Background(), producer, aggregator.New(database, nil, ""), make(chan string, 10000))
	<-sent
	pl.stop(time.Second, 10*time.Second)

	var total int
	if err := database.QueryRow(`SELECT COALESCE(SUM(count), 0) FROM requests`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Errorf("want %d requests after shutdown, got %d", n, total)
	}
}

func TestPipelineTailerTimeout(t *testing.T) {
	logs := captureLogs(t)
	ev := &eventLog{}
	release := make(chan struct{})
	returned := make(chan struct{})
	producer := producerFunc(func(ctx context.Context, ch chan<- string) error {
		defer close(returned)
		<-release // ignores ctx, like a tailer stuck in file or DB I/O
		ch <- curlLine
		return nil
	})

	pl := startPipeline(context.Background(), producer, drainConsumer(ev), make(chan string, 10))
	pl.stop(10*time.Millisecond, time.Second)

	if !strings.Contains(logs.String(), "tailer.stop_timeout") {
		t.Errorf("missing tailer.stop_timeout in logs: %s", logs.String())
	}
	if ev.index("agg.cancelled") < 0 {
		t.Errorf("aggregator must be cancelled on tailer timeout: %v", ev.events)
	}

	// Late send after the aggregator has gone: must not panic. The line is
	// not processed; this is the documented residual of the timeout path.
	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("late producer did not return")
	}
}

func TestPipelineAggregatorTimeout(t *testing.T) {
	logs := captureLogs(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	consumer := consumerFunc(func(context.Context, <-chan string) error {
		<-release // ignores ctx and channel close
		return nil
	})
	producer := producerFunc(func(ctx context.Context, _ chan<- string) error {
		<-ctx.Done()
		return ctx.Err()
	})

	pl := startPipeline(context.Background(), producer, consumer, make(chan string, 10))
	start := time.Now()
	pl.stop(time.Second, 10*time.Millisecond)

	if !strings.Contains(logs.String(), "aggregator.flush_timeout") {
		t.Errorf("missing aggregator.flush_timeout in logs: %s", logs.String())
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("stop did not return promptly after the aggregator timeout")
	}
}
