package main

import (
	"context"
	"log/slog"
	"time"
)

// lineProducer and lineConsumer are what *tailer.Tailer and
// *aggregator.Aggregator already implement; they let tests substitute fakes.
type lineProducer interface {
	Run(ctx context.Context, lines chan<- string) error
}

type lineConsumer interface {
	Run(ctx context.Context, lines <-chan string) error
}

// pipeline owns the tailer -> aggregator goroutines and their shutdown order.
type pipeline struct {
	tailCancel, aggCancel context.CancelFunc
	tailDone, aggDone     chan struct{}
}

// startPipeline starts both goroutines. The tailer is the only sender, so its
// goroutine closes lines when it returns: the aggregator then processes every
// queued line through its channel-close path with a live context. The
// aggregator context is deliberately not derived from parent; it is cancelled
// only by stop on a timeout.
func startPipeline(parent context.Context, tail lineProducer, agg lineConsumer, lines chan string) *pipeline {
	tailCtx, tailCancel := context.WithCancel(parent)
	aggCtx, aggCancel := context.WithCancel(context.Background())
	p := &pipeline{
		tailCancel: tailCancel,
		aggCancel:  aggCancel,
		tailDone:   make(chan struct{}),
		aggDone:    make(chan struct{}),
	}

	go func() {
		defer close(p.tailDone)
		defer close(lines) // only this goroutine sends, so this can never panic
		if err := tail.Run(tailCtx, lines); err != nil && err != context.Canceled {
			slog.Error("tailer.failed", "error", err)
		}
	}()

	go func() {
		defer close(p.aggDone)
		if err := agg.Run(aggCtx, lines); err != nil && err != context.Canceled {
			slog.Error("aggregator.failed", "error", err)
		}
	}()

	return p
}

// stop shuts down in order: the tailer (final position save), then the
// aggregator (final flush). It returns only once the aggregator has finished
// or timed out, so the caller can close the database afterwards.
//
// Known limitation: if the tailer misses its timeout the aggregator is
// cancelled anyway, and a line the tailer hands over after the aggregator's
// drain is lost although its offset may be saved. Closing that window would
// need an unbounded wait.
func (p *pipeline) stop(tailerTimeout, aggTimeout time.Duration) {
	p.tailCancel()
	select {
	case <-p.tailDone:
	case <-time.After(tailerTimeout):
		slog.Error("tailer.stop_timeout")
		p.aggCancel()
	}

	select {
	case <-p.aggDone:
	case <-time.After(aggTimeout):
		slog.Error("aggregator.flush_timeout")
	}
	p.aggCancel() // also releases the aggregator context on a clean stop
}
