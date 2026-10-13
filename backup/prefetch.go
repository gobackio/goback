package backup

import (
	"context"
	"io"
)

// prefetch fetches the items from..to-1 in order, keeping up to readWindow
// of them fetched or in flight ahead of the one taken, each as soon as a
// place in the window frees.
type prefetch[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	// fetched hands over each item's result in order
	fetched chan chan T
	window  chan struct{}
	next    int
	to      int
}

func newPrefetch[T any](ctx context.Context, from, to int, fetch func(context.Context, int) T) *prefetch[T] {
	ctx, cancel := context.WithCancel(ctx)

	p := &prefetch[T]{
		ctx: ctx, cancel: cancel,
		fetched: make(chan chan T, readWindow),
		window:  make(chan struct{}, readWindow),
		next:    from,
		to:      to,
	}

	go func() {
		for i := from; i < to; i++ {
			select {
			case p.window <- struct{}{}:
			case <-ctx.Done():
				return
			}

			result := make(chan T, 1)
			p.fetched <- result

			go func() { result <- fetch(ctx, i) }()
		}
	}()

	return p
}

// take waits for the next item and frees its place in the window; it
// fails with io.EOF past the last one.
func (p *prefetch[T]) take() (T, error) {
	var zero T

	if p.next == p.to {
		return zero, io.EOF
	}

	var result chan T

	select {
	case result = <-p.fetched:
	case <-p.ctx.Done():
		return zero, p.ctx.Err()
	}

	var got T

	select {
	case got = <-result:
	case <-p.ctx.Done():
		return zero, p.ctx.Err()
	}

	<-p.window
	p.next++

	return got, nil
}

// stop stops fetching what was not taken.
func (p *prefetch[T]) stop() {
	p.cancel()
}
