package backup

import (
	"context"
	"io"

	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/semaphore"
)

// prefetch fetches the items from..to-1 in order, keeping up to readWindow
// of them, and readWindowBytes of what size says they weigh, fetched or in
// flight ahead of the one taken, each as soon as the window has room.
type prefetch[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	// fetched hands over each item's result in order
	fetched chan chan T
	window  chan struct{}
	bytes   *semaphore.Weighted
	size    func(int) int64
	next    int
	to      int
}

func newPrefetch[T any](ctx context.Context, from, to int, size func(int) int64, fetch func(context.Context, int) T) *prefetch[T] {
	ctx, cancel := context.WithCancel(ctx)

	p := &prefetch[T]{
		ctx: ctx, cancel: cancel,
		fetched: make(chan chan T, readWindow),
		window:  make(chan struct{}, readWindow),
		bytes:   semaphore.NewWeighted(readWindowBytes),
		size:    size,
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

			if p.bytes.Acquire(ctx, p.weight(i)) != nil {
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
	p.bytes.Release(p.weight(p.next))
	p.next++

	return got, nil
}

// weight is what item i holds of the byte window.
func (p *prefetch[T]) weight(i int) int64 {
	return min(max(p.size(i), 0), readWindowBytes)
}

// stop stops fetching what was not taken.
func (p *prefetch[T]) stop() {
	p.cancel()
}

// partBytes is the data a part's fetch brings, none for a hole.
func partBytes(part *proto.FilePart) int64 {
	if part.GetHole() > 0 {
		return 0
	}

	return int64(part.GetLength())
}
