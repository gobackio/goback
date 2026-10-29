package storage

import (
	"context"
	"sync"
)

// Tally adds up the requests a store makes to its object store on behalf
// of a context that carries it, as WithTally gives it: the ranged reads it
// serves objects from. A caller learns what one piece of work read from
// the object store, and so that what a cache answered asked it nothing. A
// Tally is safe for concurrent use; its zero value is empty and ready.
type Tally struct {
	mu     sync.Mutex
	counts map[Operation]Count
}

// Count is the requests of one operation and the bytes they carried.
type Count struct {
	Requests int64
	Bytes    int64
}

type tallyKey struct{}

// WithTally returns a context whose requests count toward t as well as
// toward any tally ctx already carries.
func WithTally(ctx context.Context, t *Tally) context.Context {
	outer := talliesOf(ctx)

	return context.WithValue(ctx, tallyKey{}, append(outer[:len(outer):len(outer)], t))
}

func talliesOf(ctx context.Context) []*Tally {
	tallies, _ := ctx.Value(tallyKey{}).([]*Tally)
	return tallies
}

// Of is what the tally holds for op.
func (t *Tally) Of(op Operation) Count {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.counts[op]
}

// Take returns what the tally holds for op and empties it of op, for a
// caller that charges a tally's counts as they grow.
func (t *Tally) Take(op Operation) Count {
	t.mu.Lock()
	defer t.mu.Unlock()

	c := t.counts[op]
	delete(t.counts, op)

	return c
}

func (t *Tally) add(op Operation, requests, bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.counts == nil {
		t.counts = map[Operation]Count{}
	}

	c := t.counts[op]
	c.Requests += requests
	c.Bytes += bytes
	t.counts[op] = c
}
