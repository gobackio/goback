// Package progress reports how far a long-running store operation has
// got. A caller puts a Func in the context it runs the operation with;
// the operation reports each phase it enters and its counts as they move:
//
//	ctx = progress.NewContext(ctx, func(u progress.Update) { ... })
//	report, err := store.Collect(ctx, opts)
package progress

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Operations, the Op of an Update.
const (
	OpCollect = "collect"
	OpCompact = "compact"
	OpSplit   = "split"
	OpRewrite = "rewrite"
	OpScrub   = "scrub"
	OpRetire  = "retire"
	OpReIndex = "reindex"
	OpIngest  = "ingest"
	OpCommit  = "commit"
	OpReseal  = "reseal"
)

// Phases, the Phase of an Update. An operation enters its phases in the
// order listed for it and may skip some; RewritePlan enters its phase once
// per plan.
const (
	// collect: snapshot, roots, mark, merge, condemn, counting the
	// tombstones checked against the index, confirm, then publish or
	// sweep, then purge.
	PhaseSnapshot = "snapshot"
	PhaseRoots    = "roots"
	PhaseMark     = "mark"
	PhaseMerge    = "merge"
	PhaseCondemn  = "condemn"
	PhaseConfirm  = "confirm"
	PhasePublish  = "publish"
	PhaseSweep    = "sweep"
	PhasePurge    = "purge"
	// compact, split and rewrite.
	PhaseRewrite = "rewrite"
	// scrub.
	PhaseScrub = "scrub"
	// retire: due, tombstones, rows, prune.
	PhaseDue        = "due"
	PhaseTombstones = "tombstones"
	PhaseRows       = "rows"
	PhasePrune      = "prune"
	// reindex: tombstones, pins, commits, sets, policies, damage; reseal:
	// commits, then pins.
	PhasePins     = "pins"
	PhaseCommits  = "commits"
	PhaseSets     = "sets"
	PhasePolicies = "policies"
	PhaseDamage   = "damage"
	// ingest.
	PhaseIngest = "ingest"
	// commit: check, counting the session's objects walked, lost, counting
	// the objects read above what the session lacks when it is stored
	// incomplete, condemned, counting the objects walked below what the
	// session deduplicated when a collection sealed during the session,
	// then index, counting the directories walked.
	PhaseCheck     = "check"
	PhaseLost      = "lost"
	PhaseCondemned = "condemned"
	PhaseIndex     = "index"
)

// Update is how far one phase of an operation has got. Done counts the
// phase's units (archives, root batches, commits, sets) and Bytes the
// bytes it has moved; a zero Total or BytesTotal means the phase does not
// know its total. Within a phase Done and Bytes never decrease.
type Update struct {
	Op, Phase         string
	Done, Total       int64
	Bytes, BytesTotal int64
}

// Func receives the updates of the operations run with a context that
// carries it. One operation never calls it concurrently, but may call it
// from any of its goroutines, so it should return quickly. A Func shared by
// operations that run at the same time must be safe for concurrent use.
type Func func(Update)

// Interval is the least time between two updates of a phase, apart from
// the first and the last.
const Interval = time.Second

type key struct{}

// NewContext returns a copy of ctx whose operations report to fn; a nil
// fn reports nothing.
func NewContext(ctx context.Context, fn Func) context.Context {
	return context.WithValue(ctx, key{}, fn)
}

// FromContext returns the Func ctx carries, or nil.
func FromContext(ctx context.Context) Func {
	fn, _ := ctx.Value(key{}).(Func)

	return fn
}

// Phase counts the progress of one phase and reports it. Its methods are
// safe for concurrent use, and do nothing on a nil Phase.
type Phase struct {
	fn         Func
	op, phase  string
	total      int64
	bytesTotal int64

	done, bytes atomic.Int64
	last        atomic.Int64

	mtx sync.Mutex
}

// Start reports that ctx's operation op entered phase, with the totals
// it knows, and returns the Phase to count it with. It returns nil when
// ctx carries no Func.
func Start(ctx context.Context, op, phase string, total, bytesTotal int64) *Phase {
	fn := FromContext(ctx)
	if fn == nil {
		return nil
	}

	p := &Phase{fn: fn, op: op, phase: phase, total: total, bytesTotal: bytesTotal}
	p.last.Store(time.Now().UnixNano())
	p.report()

	return p
}

// Add counts done more units and bytes more bytes, and reports them if
// the last update is at least Interval old.
func (p *Phase) Add(done, bytes int64) {
	if p == nil {
		return
	}

	p.done.Add(done)
	p.bytes.Add(bytes)

	now := time.Now().UnixNano()
	last := p.last.Load()
	if now-last < int64(Interval) || !p.last.CompareAndSwap(last, now) {
		return
	}

	p.report()
}

// Finish reports the phase's final counts. Nothing may Add after it.
func (p *Phase) Finish() {
	if p == nil {
		return
	}

	p.report()
}

func (p *Phase) report() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	// read under the lock, so a later update never carries smaller counts
	p.fn(Update{Op: p.op, Phase: p.phase, Done: p.done.Load(), Total: p.total, Bytes: p.bytes.Load(), BytesTotal: p.bytesTotal})
}
