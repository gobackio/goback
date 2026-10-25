package watch

import (
	"errors"
	"fmt"
	"sync"
)

// MaxDirs bounds how many changed paths a Tracker keeps before it counts
// reading the tree in full as cheaper.
const MaxDirs = 50_000

// Tracker follows the changes of one root for every set backing it up,
// each of which asks what changed after its own position. It reads them
// from a Source when polled, opening a new one when the last one stopped
// following the root.
type Tracker struct {
	open func() (Source, error)

	mtx     sync.Mutex
	src     Source
	closed  bool
	backend string
	started bool
	since   Position
	cursor  Position
	changes *Changes
	// reason is why the tracker doesn't follow the root, or last lost track
	reason string
}

// NewTracker returns a Tracker reading the Sources open opens. It opens
// the first one on the first Poll.
func NewTracker(open func() (Source, error)) *Tracker {
	return &Tracker{open: open, changes: NewChanges()}
}

// Follow returns a Tracker of root through the best backend Open finds.
func Follow(root string) *Tracker {
	return NewTracker(func() (Source, error) { return Open(root) })
}

// Poll reads what changed since the last Poll. A tracker that isn't
// following yet starts at from, the oldest position a set still asks
// about, or at the present when from is zero or the source can't read from
// it. When the source loses track, every set whose position is from before
// counts as in doubt, and the error says why.
func (t *Tracker) Poll(from Position) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.closed {
		return errClosed
	}

	if t.src == nil {
		src, err := t.open()
		if err != nil {
			t.reason = err.Error()
			return err
		}

		t.src, t.backend = src, src.Backend()
	}

	if !t.started {
		if from.IsZero() {
			now, err := t.src.Now()
			if err != nil {
				t.lost(err)
				return err
			}

			from = now
		}

		t.started, t.since, t.cursor, t.changes = true, from, from, NewChanges()
	}

	changes, end, err := t.src.Read(t.cursor)
	if err == nil && t.changes.Len()+changes.Len() > MaxDirs {
		err = fmt.Errorf("%w: more than %d directories changed", ErrInDoubt, MaxDirs)
	}

	if err != nil {
		t.restart(err)
		return err
	}

	t.changes.merge(changes)
	t.cursor = end
	t.reason = ""

	return nil
}

// restart starts the tracker again at the source's present, after it lost
// track for err.
func (t *Tracker) restart(err error) {
	t.started, t.changes = false, NewChanges()
	t.reason = err.Error()

	now, nowErr := t.src.Now()
	if nowErr != nil {
		t.lost(nowErr)
		return
	}

	t.started, t.since, t.cursor = true, now, now
}

// lost drops a source that can't tell its present, unless it is still
// starting; the next Poll opens another.
func (t *Tracker) lost(err error) {
	t.started = false

	if errors.Is(err, ErrStarting) {
		if t.reason == "" {
			t.reason = err.Error()
		}

		return
	}

	t.reason = err.Error()
	_ = t.src.Close()
	t.src = nil
}

// Now is the source's present, the position a run takes before it reads
// the tree.
func (t *Tracker) Now() (Position, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.src == nil || !t.started {
		return Position{}, t.notFollowing()
	}

	return t.src.Now()
}

func (t *Tracker) notFollowing() error {
	if t.reason != "" {
		return errors.New(t.reason)
	}

	return ErrStarting
}

// Following reports whether the tracker follows the root without a gap
// since it last started.
func (t *Tracker) Following() bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	return t.started
}

// After reports, for a set whose last run took position p before it read
// the tree, whether nothing at or under a slash-separated directory below
// the root changed since; ok is false when the tracker can't tell.
// unchanged is safe for concurrent use and doesn't change as the tracker
// reads on.
func (t *Tracker) After(p Position) (unchanged func(rel string) bool, ok bool) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if !t.covers(p) {
		return nil, false
	}

	changes := t.changes.since(p.Seq)

	return func(rel string) bool { return changes.unchanged(fold(rel)) }, true
}

// Changed reports whether something under the root changed after p, or
// the tracker can't tell; subtrees it can't follow don't count.
func (t *Tracker) Changed(p Position) bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if !t.covers(p) {
		return true
	}

	seq, ok := t.changes.Dirs[""]

	return ok && seq >= p.Seq
}

func (t *Tracker) covers(p Position) bool {
	return t.started && p.Stream == t.since.Stream && p.Seq >= t.since.Seq
}

// Prune forgets what changed before floor, which every set has read past.
func (t *Tracker) Prune(floor Position) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.started && floor.Stream == t.since.Stream {
		t.changes.prune(floor.Seq)
	}
}

// TrackerState is what a Tracker reports about itself.
type TrackerState struct {
	// Backend names the source's backend; empty before one opened.
	Backend   string `json:"backend,omitempty"`
	Following bool   `json:"following"`
	// Reason is why the tracker isn't following, or last lost track.
	Reason string `json:"reason,omitempty"`
	// Unwatched are the subtrees it can't follow, with why.
	Unwatched map[string]string `json:"unwatched,omitempty"`
}

// State reports how the tracker follows the root.
func (t *Tracker) State() TrackerState {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	s := TrackerState{Backend: t.backend, Following: t.started, Reason: t.reason}

	if t.started && len(t.changes.Unwatched) > 0 {
		s.Unwatched = map[string]string{}
		for rel, why := range t.changes.Unwatched {
			s.Unwatched[rel] = why
		}
	}

	return s
}

var errClosed = errors.New("the tracker is closed")

// Close closes the source; the tracker follows nothing after.
func (t *Tracker) Close() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	t.started, t.closed = false, true

	if t.src == nil {
		return nil
	}

	err := t.src.Close()
	t.src = nil

	return err
}
