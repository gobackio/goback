package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Record is what a set's last watched run left: the position taken before
// it read the tree, the commit it made or kept, the settings it read the
// tree with and when the set was last read in full.
type Record struct {
	Position Position  `json:"position"`
	Commit   []byte    `json:"commit"`
	Settings string    `json:"settings"`
	FullAt   time.Time `json:"full_at"`
}

// Store keeps each set's Record; a Schedule loads one as a run of the set
// starts and saves one once it succeeded.
type Store interface {
	Load(set string) (Record, bool, error)
	Save(set string, r Record) error
}

// Trigger starts a run of a set; a Schedule calls it once something under
// a watched set's root changed and the set's gap has passed. RunNow must
// not wait for the run.
type Trigger interface {
	RunNow(set string)
}

// The reasons a run reads the whole tree.
const (
	// FullNotFollowing: no tracker follows the root, or it can't tell its
	// present, so the run has no position to record either.
	FullNotFollowing = "not-following"
	// FullFirst: the set has no record of a run yet.
	FullFirst = "first"
	// FullSettings: the run reads the tree with other settings than the
	// last, such as other filters.
	FullSettings = "settings-changed"
	// FullDue: the set's last full read is FullReadEvery ago.
	FullDue = "full-read-due"
	// FullOtherCommit: the set's latest commit isn't the one its last run
	// made.
	FullOtherCommit = "other-commit"
	// FullInDoubt: the tracker can't tell what changed since the last run,
	// as after a restart, a queue overflow or the root moving.
	FullInDoubt = "in-doubt"
)

// Options are how a Schedule watches one set.
type Options struct {
	// MinGap is the least time from a run's start to a run on change.
	MinGap time.Duration
	// FullReadEvery is how often a run reads everything whatever the
	// tracker says; zero never.
	FullReadEvery time.Duration
	// Settings fingerprints what besides the tree decides what a run
	// reads, such as its filters; a run with other settings reads
	// everything.
	Settings string
}

// Schedule decides for watched sets what each run reads, from the set's
// Record and its root's Tracker, and starts a run through Trigger once
// something changed and MinGap has passed since the set's last run
// started. A set without a Record, or whose tracker can't tell what
// changed, runs on change too.
type Schedule struct {
	Store   Store
	Trigger Trigger
	// Now is the clock; nil is time.Now.
	Now    func() time.Time
	Logger *slog.Logger

	mtx  sync.Mutex
	sets map[string]*watched
}

type watched struct {
	tracker   *Tracker
	o         Options
	lastStart time.Time
	running   bool
}

func (s *Schedule) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}

	return time.Now()
}

func (s *Schedule) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}

	return slog.Default()
}

// Watch watches set through t with o, or changes how it is watched.
func (s *Schedule) Watch(set string, t *Tracker, o Options) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.sets == nil {
		s.sets = map[string]*watched{}
	}

	if w, ok := s.sets[set]; ok {
		w.tracker, w.o = t, o
		return
	}

	s.sets[set] = &watched{tracker: t, o: o}
}

// Unwatch stops watching set; a run of it going on records nothing.
func (s *Schedule) Unwatch(set string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	delete(s.sets, set)
}

// Started tells the schedule a run of set started at, when it began
// before the schedule saw it, as the scan start of the set's newest
// commit.
func (s *Schedule) Started(set string, at time.Time) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if w, ok := s.sets[set]; ok && at.After(w.lastStart) {
		w.lastStart = at
	}
}

// Run is what one run of a set reads.
type Run struct {
	Set string
	// At is the position to record once the run succeeds; zero when there
	// is none.
	At Position
	// Unchanged reports the directories the run takes from the set's
	// latest commit, for job.PathSettings.Unchanged; nil reads everything.
	Unchanged func(rel string) bool
	// Full is why the run reads everything, one of the Full reasons; empty
	// when Unchanged is set.
	Full string
	// Detail says more about Full, such as why the tracker isn't following.
	Detail string

	started  time.Time
	settings string
	fullAt   time.Time
}

// Begin starts a run of set, whose latest commit is latest, and says what
// it reads. at is the position to record, taken before a snapshot the run
// reads; nil takes it now. End must follow.
func (s *Schedule) Begin(set string, latest []byte, at *Position) Run {
	now := s.now()
	r := Run{Set: set, started: now}

	s.mtx.Lock()
	w, ok := s.sets[set]
	if ok {
		w.running, w.lastStart = true, now
	}
	s.mtx.Unlock()

	if !ok {
		r.Full = FullNotFollowing
		return r
	}

	r.settings = w.o.Settings

	if at != nil {
		r.At = *at
	} else if p, err := w.tracker.Now(); err == nil {
		r.At = p
	} else {
		r.Detail = err.Error()
	}

	last, found, err := s.Store.Load(set)
	if err != nil {
		s.logger().Warn("reading the set's watch record failed; it is read in full", "set", set, "err", err)
		found = false
	}

	if found {
		r.fullAt = last.FullAt
	}

	switch {
	case r.At.IsZero() || !w.tracker.Following():
		r.Full = FullNotFollowing
		if r.Detail == "" {
			r.Detail = w.tracker.State().Reason
		}
	case !found || last.Commit == nil:
		r.Full = FullFirst
	case last.Settings != w.o.Settings:
		r.Full = FullSettings
	case w.o.FullReadEvery > 0 && now.Sub(last.FullAt) >= w.o.FullReadEvery:
		r.Full = FullDue
	case !bytes.Equal(latest, last.Commit):
		r.Full = FullOtherCommit
	}

	if r.Full != "" {
		return r
	}

	if err := w.tracker.Poll(last.Position); err != nil {
		r.Detail = err.Error()
	}

	unchanged, ok := w.tracker.After(last.Position)
	if !ok {
		r.Full = FullInDoubt
		return r
	}

	r.Unchanged = unchanged

	return r
}

// End ends r. When err is nil it records r's position with commit, the
// commit the run made or kept, for the set's next run.
func (s *Schedule) End(r Run, commit []byte, err error) error {
	s.mtx.Lock()
	w, ok := s.sets[r.Set]
	if ok {
		w.running = false
	}
	s.mtx.Unlock()

	if !ok || err != nil || r.At.IsZero() || commit == nil {
		return nil
	}

	rec := Record{Position: r.At, Commit: commit, Settings: r.settings, FullAt: r.fullAt}
	if r.Unchanged == nil {
		rec.FullAt = r.started
	}

	return s.Store.Save(r.Set, rec)
}

// Check reads every tracker's changes and starts the run of each watched
// set that changed since its last run, once its gap has passed. A set
// whose tracker isn't following runs on its plan alone.
func (s *Schedule) Check() {
	now := s.now()

	s.mtx.Lock()
	sets := make(map[string]watched, len(s.sets))
	for name, w := range s.sets {
		sets[name] = *w
	}
	s.mtx.Unlock()

	records := map[string]Record{}

	for name := range sets {
		rec, ok, err := s.Store.Load(name)
		if err != nil {
			s.logger().Warn("reading the set's watch record failed", "set", name, "err", err)
		}

		if ok && rec.Commit != nil {
			records[name] = rec
		}
	}

	for t, floor := range floors(sets, records) {
		if err := t.Poll(floor); err == nil {
			t.Prune(floor)
		}
	}

	// deciding under the lock puts each trigger before the Begin of the
	// run it starts, never after it
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for name, w := range s.sets {
		if w.running || !w.tracker.Following() || now.Sub(w.lastStart) < w.o.MinGap {
			continue
		}

		rec, ok, err := s.Store.Load(name)
		if err == nil && ok && rec.Commit != nil && !w.tracker.Changed(rec.Position) {
			continue
		}

		s.Trigger.RunNow(name)
	}
}

// floors is each tracker's floor: the oldest position of its sets' records
// in the stream it follows, or in any stream before it follows one.
func floors(sets map[string]watched, records map[string]Record) map[*Tracker]Position {
	out := map[*Tracker]Position{}

	for name, w := range sets {
		if _, ok := out[w.tracker]; !ok {
			out[w.tracker] = Position{}
		}

		rec, ok := records[name]
		if !ok {
			continue
		}

		w.tracker.mtx.Lock()
		stream := w.tracker.since.Stream
		if !w.tracker.started {
			stream = ""
		}
		w.tracker.mtx.Unlock()

		floor := out[w.tracker]
		if stream != "" && rec.Position.Stream != stream {
			continue
		}

		if floor.IsZero() || (rec.Position.Stream == floor.Stream && rec.Position.Seq < floor.Seq) {
			out[w.tracker] = rec.Position
		}
	}

	return out
}

// Run checks every interval until ctx ends.
func (s *Schedule) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		s.Check()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// State is how a set is watched.
type State struct {
	TrackerState
	Watched bool `json:"watched"`
}

// State reports how set is watched.
func (s *Schedule) State(set string) State {
	s.mtx.Lock()
	w, ok := s.sets[set]
	s.mtx.Unlock()

	if !ok {
		return State{}
	}

	return State{TrackerState: w.tracker.State(), Watched: true}
}

// FileStore is a Store in one JSON file.
type FileStore struct {
	path string

	mtx     sync.Mutex
	records map[string]Record
}

// OpenFileStore opens the FileStore at path, empty when there is no file
// there; one that doesn't parse is empty too, so every set is read in
// full once.
func OpenFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, records: map[string]Record{}}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &s.records); err != nil {
		s.records = map[string]Record{}
	}

	return s, nil
}

// Load implements Store.
func (s *FileStore) Load(set string) (Record, bool, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	r, ok := s.records[set]

	return r, ok, nil
}

// Save implements Store, replacing the file.
func (s *FileStore) Save(set string, r Record) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.records[set] = r

	data, err := json.Marshal(s.records)
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("recording the watched runs: %w", err)
	}

	return nil
}

// MemoryStore is a Store that keeps Records in memory.
type MemoryStore struct {
	mtx     sync.Mutex
	records map[string]Record
}

// Load implements Store.
func (s *MemoryStore) Load(set string) (Record, bool, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	r, ok := s.records[set]

	return r, ok, nil
}

// Save implements Store.
func (s *MemoryStore) Save(set string, r Record) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.records == nil {
		s.records = map[string]Record{}
	}

	s.records[set] = r

	return nil
}
