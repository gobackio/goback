package watch_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/watch"
	"github.com/stretchr/testify/require"
)

// fakeSource hands each Read the changes made since the one before, as a
// change journal does, and loses track once when lost is set.
type fakeSource struct {
	mtx     sync.Mutex
	stream  string
	seq     int64
	queued  *watch.Changes
	lost    bool
	nowErr  error
	closed  int
	started bool
}

func newFakeSource() *fakeSource {
	return &fakeSource{stream: "fake-1", seq: 10, queued: watch.NewChanges()}
}

func (f *fakeSource) Backend() string { return "fake" }

func (f *fakeSource) Now() (watch.Position, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if f.nowErr != nil {
		return watch.Position{}, f.nowErr
	}

	f.seq++

	return watch.Position{Stream: f.stream, Seq: f.seq}, nil
}

func (f *fakeSource) Read(from watch.Position) (*watch.Changes, watch.Position, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	if f.lost || from.Stream != f.stream {
		f.lost = false
		f.stream += "'"
		return nil, watch.Position{}, fmt.Errorf("%w: lost", watch.ErrInDoubt)
	}

	changes := f.queued
	f.queued = watch.NewChanges()

	return changes, watch.Position{Stream: f.stream, Seq: f.seq}, nil
}

func (f *fakeSource) Close() error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.closed++

	return nil
}

func (f *fakeSource) change(rel string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.seq++
	f.queued.Mark(rel, f.seq)
}

func (f *fakeSource) unwatch(rel string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.queued.Unwatch(rel, "no watch")
}

func trackerOf(src watch.Source) *watch.Tracker {
	return watch.NewTracker(func() (watch.Source, error) { return src, nil })
}

func TestATrackerTellsEachSetWhatChangedAfterItsOwnPosition(t *testing.T) {
	src := newFakeSource()
	tr := trackerOf(src)
	require.NoError(t, tr.Poll(watch.Position{}))

	first, err := tr.Now()
	require.NoError(t, err)

	src.change("a/b")
	require.NoError(t, tr.Poll(watch.Position{}))

	second, err := tr.Now()
	require.NoError(t, err)

	src.change("c")
	require.NoError(t, tr.Poll(watch.Position{}))

	unchanged, ok := tr.After(first)
	require.True(t, ok)
	require.False(t, unchanged("a"))
	require.False(t, unchanged("a/b"))
	require.False(t, unchanged(""))
	require.False(t, unchanged("c"))
	require.True(t, unchanged("a/x"))
	require.True(t, unchanged("d"))

	unchanged, ok = tr.After(second)
	require.True(t, ok)
	require.True(t, unchanged("a"), "the set whose run came after the change has it")
	require.False(t, unchanged("c"))
	require.True(t, tr.Changed(second))

	third, err := tr.Now()
	require.NoError(t, err)
	require.False(t, tr.Changed(third))

	src.lost = true
	require.ErrorIs(t, tr.Poll(watch.Position{}), watch.ErrInDoubt)

	_, ok = tr.After(third)
	require.False(t, ok, "once the source lost track, every set that read it before is in doubt")
	require.True(t, tr.Changed(third), "and runs on change")

	fresh, err := tr.Now()
	require.NoError(t, err)

	unchanged, ok = tr.After(fresh)
	require.True(t, ok)
	require.True(t, unchanged(""))
}

func TestAnUnwatchedSubtreeIsReadEveryRunButStartsNone(t *testing.T) {
	src := newFakeSource()
	tr := trackerOf(src)
	require.NoError(t, tr.Poll(watch.Position{}))

	p, err := tr.Now()
	require.NoError(t, err)

	src.unwatch("big/deep")
	require.NoError(t, tr.Poll(watch.Position{}))

	unchanged, ok := tr.After(p)
	require.True(t, ok)
	require.False(t, unchanged(""))
	require.False(t, unchanged("big"))
	require.False(t, unchanged("big/deep"))
	require.False(t, unchanged("big/deep/er"))
	require.True(t, unchanged("big/other"))
	require.False(t, tr.Changed(p))
	require.Equal(t, map[string]string{"big/deep": "no watch"}, tr.State().Unwatched)
}

func TestASourceThatCantTellItsPresentIsReopened(t *testing.T) {
	broken := newFakeSource()
	broken.nowErr = fmt.Errorf("%w: gone", watch.ErrInDoubt)
	good := newFakeSource()

	opened := 0
	tr := watch.NewTracker(func() (watch.Source, error) {
		opened++
		if opened == 1 {
			return broken, nil
		}
		return good, nil
	})

	require.Error(t, tr.Poll(watch.Position{}))
	require.False(t, tr.Following())
	require.Equal(t, 1, broken.closed)
	require.Contains(t, tr.State().Reason, "gone")

	require.NoError(t, tr.Poll(watch.Position{}))
	require.True(t, tr.Following())
}

func TestAStartingSourceIsKeptUntilItHasAPresent(t *testing.T) {
	src := newFakeSource()
	src.nowErr = watch.ErrStarting
	tr := trackerOf(src)

	require.ErrorIs(t, tr.Poll(watch.Position{}), watch.ErrStarting)
	require.Zero(t, src.closed)

	src.nowErr = nil
	require.NoError(t, tr.Poll(watch.Position{}))
	require.True(t, tr.Following())
}

// clock is a settable wall clock.
type clock struct {
	mtx sync.Mutex
	t   time.Time
}

func (c *clock) now() time.Time {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.t = c.t.Add(d)
}

type triggers struct {
	mtx  sync.Mutex
	sets []string
}

func (tr *triggers) RunNow(set string) {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	tr.sets = append(tr.sets, set)
}

func (tr *triggers) take() []string {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()

	out := tr.sets
	tr.sets = nil

	return out
}

type scheduleFixture struct {
	src   *fakeSource
	tr    *watch.Tracker
	clock *clock
	runs  *triggers
	s     *watch.Schedule
}

func newScheduleFixture(o watch.Options) *scheduleFixture {
	f := &scheduleFixture{src: newFakeSource(), clock: &clock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}, runs: &triggers{}}
	f.tr = trackerOf(f.src)
	f.s = &watch.Schedule{Store: &watch.MemoryStore{}, Trigger: f.runs, Now: f.clock.now}
	f.s.Watch("pc/files", f.tr, o)

	return f
}

// run runs the set once, committing commit, and returns what it read.
func (f *scheduleFixture) run(latest, commit string) watch.Run {
	r := f.s.Begin("pc/files", []byte(latest), nil)
	if err := f.s.End(r, []byte(commit), nil); err != nil {
		panic(err)
	}

	return r
}

func TestARunReadsOnlyWhatChangedOnceTheSetHasARecord(t *testing.T) {
	f := newScheduleFixture(watch.Options{MinGap: 5 * time.Minute, FullReadEvery: 7 * 24 * time.Hour, Settings: "include=*"})
	f.s.Check()

	first := f.run("", "c1")
	require.Nil(t, first.Unchanged)
	require.Equal(t, watch.FullFirst, first.Full)

	f.src.change("a")
	f.clock.add(time.Hour)

	second := f.run("c1", "c2")
	require.Empty(t, second.Full)
	require.NotNil(t, second.Unchanged)
	require.False(t, second.Unchanged("a"))
	require.True(t, second.Unchanged("b"))

	f.clock.add(time.Hour)
	third := f.run("c2", "c3")
	require.True(t, third.Unchanged("a"), "the last run read it")
}

func TestARunReadsEverythingWhenItCantTrustTheLastRecord(t *testing.T) {
	week := 7 * 24 * time.Hour
	f := newScheduleFixture(watch.Options{MinGap: 5 * time.Minute, FullReadEvery: week, Settings: "include=*"})
	f.s.Check()
	f.run("", "c1")

	f.clock.add(time.Hour)
	require.Equal(t, watch.FullOtherCommit, f.run("someone else's", "c2").Full)

	f.clock.add(time.Hour)
	require.Empty(t, f.run("c2", "c3").Full)

	f.s.Watch("pc/files", f.tr, watch.Options{MinGap: 5 * time.Minute, FullReadEvery: week, Settings: "include=*.txt"})
	require.Equal(t, watch.FullSettings, f.run("c3", "c4").Full)

	f.clock.add(week - time.Hour)
	require.Empty(t, f.run("c4", "c5").Full)

	f.clock.add(time.Hour)
	require.Equal(t, watch.FullDue, f.run("c5", "c6").Full, "a week after the last full read")
	require.Empty(t, f.run("c6", "c7").Full)

	f.src.lost = true
	_ = f.tr.Poll(watch.Position{})
	require.Equal(t, watch.FullInDoubt, f.run("c7", "c8").Full)
	require.Empty(t, f.run("c8", "c9").Full)
}

func TestAFailedRunRecordsNothing(t *testing.T) {
	f := newScheduleFixture(watch.Options{MinGap: 5 * time.Minute, FullReadEvery: 7 * 24 * time.Hour})
	f.s.Check()
	f.run("", "c1")

	f.src.change("a")
	failed := f.s.Begin("pc/files", []byte("c1"), nil)
	require.NoError(t, f.s.End(failed, nil, fmt.Errorf("the store is down")))

	again := f.run("c1", "c2")
	require.False(t, again.Unchanged("a"), "the failed run's position wasn't recorded")
}

func TestAnUnfollowedRootIsReadInFullAndStartsNoRun(t *testing.T) {
	f := newScheduleFixture(watch.Options{MinGap: 5 * time.Minute})
	f.src.nowErr = watch.ErrStarting
	f.s.Check()

	r := f.run("", "c1")
	require.Equal(t, watch.FullNotFollowing, r.Full)
	require.Contains(t, r.Detail, "starting")

	f.clock.add(time.Hour)
	f.s.Check()
	require.Empty(t, f.runs.take(), "a set whose root isn't followed runs on its plan alone")
}

func TestAChangeStartsARunOnceTheGapHasPassed(t *testing.T) {
	f := newScheduleFixture(watch.Options{MinGap: 5 * time.Minute})
	f.s.Check()
	require.Equal(t, []string{"pc/files"}, f.runs.take(), "a set without a record runs once its root is followed")

	f.run("", "c1")
	f.s.Check()
	require.Empty(t, f.runs.take())

	f.clock.add(time.Minute)
	f.src.change("a/b")
	f.s.Check()
	require.Empty(t, f.runs.take(), "inside the gap")

	f.clock.add(4 * time.Minute)
	f.s.Check()
	require.Equal(t, []string{"pc/files"}, f.runs.take())

	r := f.s.Begin("pc/files", []byte("c1"), nil)
	f.src.change("c")
	f.clock.add(10 * time.Minute)
	f.s.Check()
	require.Empty(t, f.runs.take(), "while the run goes on")
	require.NoError(t, f.s.End(r, []byte("c2"), nil))

	f.s.Check()
	require.Equal(t, []string{"pc/files"}, f.runs.take(), "a change during the run starts the next")
	f.run("c2", "c3")

	f.clock.add(time.Hour)
	f.s.Check()
	require.Empty(t, f.runs.take(), "nothing changed")
}

func TestAFileStoreKeepsRecordsAcrossOpens(t *testing.T) {
	path := t.TempDir() + "/watch.json"

	s, err := watch.OpenFileStore(path)
	require.NoError(t, err)

	rec := watch.Record{Position: watch.Position{Stream: "usn-1", Seq: 7}, Commit: []byte{1}, Settings: "x", FullAt: time.Unix(100, 0).UTC()}
	require.NoError(t, s.Save("pc/files", rec))

	again, err := watch.OpenFileStore(path)
	require.NoError(t, err)

	got, ok, err := again.Load("pc/files")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, rec, got)
}
