package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/watch"

	"github.com/stretchr/testify/require"
)

func (f *fakeIndex) LatestCommit(_ context.Context, set string) (*proto.Ref, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.commits[set]) == 0 {
		return nil, backup.ErrNotFound
	}

	return &proto.Ref{Hash: commitHash(len(f.commits[set]))}, nil
}

func commitHash(n int) []byte {
	return []byte(fmt.Sprintf("commit-%d", n))
}

// changeSource is a watch.Source whose changes the test makes.
type changeSource struct {
	mtx    sync.Mutex
	seq    int64
	queued *watch.Changes
}

func newChangeSource() *changeSource {
	return &changeSource{queued: watch.NewChanges()}
}

func (s *changeSource) Backend() string { return "test" }

func (s *changeSource) Now() (watch.Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.seq++

	return watch.Position{Stream: "test", Seq: s.seq}, nil
}

func (s *changeSource) Read(watch.Position) (*watch.Changes, watch.Position, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	changes := s.queued
	s.queued = watch.NewChanges()

	return changes, watch.Position{Stream: "test", Seq: s.seq}, nil
}

func (s *changeSource) Close() error { return nil }

func (s *changeSource) change(rel string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.seq++
	s.queued.Mark(rel, s.seq)
}

// watchedRun is what one run of a watched task was handed.
type watchedRun struct {
	at        time.Duration
	full      bool
	unchanged map[string]bool
}

// watched is a watched task on plan whose runs commit at once and record
// what they were told is unchanged among probes.
func (h *harness) watched(name string, plan *Plan, minGap, fullReadEvery time.Duration, probes ...string) (*Task, func() []watchedRun) {
	var (
		mtx  sync.Mutex
		runs []watchedRun
	)

	t := &Task{Name: name, Set: name, Kind: "path", Plan: plan, Spec: plan.String()}
	t.Watch = &Watch{
		Dir: "/srv/" + name, MinGap: minGap, FullReadEvery: fullReadEvery, Settings: "all",
		Run: func(_ context.Context, _ *job.Env, unchanged func(string) bool) (*backup.WalkResult, error) {
			r := watchedRun{at: h.since(), full: unchanged == nil, unchanged: map[string]bool{}}
			for _, p := range probes {
				r.unchanged[p] = unchanged != nil && unchanged(p)
			}

			mtx.Lock()
			runs = append(runs, r)
			mtx.Unlock()

			h.idx.commit(name, h.a.ID, h.a.now())

			h.idx.mu.Lock()
			hash := commitHash(len(h.idx.commits[name]))
			h.idx.mu.Unlock()

			return &backup.WalkResult{Ref: &proto.Ref{Hash: hash}}, nil
		},
	}
	t.Run = func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) { return t.Watch.Run(ctx, env, nil) }

	return t, func() []watchedRun {
		mtx.Lock()
		defer mtx.Unlock()

		return append([]watchedRun(nil), runs...)
	}
}

func TestAWatchedJobRunsOnChangeAfterTheGapAndOnItsPlanWithoutChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		src := newChangeSource()
		h.a.Follow = func(string) *watch.Tracker {
			return watch.NewTracker(func() (watch.Source, error) { return src, nil })
		}

		task, runs := h.watched("world", Every(24*time.Hour), 5*time.Minute, 48*time.Hour, "a/b", "c")
		h.run(task)

		time.Sleep(time.Minute)
		src.change("a/b")

		time.Sleep(30*time.Hour - time.Minute)
		src.change("c")

		time.Sleep(19 * time.Hour)
		h.stop()

		got := runs()
		require.Len(t, got, 5)

		require.Zero(t, got[0].at)
		require.True(t, got[0].full, "the first run reads everything")

		require.Equal(t, 5*time.Minute, got[1].at, "a change runs the job once the gap since the last start passed")
		require.False(t, got[1].full)
		require.Equal(t, map[string]bool{"a/b": false, "c": true}, got[1].unchanged)

		require.Equal(t, 24*time.Hour, got[2].at, "the plan runs the job without changes")
		require.Equal(t, map[string]bool{"a/b": true, "c": true}, got[2].unchanged)

		require.GreaterOrEqual(t, got[3].at, 30*time.Hour)
		require.Less(t, got[3].at, 30*time.Hour+time.Minute)
		require.Equal(t, map[string]bool{"a/b": true, "c": false}, got[3].unchanged)

		require.Equal(t, 48*time.Hour, got[4].at)
		require.True(t, got[4].full, "full-read-every after the first run's full read")
	})
}

func TestAPausedAgentStartsNoRunOnChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		src := newChangeSource()
		h.a.Follow = func(string) *watch.Tracker {
			return watch.NewTracker(func() (watch.Source, error) { return src, nil })
		}

		task, runs := h.watched("world", Every(24*time.Hour), 5*time.Minute, 48*time.Hour)
		h.run(task)

		time.Sleep(time.Minute)
		h.a.Pause()
		src.change("a")

		time.Sleep(time.Hour)
		require.Len(t, runs(), 1)

		h.a.Resume()
		time.Sleep(time.Minute)
		h.stop()

		require.Len(t, runs(), 2, "the change runs the job on resume")
	})
}

func TestAWatchedJobWithoutARecordWaitsOutTheGapSinceItsLastCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		src := newChangeSource()
		h.a.Follow = func(string) *watch.Tracker {
			return watch.NewTracker(func() (watch.Source, error) { return src, nil })
		}

		h.idx.commit("world", h.a.ID, h.t0.Add(-time.Minute))

		// the bubble starts at midnight, a daily plan's slot
		task, runs := h.watched("world", Every(7*24*time.Hour), 5*time.Minute, 48*time.Hour)
		h.run(task)

		time.Sleep(10 * time.Minute)
		h.stop()

		got := runs()
		require.Len(t, got, 1)
		require.GreaterOrEqual(t, got[0].at, 4*time.Minute, "the commit a minute before the start began the gap")
		require.Less(t, got[0].at, 5*time.Minute)
		require.True(t, got[0].full)
	})
}

func TestTheStatusFileSaysHowAJobIsWatchedAndWhyItsLastRunReadEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		src := newChangeSource()
		h.a.Follow = func(string) *watch.Tracker {
			return watch.NewTracker(func() (watch.Source, error) { return src, nil })
		}

		task, _ := h.watched("world", Every(24*time.Hour), 5*time.Minute, 48*time.Hour)
		plain := h.fake("etc", Every(time.Hour), time.Second, nil)
		h.run(task, plain)

		time.Sleep(time.Minute)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Equal(t, &WatchStatus{Backend: "test", Following: true, FullRead: watch.FullFirst}, status.Jobs[0].Watch)
		require.Nil(t, status.Jobs[1].Watch)

		src.change("a")
		time.Sleep(5 * time.Minute)

		status, err = ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Equal(t, &WatchStatus{Backend: "test", Following: true}, status.Jobs[0].Watch, "the run on change read only what changed")

		h.stop()
	})
}

func TestAJobWhoseDirectoryCantBeFollowedSaysWhy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()
		h.a.Follow = func(string) *watch.Tracker {
			return watch.NewTracker(func() (watch.Source, error) {
				return nil, fmt.Errorf("%w: no backend here", watch.ErrUnavailable)
			})
		}

		task, runs := h.watched("world", Every(24*time.Hour), 5*time.Minute, 48*time.Hour)
		h.run(task)

		time.Sleep(time.Hour)
		h.stop()

		require.Len(t, runs(), 1, "it runs on its plan alone")

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)

		w := status.Jobs[0].Watch
		require.False(t, w.Following)
		require.Contains(t, w.Reason, "no backend here")
		require.Equal(t, watch.FullNotFollowing, w.FullRead)
	})
}
