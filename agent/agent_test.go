package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// fakeIndex answers CommitInfo from the commits the fake tasks make.
type fakeIndex struct {
	backup.Index

	mu      sync.Mutex
	commits map[string][]*proto.Commit
	err     error
}

func (f *fakeIndex) CommitInfo(_ context.Context, set string, _ time.Time, _ int) ([]*proto.Commit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	cs := f.commits[set]
	if len(cs) == 0 {
		return nil, nil
	}

	return cs[len(cs)-1:], nil
}

func (f *fakeIndex) commit(set, agent string, start time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.commits == nil {
		f.commits = map[string][]*proto.Commit{}
	}

	f.commits[set] = append(f.commits[set], &proto.Commit{BackupSet: set, AgentId: agent, ScanStartNs: start.UnixNano(), Timestamp: start.Unix()})
}

// harness runs an agent over fake tasks inside a synctest bubble, and
// records when each task starts, relative to the bubble's start.
type harness struct {
	t   *testing.T
	t0  time.Time
	idx *fakeIndex
	a   *Agent

	// step moves the agent's wall clock away from the bubble's.
	step atomic.Int64

	mu      sync.Mutex
	starts  map[string][]time.Duration
	results map[string][]string
	full    int
	maxFull int

	cancel context.CancelFunc
	done   chan struct{}
}

func newHarness(t *testing.T) *harness {
	h := &harness{
		t:       t,
		t0:      time.Now(),
		idx:     &fakeIndex{},
		starts:  map[string][]time.Duration{},
		results: map[string][]string{},
	}

	h.a = &Agent{
		ID:          "a1",
		Open:        func(context.Context) (*job.Env, error) { return &job.Env{Index: h.idx}, nil },
		Concurrency: 2,
		Grace:       30 * time.Second,
		Logger:      slog.New(slog.DiscardHandler),
		Now:         func() time.Time { return time.Now().Add(time.Duration(h.step.Load())) },
	}

	return h
}

func (h *harness) since() time.Duration {
	return time.Since(h.t0)
}

func (h *harness) started(name string, log bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.starts[name] = append(h.starts[name], h.since())

	if !log {
		h.full++
		h.maxFull = max(h.maxFull, h.full)
	}
}

func (h *harness) ended(name string, log bool, result string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.results[name] = append(h.results[name], result)

	if !log {
		h.full--
	}
}

// fake is a task on plan whose runs take d, then commit, or fail when fail
// says so for that attempt.
func (h *harness) fake(name string, plan *Plan, d time.Duration, fail func(attempt int) error) *Task {
	t := &Task{Name: name, Set: name, Kind: "path", Plan: plan, Spec: plan.String()}

	attempts := 0
	t.Run = func(ctx context.Context, _ *job.Env) (*backup.WalkResult, error) {
		start := h.a.now()
		attempts++
		h.started(name, t.Log)

		select {
		case <-time.After(d):
		case <-ctx.Done():
			h.ended(name, t.Log, ResultCancelled)
			return nil, ctx.Err()
		}

		if fail != nil {
			if err := fail(attempts); err != nil {
				h.ended(name, t.Log, ResultFailed)
				return nil, err
			}
		}

		h.idx.commit(name, h.a.ID, start)
		h.ended(name, t.Log, ResultOK)

		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{byte(attempts)}}}, nil
	}

	return t
}

func (h *harness) run(tasks ...*Task) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})

	go func() {
		defer close(h.done)
		require.NoError(h.t, h.a.Run(ctx, tasks))
	}()
}

// stop signals the agent and waits for Run to return.
func (h *harness) stop() {
	h.cancel()
	<-h.done
}

func (h *harness) startsOf(name string) []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]time.Duration(nil), h.starts[name]...)
}

func minutes(ms ...float64) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m * float64(time.Minute))
	}

	return out
}

func TestATaskNeverOverlapsItselfAndRunsOnceMoreAfterAnOverrun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.run(h.fake("wal", Every(time.Minute), 150*time.Second, nil))

		time.Sleep(6*time.Minute + time.Second)
		h.stop()

		require.Equal(t, minutes(0, 2.5, 5), h.startsOf("wal"))
		require.Equal(t, 1, h.maxFull)
	})
}

func TestTheConcurrencyLimitHoldsAndLogTasksBypassIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		var tasks []*Task
		for _, name := range []string{"f1", "f2", "f3", "f4"} {
			tasks = append(tasks, h.fake(name, Every(time.Hour), 10*time.Minute, nil))
		}

		wal := h.fake("wal", Every(time.Minute), time.Second, nil)
		wal.Log = true

		h.run(append(tasks, wal)...)

		time.Sleep(30*time.Minute - time.Second)
		h.stop()

		require.Equal(t, 2, h.maxFull)
		require.Equal(t, minutes(0), h.startsOf("f1"))
		require.Equal(t, minutes(0), h.startsOf("f2"))
		require.Equal(t, minutes(10), h.startsOf("f3"))
		require.Equal(t, minutes(10), h.startsOf("f4"))
		require.Len(t, h.startsOf("wal"), 30)
	})
}

func TestBackoffDoublesCapsAndYieldsToTheNextSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		failing := func(int) error { return errors.New("store unreachable") }

		h.run(
			h.fake("daily", Every(24*time.Hour), 0, failing),
			h.fake("often", Every(10*time.Minute), 0, failing),
		)

		time.Sleep(190 * time.Minute)
		h.stop()

		require.Equal(t, minutes(0, 1, 3, 7, 15, 31, 63, 123, 183), h.startsOf("daily"))
		require.Equal(t, minutes(0, 1, 3, 7, 10, 20, 30), h.startsOf("often")[:7])
	})
}

func TestASuccessResetsTheBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		h.run(h.fake("w", Every(time.Hour), 0, func(attempt int) error {
			if attempt == 3 {
				return nil
			}

			return errors.New("bad hook")
		}))

		time.Sleep(65 * time.Minute)
		h.stop()

		require.Equal(t, minutes(0, 1, 3, 60, 61, 63), h.startsOf("w"))
	})
}

func TestAPanicIsAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		task := h.fake("w", Every(time.Hour), 0, nil)
		run := task.Run
		panicked := false
		task.Run = func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
			if !panicked {
				panicked = true
				h.started("w", false)
				h.ended("w", false, ResultFailed)
				panic("boom")
			}

			return run(ctx, env)
		}

		h.run(task)

		time.Sleep(2 * time.Minute)
		h.stop()

		require.Equal(t, minutes(0, 1), h.startsOf("w"))
		require.Equal(t, []string{ResultFailed, ResultOK}, h.results["w"])
	})
}

func TestShutdownGivesRunsTheGraceThenCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.run(
			h.fake("quick", Every(time.Hour), 10*time.Second, nil),
			h.fake("slow", Every(time.Hour), 2*time.Minute, nil),
		)

		time.Sleep(5 * time.Second)
		h.stop()

		require.Equal(t, 35*time.Second, h.since())
		require.Equal(t, []string{ResultOK}, h.results["quick"])
		require.Equal(t, []string{ResultCancelled}, h.results["slow"])
	})
}

func TestASecondSignalCancelsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.run(h.fake("slow", Every(time.Hour), 2*time.Minute, nil))

		time.Sleep(5 * time.Second)
		h.cancel()
		time.Sleep(time.Second)
		h.a.Abort()
		<-h.done

		require.Equal(t, 6*time.Second, h.since())
		require.Equal(t, []string{ResultCancelled}, h.results["slow"])
	})
}

func TestAClockSteppedForwardRunsADueTaskOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.idx.commit("w", "a1", h.t0)

		h.run(h.fake("w", Every(time.Hour), 0, nil))

		time.Sleep(30 * time.Second)
		require.Empty(t, h.startsOf("w"))

		h.step.Store(int64(2 * time.Hour))

		time.Sleep(30 * time.Minute)
		h.stop()

		require.Equal(t, minutes(1), h.startsOf("w"))
	})
}

func TestAnUnreachableStoreDelaysRunsWithSpacedRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		var attempts []time.Duration
		h.a.Open = func(context.Context) (*job.Env, error) {
			attempts = append(attempts, h.since())
			if h.since() < 10*time.Minute {
				return nil, errors.New("connection refused")
			}

			return &job.Env{Index: h.idx}, nil
		}

		h.run(h.fake("w", Every(time.Hour), 0, nil))

		time.Sleep(20 * time.Minute)
		h.stop()

		seconds := func(ss ...int) []time.Duration {
			out := make([]time.Duration, len(ss))
			for i, s := range ss {
				out[i] = time.Duration(s) * time.Second
			}

			return out
		}

		require.Equal(t, seconds(0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 811), attempts)
		require.Equal(t, seconds(811), h.startsOf("w"))
	})
}

func TestALogTaskWaitsForItsBase(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		wal := h.fake("db/wal", Every(time.Minute), time.Second, nil)
		wal.Log = true
		wal.After = "db/base"

		h.run(h.fake("db/base", Every(24*time.Hour), 5*time.Minute, nil), wal)

		time.Sleep(7*time.Minute - time.Second)
		h.stop()

		require.Equal(t, minutes(5, 6), h.startsOf("db/wal"))
	})
}

func TestReloadReplansOnlyWhatChanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)

		a := h.fake("a", Every(time.Hour), time.Second, nil)
		b := h.fake("b", Every(time.Hour), 10*time.Minute, nil)
		h.run(a, b)

		time.Sleep(time.Minute)

		changed := h.fake("b", Every(2*time.Hour), 10*time.Minute, nil)
		added := h.fake("c", Every(time.Hour), time.Second, nil)
		h.a.Reload([]*Task{h.fake("a", Every(time.Hour), time.Second, nil), changed, added})

		time.Sleep(24 * time.Minute)
		h.a.Reload([]*Task{a})

		time.Sleep(2*time.Hour + 30*time.Minute)
		h.stop()

		require.Equal(t, minutes(0, 60, 120), h.startsOf("a"))
		require.Equal(t, minutes(0, 10), h.startsOf("b"))
		require.Equal(t, minutes(1), h.startsOf("c"))
	})
}

func TestAServeTaskRestartsWithBackoffAndCommitsOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		var starts []time.Duration

		var commits atomic.Int32

		task := &Task{Name: "db/wal", Set: "db/wal", Kind: "postgres-receive", Log: true}
		task.Serve = func(ctx context.Context, _ *job.Env, c *Control) error {
			starts = append(starts, h.since())
			if len(starts) < 3 {
				return errors.New("connection reset")
			}

			tick := time.NewTicker(30 * time.Second)
			defer tick.Stop()

			for {
				select {
				case <-tick.C:
					commits.Add(1)
					c.Committed(&backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}})
				case <-c.Stop():
					commits.Add(1)
					c.Committed(&backup.WalkResult{Ref: &proto.Ref{Hash: []byte{2}}})

					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		h.run(task)

		time.Sleep(2*time.Minute + 10*time.Second)
		h.stop()

		require.Equal(t, []time.Duration{0, time.Second, 3 * time.Second}, starts)
		require.Equal(t, 2*time.Minute+10*time.Second, h.since())
		require.EqualValues(t, 5, commits.Load())

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Equal(t, ResultOK, status.Jobs[0].LastResult)
		require.Equal(t, "02", status.Jobs[0].LastCommit)
	})
}

func TestAnAgentThatNeverOpensItsStoreBecomesUnhealthy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()
		h.a.Open = func(context.Context) (*job.Env, error) { return nil, errors.New("access denied") }

		h.run(h.fake("w", Every(time.Hour), 0, nil))

		time.Sleep(3*time.Hour + 10*time.Minute)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		h.stop()

		require.Equal(t, JobPlanning, status.Jobs[0].State)
		require.Len(t, status.Problems(h.a.now()), 1)
		require.Equal(t, "access denied", status.OpenError)
	})
}

func TestTheStatusSaysWhyTheLastRunCannotBeRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()
		h.idx.err = errors.New("index unreachable")

		h.run(h.fake("w", Every(time.Hour), 0, nil))
		time.Sleep(10 * time.Second)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Empty(t, status.OpenError)
		require.Equal(t, "index unreachable", status.Jobs[0].PlanError)

		h.idx.mu.Lock()
		h.idx.err = nil
		h.idx.mu.Unlock()
		time.Sleep(10 * time.Minute)

		status, err = ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Empty(t, status.Jobs[0].PlanError)

		h.stop()
	})
}

func TestAStoreThatHangsOnOpenHoldsNeitherTheStatusNorTheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()
		h.a.Open = func(ctx context.Context) (*job.Env, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		h.run(h.fake("w", Every(time.Hour), 0, nil))

		time.Sleep(3*time.Minute + time.Second)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.LessOrEqual(t, h.a.now().Sub(status.UpdatedAt), maxSleep, "the heartbeat goes on")

		h.stop()
		require.Equal(t, 3*time.Minute+time.Second, h.since(), "the stop waits for no open")
	})
}

func TestADatabaseShutdownRestartsTheReceiverWithoutAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		var starts []time.Duration

		task := &Task{Name: "db/wal", Set: "db/wal", Kind: "postgres-receive", Log: true}
		task.Serve = func(ctx context.Context, _ *job.Env, c *Control) error {
			starts = append(starts, h.since())
			if len(starts) < 4 {
				return postgres.ErrServerShutDown
			}

			<-c.Stop()

			return nil
		}

		h.run(task)

		time.Sleep(time.Minute)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Zero(t, status.Jobs[0].Failures)

		h.stop()

		require.Equal(t, []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second}, starts)
	})
}

func TestAServeTaskIsJudgedByItsHealthReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		var stateDir string

		task := &Task{Name: "db/wal", Set: "db/wal", Kind: "postgres-receive", Log: true, StaleAfter: 3 * time.Minute}
		task.Serve = func(ctx context.Context, _ *job.Env, c *Control) error {
			stateDir = c.StateDir()

			for i := 0; ; i++ {
				c.Report(map[string]int{"lag_seconds": i * 10}, i < 6)

				if i%6 == 0 {
					c.Committed(&backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}})
				}

				select {
				case <-time.After(10 * time.Second):
				case <-c.Stop():
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		h.run(task)

		time.Sleep(55 * time.Second)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Equal(t, h.a.StateDir, stateDir)
		require.Equal(t, map[string]any{"lag_seconds": float64(50)}, status.Jobs[0].Details)
		require.EqualValues(t, 180, status.Jobs[0].StaleAfter)
		require.Empty(t, status.Problems(time.Now()))

		time.Sleep(4 * time.Minute)

		status, err = ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Len(t, status.Problems(time.Now()), 1, "lagging since its last healthy report, though it commits")

		h.stop()
	})
}

func TestTheStatusFileReportsEachJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		h.run(h.fake("w", Every(time.Hour), time.Minute, nil))

		time.Sleep(30 * time.Second)

		status, err := ReadStatus(h.a.StateDir)
		require.NoError(t, err)
		require.Equal(t, AgentRunning, status.State)
		require.Equal(t, JobRunning, status.Jobs[0].State)

		time.Sleep(time.Minute)

		status, err = ReadStatus(h.a.StateDir)
		require.NoError(t, err)

		j := status.Jobs[0]
		require.Equal(t, JobIdle, j.State)
		require.Equal(t, ResultOK, j.LastResult)
		require.True(t, h.t0.Equal(j.LastSuccess))
		require.True(t, h.t0.Add(time.Hour).Equal(j.NextDue))
		require.EqualValues(t, 3*60*60, j.StaleAfter)
		require.Empty(t, status.Problems(time.Now()))

		h.stop()
	})
}

func TestABinlogTaskIsUnhealthyWhileTheBinaryLogIsOff(t *testing.T) {
	stop := make(chan struct{})

	var reports []bool

	c := &Control{stop: stop, report: func(_ any, healthy bool) {
		reports = append(reports, healthy)
		close(stop)
	}}

	require.NoError(t, receiveWhileBinlogOn(context.Background(), c, func() error {
		return fmt.Errorf("checking the server: %w", mysql.ErrNoBinlog)
	}))
	require.Equal(t, []bool{false}, reports)
}

func TestProblems(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	hour := int64(3600)

	healthy := &Status{StartedAt: now.Add(-24 * time.Hour), UpdatedAt: now.Add(-time.Minute), Jobs: []JobStatus{
		{Name: "ok", StaleAfter: 3 * hour, LastSuccess: now.Add(-2 * time.Hour)},
		{Name: "base pending", State: JobBlocked, StaleAfter: 60},
		{Name: "no bound"},
	}}
	require.Empty(t, healthy.Problems(now))

	hung := *healthy
	hung.UpdatedAt = now.Add(-4 * time.Minute)
	require.Len(t, hung.Problems(now), 1)

	stopped := *healthy
	stopped.State = AgentStopped
	require.Len(t, stopped.Problems(now), 1)

	stale := *healthy
	stale.Jobs = []JobStatus{{Name: "old", StaleAfter: hour, LastSuccess: now.Add(-2 * time.Hour)}}
	require.Len(t, stale.Problems(now), 1)

	never := *healthy
	never.Jobs = []JobStatus{{Name: "never", StaleAfter: hour}}
	require.Len(t, never.Problems(now), 1)

	never.StartedAt = now.Add(-time.Minute)
	require.Empty(t, never.Problems(now))

	dirty := *healthy
	dirty.Jobs = []JobStatus{{Name: "dirty", StaleAfter: 3 * hour, LastSuccess: now.Add(-2 * time.Hour), DirtyRuns: 3}}
	require.Len(t, dirty.Problems(now), 1)

	dirty.Jobs[0].DirtyRuns = 2
	require.Empty(t, dirty.Problems(now))
}

func TestADirtyRunIsNoSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.a.StateDir = t.TempDir()

		task := h.fake("w", Every(time.Hour), 0, nil)
		run := task.Run
		clean := false
		task.Run = func(ctx context.Context, env *job.Env) (*backup.WalkResult, error) {
			result, err := run(ctx, env)
			if result != nil && !clean {
				result.Unreadable = 1
			}

			return result, err
		}

		h.run(task)

		status := func() JobStatus {
			s, err := ReadStatus(h.a.StateDir)
			require.NoError(t, err)

			return s.Jobs[0]
		}

		time.Sleep(2*time.Hour + time.Minute)

		j := status()
		require.Equal(t, ResultDirty, j.LastResult)
		require.Zero(t, j.LastSuccess)
		require.True(t, h.t0.Add(2*time.Hour).Equal(j.LastDirty))
		require.Equal(t, 3, j.DirtyRuns)
		require.Zero(t, j.Failures)

		clean = true
		time.Sleep(time.Hour)

		j = status()
		require.True(t, h.t0.Add(3*time.Hour).Equal(j.LastSuccess))
		require.Zero(t, j.DirtyRuns)

		h.stop()
	})
}

func TestAPausedAgentStartsNothingScheduledAndCatchesUpOnResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.run(h.fake("files", Every(10*time.Minute), time.Minute, nil))

		time.Sleep(2 * time.Minute)
		h.a.Pause()
		time.Sleep(13 * time.Minute)
		h.a.Resume()
		time.Sleep(time.Second)
		h.stop()

		require.Equal(t, minutes(0, 15), h.startsOf("files"))
	})
}

func TestRunNowStartsARunAtOnceOrRightAfterTheOneGoing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.run(h.fake("files", Every(time.Hour), 2*time.Minute, nil))

		time.Sleep(5 * time.Minute)
		h.a.RunNow("files")
		time.Sleep(30 * time.Second)
		h.a.RunNow("files")
		h.a.RunNow("unknown")

		time.Sleep(5 * time.Minute)
		h.a.Pause()
		h.a.RunNow("files")
		time.Sleep(time.Second)
		h.stop()

		require.Equal(t, minutes(0, 5, 7, 10.5), h.startsOf("files"))
	})
}
