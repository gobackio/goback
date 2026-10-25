package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/schedule"

	"github.com/stretchr/testify/require"
)

func TestTheScheduleSaysWhatEachTaskPlans(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - {name: world, every: 6h, jitter: 10m, path: {dir: /srv/world, watch: off}}
  - {name: docs, path: {dir: /srv/docs}}
  - {name: mods, cron: "0 4 * * 1-5", path: {dir: /srv/mods}}
  - {name: db, postgres: {commit-every: 2m}}
  - {name: shop, mysql: {}}
`)
	require.NoError(t, err)

	sets := map[string]schedule.Expectation{}
	for _, e := range Expectations(cfg.Tasks(), time.Now()) {
		sets[e.Set] = e
	}

	require.Equal(t, schedule.Expectation{Set: "world", Kind: schedule.Every, LongestGap: 6 * time.Hour, Jitter: 10 * time.Minute}, sets["world"])
	require.Equal(t, schedule.Expectation{Set: "docs", Kind: schedule.Every, LongestGap: 24 * time.Hour}, sets["docs"], "a watched backup runs on its plan while nothing changes")
	require.Equal(t, schedule.Cron, sets["mods"].Kind)
	require.InDelta(t, float64(72*time.Hour), float64(sets["mods"].LongestGap), float64(time.Hour))
	require.Equal(t, schedule.Cron, sets["db/base"].Kind)
	require.Equal(t, schedule.Expectation{Set: "db/wal", Kind: schedule.Receiver, CommitEvery: 2 * time.Minute}, sets["db/wal"])
	require.Equal(t, schedule.Expectation{Set: "shop/binlog", Kind: schedule.Receiver, CommitEvery: time.Minute}, sets["shop/binlog"])
	require.Len(t, sets, 7)
}

// reportingIndex keeps the schedule reports it takes and when, and answers
// each with err.
type reportingIndex struct {
	*fakeIndex
	t0 time.Time

	mu      sync.Mutex
	at      []time.Duration
	reports []schedule.Report
	err     error
	// hang holds every report until its context ends.
	hang bool
}

func (r *reportingIndex) ReportSchedule(ctx context.Context, report schedule.Report) error {
	r.mu.Lock()
	r.at = append(r.at, time.Since(r.t0))
	r.reports = append(r.reports, report)
	hang, err := r.hang, r.err
	r.mu.Unlock()

	if hang {
		<-ctx.Done()
		return ctx.Err()
	}

	return err
}

func (r *reportingIndex) sent() []schedule.Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]schedule.Report(nil), r.reports...)
}

func (r *reportingIndex) times() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]time.Duration(nil), r.at...)
}

func setsOf(r schedule.Report) []string {
	var sets []string
	for _, e := range r.Sets {
		sets = append(sets, e.Set)
	}

	return sets
}

func TestTheAgentReportsItsScheduleThroughItsStoreAtStartAfterEachReloadAndDaily(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		store := &reportingIndex{fakeIndex: h.idx, t0: h.t0}
		h.a.Open = func(context.Context) (*job.Env, error) { return &job.Env{Index: store}, nil }

		a := h.fake("a", Every(time.Hour), time.Second, nil)
		h.run(a)

		time.Sleep(time.Minute)
		h.a.Reload([]*Task{a, h.fake("b", Every(2*time.Hour), time.Second, nil)})

		time.Sleep(25 * time.Hour)
		h.stop()

		require.Equal(t, []time.Duration{0, time.Minute, 24*time.Hour + time.Minute}, store.times())

		sent := store.sent()
		require.Equal(t, "a1", sent[0].Agent)
		require.Equal(t, []string{"a"}, setsOf(sent[0]))
		require.Equal(t, []string{"a", "b"}, setsOf(sent[1]))
		require.Equal(t, []string{"a", "b"}, setsOf(sent[2]))
		require.Equal(t, minutes(0, 60), h.startsOf("a")[:2])
	})
}

func TestAFailedReportNeitherFailsNorHoldsABackupAndIsSentAgainNextTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		reporter := &reportingIndex{t0: h.t0, hang: true}
		h.a.Reporter = reporter

		h.run(h.fake("a", Every(time.Hour), time.Second, nil))

		time.Sleep(2 * time.Hour)

		reporter.mu.Lock()
		reporter.hang, reporter.err = false, errors.New("the store is down")
		reporter.mu.Unlock()

		time.Sleep(23 * time.Hour)
		h.stop()

		require.Equal(t, minutes(0, 60, 120), h.startsOf("a")[:3])
		require.Equal(t, []string{ResultOK, ResultOK, ResultOK}, h.results["a"][:3])
		require.Equal(t, []time.Duration{0, 24 * time.Hour}, reporter.times())
	})
}
