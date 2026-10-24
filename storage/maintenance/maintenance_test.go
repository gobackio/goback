package maintenance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	sweeps, compactions atomic.Int32
	lastSweep           atomic.Value
}

func (f *fakeStore) Sweep(now time.Time) { f.sweeps.Add(1); f.lastSweep.Store(now) }
func (f *fakeStore) Compact(context.Context) (*pack.CompactReport, error) {
	f.compactions.Add(1)
	return &pack.CompactReport{Candidates: 3}, errors.New("disk full")
}

type fakeCollector struct{ runs atomic.Int32 }

func (f *fakeCollector) Collect(context.Context, pack.CollectOptions) (*pack.CollectReport, error) {
	f.runs.Add(1)
	return &pack.CollectReport{Objects: 7}, nil
}

type fakePresence struct{ runs atomic.Int32 }

func (f *fakePresence) BuildPendingPresence(context.Context) (int, error) {
	f.runs.Add(1)
	return 1, nil
}

type fakeRetirer struct{ runs atomic.Int32 }

func (f *fakeRetirer) Retire(context.Context, time.Time) (int, error) {
	f.runs.Add(1)
	return 2, nil
}

type fakeReindexer struct{ runs atomic.Int32 }

func (f *fakeReindexer) ReindexChurned(context.Context) ([]index.Reindexed, error) {
	f.runs.Add(1)
	return []index.Reindexed{{Table: "objects"}}, nil
}

func TestRunnerTicksEveryScheduledJob(t *testing.T) {
	store, collector, retirer, presence, reindexer := &fakeStore{}, &fakeCollector{}, &fakeRetirer{}, &fakePresence{}, &fakeReindexer{}
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var reports atomic.Int32

	r := &Runner{
		Store:     store,
		Collector: collector,
		Retirer:   retirer,
		Presence:  presence,
		Reindexer: reindexer,
		Schedule:  Schedule{Sweep: 10 * time.Millisecond, Compact: 10 * time.Millisecond, Collect: 10 * time.Millisecond, Presence: 10 * time.Millisecond},
		OnCollect: func(report *pack.CollectReport) { reports.Add(int32(report.Objects)) },
		Now:       func() time.Time { return now },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	require.GreaterOrEqual(t, store.sweeps.Load(), int32(2), "one sweep at start, then on the ticker")
	require.Equal(t, now, store.lastSweep.Load())
	require.GreaterOrEqual(t, store.compactions.Load(), int32(1), "a failing compaction is logged and retried")
	require.GreaterOrEqual(t, collector.runs.Load(), int32(1))
	require.Equal(t, reports.Load(), 7*collector.runs.Load())
	require.GreaterOrEqual(t, presence.runs.Load(), int32(1))
	require.Zero(t, retirer.runs.Load(), "retirement was not scheduled")
	require.Equal(t, collector.runs.Load()+store.compactions.Load(), reindexer.runs.Load(), "every compaction and collection is followed by a reindex")
}

func TestRunnerSkipsWhatItDoesNotHave(t *testing.T) {
	r := &Runner{Schedule: Schedule{Sweep: time.Millisecond, Compact: time.Millisecond, Collect: time.Millisecond, Retire: time.Millisecond, Presence: time.Millisecond}}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	retirer := &fakeRetirer{}
	r.Retirer = retirer
	r.Retire(context.Background())
	require.Equal(t, int32(1), retirer.runs.Load())
}

type lastCollected struct {
	fakeCollector
	at time.Time
}

func (l *lastCollected) LastCollected() (time.Time, error) { return l.at, nil }

type fakeAttributor struct{ recorded atomic.Int32 }

func (f *fakeAttributor) RootOwner(context.Context) (func([]byte) pack.Attribution, error) {
	return func([]byte) pack.Attribution { return pack.Attribution{Set: 1} }, nil
}

func (f *fakeAttributor) RecordSetSizes(context.Context, *pack.CollectReport) error {
	f.recorded.Add(1)
	return nil
}

func TestDueCollectsOnlyOnceTheLastCollectionIsOldEnough(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	collector := &lastCollected{at: now.Add(-6 * 24 * time.Hour)}
	retirer, presence, attributor := &fakeRetirer{}, &fakePresence{}, &fakeAttributor{}

	r := &Runner{Collector: collector, Retirer: retirer, Presence: presence, Attributor: attributor, Schedule: DefaultSchedule, Now: func() time.Time { return now }}

	ran, err := r.Due(context.Background())
	require.NoError(t, err)
	require.Nil(t, ran.Collected, "six days since the last collection, a week between them")
	require.Equal(t, 2, ran.Retired)
	require.Equal(t, 1, ran.Presence)

	collector.at = now.Add(-7 * 24 * time.Hour)
	ran, err = r.Due(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ran.Collected)
	require.Equal(t, int32(1), collector.runs.Load())
	require.Equal(t, int32(1), attributor.recorded.Load(), "what each set takes up is recorded")
}

func TestDueRunsEveryJobPastAFailingOne(t *testing.T) {
	store, retirer, reindexer := &fakeStore{}, &fakeRetirer{}, &fakeReindexer{}
	r := &Runner{Store: store, Retirer: retirer, Reindexer: reindexer, Schedule: DefaultSchedule}

	ran, err := r.Due(context.Background())
	require.ErrorContains(t, err, "disk full")
	require.True(t, ran.Swept)
	require.Equal(t, 3, ran.Compacted.Candidates, "a failed compaction still reports what it found")
	require.Equal(t, 2, ran.Retired)
	require.Len(t, ran.Reindexed, 1, "what the jobs churned is reindexed after them")
}

type fakeArchives struct {
	backup.ObjectStore
	fakeStore
	fakeCollector
}

type wrapping struct{ backup.ObjectStore }

func (w wrapping) Unwrap() backup.ObjectStore { return w.ObjectStore }

type fakeIndex struct {
	backup.Index
	fakeAttributor
	fakeReindexer
	fakeRetirer
}

func TestNewRunnerFindsTheJobsAndEachReindexesAfter(t *testing.T) {
	ctx := context.Background()
	archives, x := &fakeArchives{}, &fakeIndex{}

	r := NewRunner(wrapping{archives}, x, DefaultSchedule)
	require.NotNil(t, r.Store, "found beneath the wrapping store")
	require.NotNil(t, r.Collector)
	require.Nil(t, r.Presence)

	n, err := r.Retire(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, int32(1), x.fakeReindexer.runs.Load())

	_, err = r.Compact(ctx)
	require.ErrorContains(t, err, "disk full")
	require.Equal(t, int32(2), x.fakeReindexer.runs.Load(), "a failed compaction may still have churned")

	_, err = Collect(ctx, archives, x, pack.CollectOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(1), x.recorded.Load())
	require.Equal(t, int32(3), x.fakeReindexer.runs.Load())
}

type fakeForecaster struct{ asked time.Time }

func (f *fakeForecaster) RootDeath(_ context.Context, now time.Time) (func([]byte) time.Time, error) {
	f.asked = now
	return func([]byte) time.Time { return now.Add(time.Hour) }, nil
}

type optsCollector struct{ opts pack.CollectOptions }

func (c *optsCollector) Collect(_ context.Context, opts pack.CollectOptions) (*pack.CollectReport, error) {
	c.opts = opts
	return &pack.CollectReport{}, nil
}

type forecastingIndex struct {
	backup.Index
	fakeForecaster
}

func TestACollectionExpectsTheDeathsTheIndexForecasts(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	collector, x := &optsCollector{}, &forecastingIndex{}
	_, err := Collect(ctx, collector, x, pack.CollectOptions{Now: now})
	require.NoError(t, err)
	require.Equal(t, now, x.asked, "the index forecasts as of the collection's now")
	require.NotNil(t, collector.opts.Death)
	require.Equal(t, now.Add(time.Hour), collector.opts.Death(nil))

	r := NewRunner(&fakeArchives{}, x, DefaultSchedule)
	require.NotNil(t, r.Forecaster)

	collector.opts = pack.CollectOptions{}
	r.Collector = collector
	_, err = r.Collect(ctx)
	require.NoError(t, err)
	require.NotNil(t, collector.opts.Death)
}

type fakeRepairer struct {
	backup.ObjectStore
	lost []*proto.Ref
}

func (f *fakeRepairer) Repair(context.Context) (*pack.RepairReport, error) {
	return &pack.RepairReport{Lost: f.lost}, nil
}

func TestRepairFindsTheRepairerBeneathWrappingStores(t *testing.T) {
	ctx := context.Background()

	_, _, err := Repair(ctx, &fakeArchives{}, &fakeIndex{})
	require.ErrorIs(t, err, errors.ErrUnsupported)

	lost := []*proto.Ref{{Hash: []byte{1}}}
	report, damage, err := Repair(ctx, wrapping{&fakeRepairer{lost: lost}}, &fakeIndex{})
	require.NoError(t, err)
	require.Equal(t, lost, report.Lost)
	require.Nil(t, damage, "an index that records no damage records none")
}
