// Package maintenance runs a store's housekeeping on a schedule. The
// store only exposes the operations; whoever owns the store, a server
// with one or a host with many, decides when they run.
package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"
)

// Store is what the runner sweeps and compacts.
type Store interface {
	Sweep(now time.Time)
	Compact(ctx context.Context) (*pack.CompactReport, error)
}

// Presence is an index that builds presence filters on demand.
type Presence interface {
	BuildPendingPresence(ctx context.Context) (int, error)
}

// Measurer is an index that stores the sizes of the sets whose commits
// changed, and reports the sets it measured.
type Measurer interface {
	MeasureSets(ctx context.Context) ([]index.SetInfo, error)
}

// Attributor is an index that names the set behind each root and keeps
// what a collection attributed to every set.
type Attributor interface {
	RootOwner(ctx context.Context) (func(root []byte) pack.Attribution, error)
	RecordSetSizes(ctx context.Context, report *pack.CollectReport) error
}

// Forecaster is an index that expects when each root stops being one, so
// a collection writes what dies about the same time together.
type Forecaster interface {
	RootDeath(ctx context.Context, now time.Time) (func(root []byte) time.Time, error)
}

// Reindexer is an index that rebuilds the indexes of its tables a
// maintenance run churned, and reports the tables it rebuilt.
type Reindexer interface {
	ReindexChurned(ctx context.Context) ([]index.Reindexed, error)
}

// Collected is a store that knows when it was last garbage collected.
type Collected interface {
	LastCollected() (time.Time, error)
}

// Schedule is how often each job runs; zero never runs it.
type Schedule struct {
	Sweep   time.Duration
	Compact time.Duration
	Collect time.Duration
	Retire  time.Duration
	// Presence also measures the sets whose commits changed.
	Presence time.Duration
}

// DefaultSchedule is how often a store server runs each job unless told
// otherwise.
var DefaultSchedule = Schedule{
	Sweep:    30 * time.Second,
	Compact:  time.Hour,
	Collect:  7 * 24 * time.Hour,
	Retire:   time.Hour,
	Presence: 30 * time.Second,
}

// Collect garbage collects the store with opts. When idx is an Attributor,
// what survives is grouped by the set that owns it and what each set takes
// up is recorded; when it is a Forecaster, by when it is expected to die;
// when it is a Reindexer, the indexes the collection churned are rebuilt.
// idx may be nil.
func Collect(ctx context.Context, collector pack.Collector, idx backup.Index, opts pack.CollectOptions) (*pack.CollectReport, error) {
	attributor, _ := idx.(Attributor)
	forecaster, _ := idx.(Forecaster)
	reindexer, _ := idx.(Reindexer)

	return collect(ctx, collector, attributor, forecaster, reindexer, opts)
}

func collect(ctx context.Context, collector pack.Collector, attributor Attributor, forecaster Forecaster, reindexer Reindexer, opts pack.CollectOptions) (*pack.CollectReport, error) {
	report, err := attributed(ctx, collector, attributor, forecaster, opts)

	return report, errors.Join(err, reindex(ctx, reindexer))
}

func attributed(ctx context.Context, collector pack.Collector, attributor Attributor, forecaster Forecaster, opts pack.CollectOptions) (*pack.CollectReport, error) {
	if forecaster != nil {
		now := opts.Now
		if now.IsZero() {
			now = time.Now()
		}

		death, err := forecaster.RootDeath(ctx, now)
		if err != nil {
			return nil, err
		}

		opts.Death = death
	}

	if attributor != nil {
		owner, err := attributor.RootOwner(ctx)
		if err != nil {
			return nil, err
		}

		opts.Owner = owner
	}

	report, err := collector.Collect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if attributor != nil && report.Waiting == 0 {
		if err := attributor.RecordSetSizes(ctx, report); err != nil {
			return report, err
		}
	}

	return report, nil
}

func reindex(ctx context.Context, reindexer Reindexer) error {
	if reindexer == nil {
		return nil
	}

	_, err := reindexer.ReindexChurned(ctx)

	return err
}

// NewRunner is the runner of the jobs store and idx support, found by
// what the innermost store and the index implement.
func NewRunner(store backup.ObjectStore, idx backup.Index, schedule Schedule) *Runner {
	r := &Runner{Schedule: schedule}

	base := wrapped.Innermost(store)
	r.Store, _ = base.(Store)
	r.Collector, _ = base.(pack.Collector)
	r.Retirer, _ = idx.(backup.Retirer)
	r.Presence, _ = idx.(Presence)
	r.Measurer, _ = idx.(Measurer)
	r.Attributor, _ = idx.(Attributor)
	r.Forecaster, _ = idx.(Forecaster)
	r.Reindexer, _ = idx.(Reindexer)

	return r
}

// Runner ticks the jobs of one store. A nil member skips its jobs.
type Runner struct {
	Store      Store
	Collector  pack.Collector
	Attributor Attributor
	Forecaster Forecaster
	Retirer    backup.Retirer
	Presence   Presence
	Measurer   Measurer
	Reindexer  Reindexer
	Schedule   Schedule
	// OnCollect sees every garbage collection report; nil ignores them.
	OnCollect func(*pack.CollectReport)
	// Logger is where failures go; nil means slog.Default.
	Logger *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (r *Runner) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}

	return slog.Default()
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// Run sweeps once, then runs every scheduled job at its interval until ctx
// ends. Jobs of one runner never overlap.
func (r *Runner) Run(ctx context.Context) {
	r.Sweep()

	jobs := []struct {
		every time.Duration
		run   func(context.Context)
	}{
		{r.Schedule.Sweep, func(context.Context) { r.Sweep() }},
		{r.Schedule.Compact, r.logCompact},
		{r.Schedule.Collect, r.logCollect},
		{r.Schedule.Retire, r.logRetire},
		{r.Schedule.Presence, func(ctx context.Context) { r.BuildPresence(ctx); r.MeasureSets(ctx) }},
	}

	var tickers []*time.Ticker
	cases := make([]<-chan time.Time, len(jobs))
	for i, job := range jobs {
		if job.every <= 0 {
			continue
		}

		t := time.NewTicker(job.every)
		tickers = append(tickers, t)
		cases[i] = t.C
	}

	defer func() {
		for _, t := range tickers {
			t.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cases[0]:
			jobs[0].run(ctx)
		case <-cases[1]:
			jobs[1].run(ctx)
		case <-cases[2]:
			jobs[2].run(ctx)
		case <-cases[3]:
			jobs[3].run(ctx)
		case <-cases[4]:
			jobs[4].run(ctx)
		}
	}
}

// Ran is what one pass of Due did.
type Ran struct {
	Swept             bool
	Retired, Presence int
	// Compacted is nil when no compaction ran; alongside a failed one it
	// counts what that finished.
	Compacted *pack.CompactReport
	// Measured is how many sets had their sizes measured.
	Measured int
	// Collected is nil when no collection was due.
	Collected *pack.CollectReport
	// Reindexed are the tables whose indexes the run's churn rebuilt.
	Reindexed []index.Reindexed
}

// Due runs every scheduled job once, except a collection, which runs only
// when the store's last one is at least Schedule.Collect old, and then
// reindexes what they churned. It is what a cron job calls; errors are
// joined and every job still runs.
func (r *Runner) Due(ctx context.Context) (Ran, error) {
	var ran Ran
	var errs []error

	if r.Store != nil && r.Schedule.Sweep > 0 {
		r.Store.Sweep(r.now())
		ran.Swept = true
	}

	if r.Retirer != nil && r.Schedule.Retire > 0 {
		n, err := r.Retirer.Retire(ctx, r.now())
		ran.Retired = n
		errs = append(errs, err)
	}

	if r.Store != nil && r.Schedule.Compact > 0 {
		report, err := r.Store.Compact(ctx)
		ran.Compacted = report
		errs = append(errs, err)
	}

	if r.Presence != nil && r.Schedule.Presence > 0 {
		n, err := r.Presence.BuildPendingPresence(ctx)
		ran.Presence = n
		errs = append(errs, err)
	}

	if r.Measurer != nil && r.Schedule.Presence > 0 {
		measured, err := r.Measurer.MeasureSets(ctx)
		ran.Measured = len(measured)
		errs = append(errs, err)
	}

	due, err := r.collectDue()
	errs = append(errs, err)

	if due {
		ran.Collected, err = attributed(ctx, r.Collector, r.Attributor, r.Forecaster, pack.CollectOptions{})
		errs = append(errs, err)
	}

	if r.Reindexer != nil {
		ran.Reindexed, err = r.Reindexer.ReindexChurned(ctx)
		errs = append(errs, err)
	}

	return ran, errors.Join(errs...)
}

func (r *Runner) collectDue() (bool, error) {
	if r.Collector == nil || r.Schedule.Collect <= 0 {
		return false, nil
	}

	collected, ok := r.Collector.(Collected)
	if !ok {
		return true, nil
	}

	last, err := collected.LastCollected()
	if err != nil {
		return false, err
	}

	return last.IsZero() || r.now().Sub(last) >= r.Schedule.Collect, nil
}

// Sweep finalizes idle archives and ends expired sessions.
func (r *Runner) Sweep() {
	if r.Store != nil {
		r.Store.Sweep(r.now())
	}
}

// Compact rewrites small archives and rebuilds the indexes that churned.
// Without a Store it does nothing.
func (r *Runner) Compact(ctx context.Context) (*pack.CompactReport, error) {
	if r.Store == nil {
		return &pack.CompactReport{}, nil
	}

	report, err := r.Store.Compact(ctx)

	return report, errors.Join(err, reindex(ctx, r.Reindexer))
}

func (r *Runner) logCompact(ctx context.Context) {
	if _, err := r.Compact(ctx); err != nil {
		r.logger().Error("compaction failed", "err", err)
	}
}

// Collect garbage collects the store, hands the report to OnCollect and
// rebuilds the indexes that churned. Without a Collector it returns nil.
func (r *Runner) Collect(ctx context.Context) (*pack.CollectReport, error) {
	if r.Collector == nil {
		return nil, nil
	}

	report, err := collect(ctx, r.Collector, r.Attributor, r.Forecaster, r.Reindexer, pack.CollectOptions{})
	if report != nil && r.OnCollect != nil {
		r.OnCollect(report)
	}

	return report, err
}

func (r *Runner) logCollect(ctx context.Context) {
	if _, err := r.Collect(ctx); err != nil {
		r.logger().Error("garbage collection failed", "err", err)
	}
}

// BuildPresence builds the presence filters that are missing.
func (r *Runner) BuildPresence(ctx context.Context) {
	if r.Presence == nil {
		return
	}

	if _, err := r.Presence.BuildPendingPresence(ctx); err != nil {
		r.logger().Error("building presence filters failed", "err", err)
	}
}

// MeasureSets measures the sets whose commits changed.
func (r *Runner) MeasureSets(ctx context.Context) {
	if r.Measurer == nil {
		return
	}

	if _, err := r.Measurer.MeasureSets(ctx); err != nil {
		r.logger().Error("measuring set sizes failed", "err", err)
	}
}

// Retire tombstones every retired commit past its window and rebuilds the
// indexes that churned. Without a Retirer it does nothing.
func (r *Runner) Retire(ctx context.Context) (int, error) {
	if r.Retirer == nil {
		return 0, nil
	}

	n, err := r.Retirer.Retire(ctx, r.now())

	return n, errors.Join(err, reindex(ctx, r.Reindexer))
}

func (r *Runner) logRetire(ctx context.Context) {
	n, err := r.Retire(ctx)
	if err != nil {
		r.logger().Error("retirement failed", "retired", n, "err", err)
		return
	}

	if n > 0 {
		r.logger().Info("retired commits", "count", n)
	}
}
