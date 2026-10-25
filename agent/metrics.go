package agent

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("goback.io/agent")

	runsTotal   = mustInstrument(meter.Int64Counter("goback.schedule.runs", metric.WithDescription("runs of a job by result")))
	runDuration = mustInstrument(meter.Float64Histogram("goback.schedule.run", metric.WithDescription("duration of a job's runs"), metric.WithUnit("s")))
	lastSuccess = mustInstrument(meter.Float64Gauge("goback.schedule.last_success_timestamp", metric.WithDescription("when a job's last clean run started, in Unix seconds"), metric.WithUnit("s")))
	lastDirty   = mustInstrument(meter.Float64Gauge("goback.schedule.last_dirty_timestamp", metric.WithDescription("when a job's last dirty run, which left out torn or unreadable files, started, in Unix seconds"), metric.WithUnit("s")))

	watchFollowing = mustInstrument(meter.Int64Gauge("goback.watch.following", metric.WithDescription("whether a watched job's directory is followed for changes, 1 or 0, by backend")))
	watchUnwatched = mustInstrument(meter.Int64Gauge("goback.watch.unwatched", metric.WithDescription("subtrees of a watched job's directory that can't be followed and are read on every run")))
	watchRuns      = mustInstrument(meter.Int64Counter("goback.watch.runs", metric.WithDescription("runs of a watched job by what they read: changed for only what changed, else why they read everything")))
)

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}

	return instrument
}

// recordWatching records how a watched job's directory is followed.
func recordWatching(job string, s WatchStatus) {
	ctx := context.Background()
	jobAttr := attribute.String("job", job)

	following := int64(0)
	if s.Following {
		following = 1
	}

	watchFollowing.Record(ctx, following, metric.WithAttributes(jobAttr, attribute.String("backend", s.Backend)))
	watchUnwatched.Record(ctx, int64(len(s.Unwatched)), metric.WithAttributes(jobAttr))
}

// recordWatchedRun records what a run of a watched job read: full is why
// it read everything, empty when it read only what changed.
func recordWatchedRun(job, full string) {
	read := full
	if read == "" {
		read = "changed"
	}

	watchRuns.Add(context.Background(), 1, metric.WithAttributes(attribute.String("job", job), attribute.String("read", read)))
}

// recordRun records a run of job that took d and ended with result;
// started is when it started.
func recordRun(job, result string, started time.Time, d time.Duration) {
	ctx := context.Background()
	jobAttr := attribute.String("job", job)

	runsTotal.Add(ctx, 1, metric.WithAttributes(jobAttr, attribute.String("result", result)))

	if d > 0 {
		runDuration.Record(ctx, d.Seconds(), metric.WithAttributes(jobAttr))
	}

	switch result {
	case ResultOK:
		lastSuccess.Record(ctx, float64(started.UnixNano())/1e9, metric.WithAttributes(jobAttr))
	case ResultDirty:
		lastDirty.Record(ctx, float64(started.UnixNano())/1e9, metric.WithAttributes(jobAttr))
	}
}
