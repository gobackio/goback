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
)

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}

	return instrument
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
