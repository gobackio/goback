package pack

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "goback.io/storage/pack"

var (
	tracer = otel.Tracer(instrumentationName)
	meter  = otel.Meter(instrumentationName)

	keyObjectType = attribute.Key("object_type")

	bytesBuckets        = metric.WithExplicitBucketBoundaries(0, 256, 512, 1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216)
	millisecondsBuckets = metric.WithExplicitBucketBoundaries(0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000)

	getObjectSize       = mustInstrument(meter.Int64Histogram("goback.storage.pack.get_object_size", metric.WithDescription("size of objects retrieved"), metric.WithUnit("By"), bytesBuckets))
	putObjectSize       = mustInstrument(meter.Int64Histogram("goback.storage.pack.put_object_size", metric.WithDescription("size of objects stored"), metric.WithUnit("By"), bytesBuckets))
	archiveReadSize     = mustInstrument(meter.Int64Histogram("goback.storage.pack.archive_read_size", metric.WithDescription("size of archive reads"), metric.WithUnit("By"), bytesBuckets))
	archiveWriteSize    = mustInstrument(meter.Int64Histogram("goback.storage.pack.archive_write_size", metric.WithDescription("size of archive writes"), metric.WithUnit("By"), bytesBuckets))
	archiveReadLatency  = mustInstrument(meter.Float64Histogram("goback.storage.pack.archive_read_latency", metric.WithDescription("duration of archive reads"), metric.WithUnit("ms"), millisecondsBuckets))
	archiveWriteLatency = mustInstrument(meter.Float64Histogram("goback.storage.pack.archive_write_latency", metric.WithDescription("duration of archive writes"), metric.WithUnit("ms"), millisecondsBuckets))

	gcMarkDuration   = mustInstrument(meter.Float64Histogram("goback.storage.pack.gc.mark_duration", metric.WithDescription("duration of the gc mark"), metric.WithUnit("s")))
	gcMergeDuration  = mustInstrument(meter.Float64Histogram("goback.storage.pack.gc.merge_duration", metric.WithDescription("duration of the gc merge"), metric.WithUnit("s")))
	gcSweepDuration  = mustInstrument(meter.Float64Histogram("goback.storage.pack.gc.sweep_duration", metric.WithDescription("duration of the gc sweep"), metric.WithUnit("s")))
	gcDeadBytes      = mustInstrument(meter.Int64Gauge("goback.storage.pack.gc.dead_bytes", metric.WithDescription("bytes the last generation found unreachable"), metric.WithUnit("By")))
	gcReclaimedBytes = mustInstrument(meter.Int64Counter("goback.storage.pack.gc.reclaimed_bytes", metric.WithDescription("bytes dropped by gc sweeps"), metric.WithUnit("By")))

	rewriteArchives = mustInstrument(meter.Int64Counter("goback.storage.pack.rewrite.archives", metric.WithDescription("archives a sweep or compaction rewrote")))
	rewriteCopied   = mustInstrument(meter.Int64Counter("goback.storage.pack.rewrite.copied_bytes", metric.WithDescription("bytes a sweep or compaction carried into new archives"), metric.WithUnit("By")))
	rewriteDuration = mustInstrument(meter.Float64Histogram("goback.storage.pack.rewrite.archive_duration", metric.WithDescription("duration of rewriting one archive"), metric.WithUnit("s")))

	hasAsked = mustInstrument(meter.Int64Counter("goback.storage.pack.has.asked", metric.WithDescription("refs the store was asked whether it holds")))
	hasHeld  = mustInstrument(meter.Int64Counter("goback.storage.pack.has.held", metric.WithDescription("refs asked about that the store holds")))
)

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}

	return instrument
}
