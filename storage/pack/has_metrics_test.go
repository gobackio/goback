package pack

import (
	"context"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// reader collects what the counters recorded; the global meter provider
// takes only the first one set, so the whole binary shares it.
var reader = func() *metric.ManualReader {
	reader := metric.NewManualReader()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))

	return reader
}()

func counter(t *testing.T, name string, caller backup.Lookup) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			var total int64
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if v, _ := dp.Attributes.Value(keyCaller); v.AsString() == string(caller) {
					total += dp.Value
				}
			}

			return total
		}
	}

	return 0
}

func TestHasAllCountsTheRefsAskedAndHeldByCaller(t *testing.T) {
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	ctx := backup.WithLookup(context.Background(), backup.LookupMissing)
	objs := makeTestData(t, 3)

	for _, obj := range objs[:2] {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Flush())

	asked, held := counter(t, "goback.storage.pack.has.asked", backup.LookupMissing), counter(t, "goback.storage.pack.has.held", backup.LookupMissing)

	_, err := store.HasAll(ctx, []*proto.Ref{objs[0].Ref(), objs[1].Ref(), objs[2].Ref()})
	require.NoError(t, err)

	require.Equal(t, asked+3, counter(t, "goback.storage.pack.has.asked", backup.LookupMissing))
	require.Equal(t, held+2, counter(t, "goback.storage.pack.has.held", backup.LookupMissing))
}
