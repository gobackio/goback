package storage_test

import (
	"context"
	"io"
	"testing"

	"github.com/gobackio/goback/storage"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

// reader collects what the counters recorded. The global meter provider
// takes only the first one set, so the whole binary shares this; each
// case counts under a store name of its own instead.
var reader = func() *metric.ManualReader {
	reader := metric.NewManualReader()
	otel.SetMeterProvider(metric.NewMeterProvider(metric.WithReader(reader)))

	return reader
}()

// counted is what one store sent to the object store while fn ran: the
// number of requests of each operation, and the bytes they carried.
func counted(t *testing.T, name string, fn func(*storage.BucketStore)) (map[string]int64, map[string]int64) {
	t.Helper()

	bucket, err := blob.OpenBucket(context.Background(), "mem://")
	require.NoError(t, err)

	defer bucket.Close()

	return countedOn(t, name, bucket, fn)
}

// countedOn is counted over bucket.
func countedOn(t *testing.T, name string, bucket *blob.Bucket, fn func(*storage.BucketStore)) (map[string]int64, map[string]int64) {
	t.Helper()

	fn(storage.NewBucketStore(bucket, storage.WithStoreName(name)))

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &collected))

	requests := map[string]int64{}
	bytes := map[string]int64{}

	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sums, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}

			for _, point := range sums.DataPoints {
				store, _ := point.Attributes.Value(attribute.Key("store"))
				if store.AsString() != name {
					continue
				}

				op, _ := point.Attributes.Value(attribute.Key("operation"))

				switch m.Name {
				case "goback.storage.bucket.requests":
					requests[op.AsString()] += point.Value
				case "goback.storage.bucket.bytes":
					bytes[op.AsString()] += point.Value
				}
			}
		}
	}

	return requests, bytes
}

func write(t *testing.T, store *storage.BucketStore, name string, content []byte) {
	t.Helper()

	file, err := store.Create(name)
	require.NoError(t, err)

	_, err = file.Write(content)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestEveryRequestToTheObjectStoreIsCounted(t *testing.T) {
	requests, bytes := counted(t, "one", func(store *storage.BucketStore) {
		write(t, store, "a.archive", []byte("hello"))

		file, err := store.Open("a.archive")
		require.NoError(t, err)

		buf := make([]byte, 5)
		_, err = file.Read(buf)
		require.NoError(t, err)

		_, err = store.List(".archive")
		require.NoError(t, err)

		require.NoError(t, store.Delete("a.archive"))
	})

	require.Equal(t, int64(1), requests["put"])
	require.Equal(t, int64(1), requests["head"], "opening a file reads its attributes")
	require.Equal(t, int64(1), requests["get"])
	require.Equal(t, int64(1), requests["list"])
	require.Equal(t, int64(1), requests["delete"])

	require.Equal(t, int64(5), bytes["put"])
	require.Equal(t, int64(5), bytes["get"])
}

func TestAFailedRequestIsCountedToo(t *testing.T) {
	requests, _ := counted(t, "two", func(store *storage.BucketStore) {
		_, err := store.Open("missing.archive")
		require.Error(t, err, "the object store charges for a miss")
	})

	require.Equal(t, int64(1), requests["head"])
}

func TestAListingIsCountedByThePage(t *testing.T) {
	requests, _ := counted(t, "three", func(store *storage.BucketStore) {
		for i := range 1200 {
			write(t, store, string(rune('a'+i%26))+string(rune('a'+i/26))+".archive", []byte("x"))
		}

		names, err := store.List(".archive")
		require.NoError(t, err)
		require.Len(t, names, 1200)
	})

	require.Equal(t, int64(2), requests["list"], "1200 keys is two pages of a thousand")
}

func TestReadingAWholeFileCountsTheBytesItCarried(t *testing.T) {
	_, bytes := counted(t, "four", func(store *storage.BucketStore) {
		write(t, store, "b.archive", []byte("0123456789"))

		file, err := store.Open("b.archive")
		require.NoError(t, err)

		to, ok := file.(io.WriterTo)
		require.True(t, ok)

		_, err = to.WriteTo(io.Discard)
		require.NoError(t, err)
	})

	require.Equal(t, int64(10), bytes["get"])
}

func TestSequentialReadsShareOneRequestUntilASeek(t *testing.T) {
	content := make([]byte, 10000)
	for i := range content {
		content[i] = byte(i)
	}

	requests, bytes := counted(t, "five", func(store *storage.BucketStore) {
		write(t, store, "c.archive", content)

		file, err := store.Open("c.archive")
		require.NoError(t, err)
		defer file.Close()

		got := make([]byte, 0, len(content))
		buf := make([]byte, 333)
		for {
			n, err := file.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.Equal(t, content, got)

		_, err = file.Seek(5000, io.SeekStart)
		require.NoError(t, err)

		tail, err := io.ReadAll(file)
		require.NoError(t, err)
		require.Equal(t, content[5000:], tail)
	})

	require.Equal(t, int64(2), requests["get"], "one request for the whole file, one after the seek")
	require.Equal(t, int64(15000), bytes["get"])
}

func TestOpeningAListedFileAsksTheObjectStoreNothing(t *testing.T) {
	requests, _ := counted(t, "six", func(store *storage.BucketStore) {
		write(t, store, "d.archive", []byte("0123456789"))

		listed, err := store.ListInfo(".archive")
		require.NoError(t, err)
		require.Len(t, listed, 1)

		file, err := store.OpenListed(listed[0])
		require.NoError(t, err)

		info, err := file.Stat()
		require.NoError(t, err)
		require.EqualValues(t, 10, info.Size())

		buf := make([]byte, 4)
		_, err = file.(io.ReaderAt).ReadAt(buf, 6)
		require.NoError(t, err)
		require.Equal(t, "6789", string(buf))
	})

	require.Zero(t, requests["head"])
	require.Equal(t, int64(1), requests["get"])
}
