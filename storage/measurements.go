package storage

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "goback.io/storage"

// Operation is a request a store makes to the object store holding it.
// Object stores bill by request, and each charges these differently, so
// they are counted apart and priced by whoever pays the bill.
type Operation string

const (
	// OpGet reads an object or a range of one.
	OpGet Operation = "get"
	// OpHead reads an object's attributes without its body.
	OpHead Operation = "head"
	// OpPut writes an object.
	OpPut Operation = "put"
	// OpList reads one page of a listing.
	OpList Operation = "list"
	// OpDelete removes an object.
	OpDelete Operation = "delete"
	// OpCopy copies an object within the bucket.
	OpCopy Operation = "copy"
)

var (
	meter = otel.Meter(instrumentationName)

	keyOperation = attribute.Key("operation")
	keyStore     = attribute.Key("store")

	bucketRequests = mustInstrument(meter.Int64Counter(
		"goback.storage.bucket.requests",
		metric.WithDescription("requests a store made to the object store"),
	))
	bucketBytes = mustInstrument(meter.Int64Counter(
		"goback.storage.bucket.bytes",
		metric.WithDescription("bytes those requests carried"),
		metric.WithUnit("By"),
	))
)

func mustInstrument[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}

	return instrument
}

// count records one request and the bytes it carried, also in the
// tallies ctx carries. A failed request counts too, because the object
// store charges for it either way.
func (c *BucketStore) count(ctx context.Context, op Operation, bytes int64) {
	attrs := metric.WithAttributes(append(c.attrs, keyOperation.String(string(op)))...)

	bucketRequests.Add(ctx, 1, attrs)

	if bytes > 0 {
		bucketBytes.Add(ctx, bytes, attrs)
	}

	for _, t := range talliesOf(ctx) {
		t.add(op, 1, bytes)
	}
}

// carried records bytes a request already counted carried since.
func (c *BucketStore) carried(op Operation, bytes int64) {
	if bytes > 0 {
		bucketBytes.Add(context.Background(), bytes, metric.WithAttributes(append(c.attrs, keyOperation.String(string(op)))...))
	}
}
