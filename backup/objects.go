package backup

import (
	"context"
	"sync"

	"github.com/gobackio/goback/proto"
)

// recordChunk is how many records ReadObjects asks a RecordReader for at
// once.
const recordChunk = 10_000

// getWorkers is how many objects ReadObjects gets at once that no record
// read returned.
const getWorkers = 32

// ReadObjects reads the object of each of refs, with the error of each it
// could not read. Where store, or a store it wraps, is a RecordReader it
// reads them recordChunk at a time, a range read per run of neighbours,
// and gets only the rest one by one, through store.
func ReadObjects(ctx context.Context, store Getter, refs []*proto.Ref) ([]*proto.Object, []error) {
	objects := make([]*proto.Object, len(refs))
	errs := make([]error, len(refs))

	if reader, ok := recordReader(store); ok {
		for start := 0; start < len(refs); start += recordChunk {
			end := min(start+recordChunk, len(refs))

			if read, err := reader.ReadRecords(ctx, refs[start:end]); err == nil {
				copy(objects[start:end], read)
			}
		}
	}

	var gets sync.WaitGroup
	slots := make(chan struct{}, getWorkers)

	for i, ref := range refs {
		if objects[i] != nil {
			continue
		}

		slots <- struct{}{}
		gets.Go(func() {
			defer func() { <-slots }()

			objects[i], errs[i] = store.Get(ctx, ref)
		})
	}

	gets.Wait()

	return objects, errs
}

// recordReader finds a RecordReader in store or the stores it wraps.
func recordReader(store Getter) (RecordReader, bool) {
	for store != nil {
		if reader, ok := store.(RecordReader); ok {
			return reader, true
		}

		wrapper, ok := store.(interface{ Unwrap() ObjectStore })
		if !ok {
			return nil, false
		}

		store = wrapper.Unwrap()
	}

	return nil, false
}

// readObjects is ReadObjects failing with the first error.
func readObjects(ctx context.Context, store Getter, refs []*proto.Ref) ([]*proto.Object, error) {
	objects, errs := ReadObjects(ctx, store, refs)
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return objects, nil
}
