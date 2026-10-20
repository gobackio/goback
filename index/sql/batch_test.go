package sql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// lookups is a memStore that answers HasAll and counts the calls.
type lookups struct {
	*memStore
	calls atomic.Int64
}

func (l *lookups) HasAll(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	l.calls.Add(1)

	held := make([]bool, len(refs))
	for i, ref := range refs {
		held[i], _ = l.memStore.Has(ctx, ref)
	}

	return held, nil
}

func TestPutObjectsChecksWhatTheyReferenceInOneLookup(t *testing.T) {
	store := &lookups{memStore: newMemStore()}
	x := openMemory(t, store)
	ctx := context.Background()

	held := proto.NewObject(&proto.Blob{Data: []byte("held")})
	require.NoError(t, x.Put(ctx, held))

	var (
		objects []*proto.Object
		refs    []*proto.Ref
	)

	for i := range 50 {
		part := proto.NewObject(&proto.Blob{Data: []byte(fmt.Sprint(i))})
		file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: part.Ref(), Length: 1}, {Ref: held.Ref(), Offset: 1, Length: 4}}})
		objects = append(objects, file, part)
		refs = append(refs, file.Ref(), part.Ref())
	}

	store.calls.Store(0)
	require.NoError(t, x.PutObjects(ctx, objects), "a file may come before its part")
	require.EqualValues(t, 1, store.calls.Load())

	missing, err := x.Missing(ctx, refs)
	require.NoError(t, err)
	require.Empty(t, missing)

	absent := proto.NewObject(&proto.Blob{Data: []byte("absent")})
	dangling := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: absent.Ref(), Length: 6}}})
	require.ErrorIs(t, x.PutObjects(ctx, []*proto.Object{dangling}), backup.ErrDanglingRef)

	has, err := store.Has(ctx, dangling.Ref())
	require.NoError(t, err)
	require.False(t, has, "nothing of a refused batch is stored")
}
