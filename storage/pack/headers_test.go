package pack

import (
	"context"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestWalkHeadersSeesTombstones(t *testing.T) {
	store, err := NewPackStorage(
		WithArchiveStorage(newLocal(t.TempDir())),
		WithArchiveIndex(NewInMemoryIndex()),
	)
	require.NoError(t, err)

	ctx := context.Background()
	objects := makeTestData(t, 3)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}

	gone := objects[1].Ref()
	require.NoError(t, store.Delete(ctx, gone))
	require.NoError(t, store.Flush())

	var tombstones []*proto.Ref
	err = store.WalkHeaders(ctx, proto.ObjectType_TOMBSTONE, func(hdr *proto.ObjectHeader) error {
		require.Equal(t, proto.ObjectType_TOMBSTONE, hdr.Type)
		tombstones = append(tombstones, hdr.TombstoneFor)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, tombstones, 1)
	require.True(t, tombstones[0].Equal(gone))

	all := 0
	err = store.WalkHeaders(ctx, proto.ObjectType_INVALID, func(*proto.ObjectHeader) error {
		all++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(objects)+1, all)

	require.NoError(t, store.Close())
}

func TestATypedWalkReadsOnlyTheObjectsOfItsType(t *testing.T) {
	storage := &countingStorage{localArchiveStorage: newLocal(t.TempDir())}
	store, err := NewPackStorage(
		WithArchiveStorage(storage),
		WithArchiveIndex(NewInMemoryIndex()),
		WithAtRestKey(atRestKey(t)),
	)
	require.NoError(t, err)

	ctx := context.Background()
	objects := makeTestData(t, 200)
	for _, obj := range objects {
		require.NoError(t, store.Put(ctx, obj))
	}

	var pins []*proto.Object
	for i := range 3 {
		pin := proto.NewObject(&proto.Pin{Target: objects[i].Ref(), ReceivedAtNs: int64(i + 1)})
		require.NoError(t, store.Put(ctx, pin))
		pins = append(pins, pin)
	}

	require.NoError(t, store.Delete(ctx, objects[10].Ref()))
	require.NoError(t, store.Flush())

	storage.mu.Lock()
	storage.bytes = 0
	storage.mu.Unlock()

	var want, walked []string
	for _, pin := range pins {
		want = append(want, string(plain(pin)))
	}

	err = store.Walk(ctx, true, proto.ObjectType_PIN, func(obj *proto.Object) error {
		walked = append(walked, string(plain(obj)))
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, want, walked)

	var unpinned []*proto.Ref
	err = store.WalkHeaders(ctx, proto.ObjectType_TOMBSTONE, func(hdr *proto.ObjectHeader) error {
		unpinned = append(unpinned, hdr.TombstoneFor)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, unpinned, 1)
	require.True(t, unpinned[0].Equal(objects[10].Ref()))

	var stored int64
	for _, obj := range objects {
		stored += int64(len(obj.Bytes()))
	}

	storage.mu.Lock()
	read := storage.bytes
	storage.mu.Unlock()
	require.Less(t, read, stored/10, "the walks read the pins and the tombstone, not the archive")

	require.NoError(t, store.Close())
}
