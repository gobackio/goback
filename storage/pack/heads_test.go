package pack

import (
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestASetsHeadIsItsLatestCompleteCommit(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	tree := makeTestData(t, 1)[0]
	putAll(t, store, []*proto.Object{tree})

	commit := func(set uint64, received int64, partial bool) *proto.Object {
		obj := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world", Partial: partial})
		obj.Stamp(set, time.Unix(0, received))
		putAll(t, store, []*proto.Object{obj})

		return obj
	}

	first := commit(1, 10, false)
	other := commit(2, 15, false)
	commit(1, 20, true)
	commit(1, 5, false)

	heads, err := store.Heads()
	require.NoError(t, err)
	require.ElementsMatch(t, []Head{
		{SetID: 1, Set: "world", Commit: first.Ref().Hash, ReceivedAtNs: 10},
		{SetID: 2, Set: "world", Commit: other.Ref().Hash, ReceivedAtNs: 15},
	}, heads)

	latest := commit(1, 30, false)

	heads, err = store.Heads()
	require.NoError(t, err)
	require.Contains(t, heads, Head{SetID: 1, Set: "world", Commit: latest.Ref().Hash, ReceivedAtNs: 30})
}

func TestAHeadThatDoesNotDecodeIsLeftOutAndReplaced(t *testing.T) {
	dir := t.TempDir()
	store := newGCStore(t, dir)

	tree := makeTestData(t, 1)[0]
	putAll(t, store, []*proto.Object{tree})

	commit := func(store *PackStorage, set uint64, received int64) *proto.Object {
		obj := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world"})
		obj.Stamp(set, time.Unix(0, received))
		putAll(t, store, []*proto.Object{obj})

		return obj
	}

	kept := commit(store, 1, 10)
	commit(store, 2, 10)

	file, err := store.storage.Create(headName(2))
	require.NoError(t, err)
	_, err = file.Write([]byte(`{"setId":`))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	heads, err := store.Heads()
	require.NoError(t, err)
	require.Equal(t, []Head{{SetID: 1, Set: "world", Commit: kept.Ref().Hash, ReceivedAtNs: 10}}, heads)

	require.NoError(t, store.Close())

	store = newGCStore(t, dir)
	t.Cleanup(func() { _ = store.Close() })

	replaced := commit(store, 2, 5)

	heads, err = store.Heads()
	require.NoError(t, err)
	require.Contains(t, heads, Head{SetID: 2, Set: "world", Commit: replaced.Ref().Hash, ReceivedAtNs: 5})
}

func TestAStoreKeepsHeadsOnlyForTheSetsItIsToldTo(t *testing.T) {
	base := t.TempDir()
	tree := makeTestData(t, 1)[0]

	commit := func(store *PackStorage, set uint64, received int64) *proto.Object {
		obj := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world"})
		obj.Stamp(set, time.Unix(0, received))
		putAll(t, store, []*proto.Object{obj})

		return obj
	}

	before := newGCStore(t, base)
	putAll(t, before, []*proto.Object{tree})
	commit(before, 2, 10)
	require.NoError(t, before.Close())

	store := closing(t, newTestStore(t, base, WithHeadsFor(func(set uint64) bool { return set == 1 })))
	kept := commit(store, 1, 20)
	commit(store, 2, 30)

	heads, err := store.Heads()
	require.NoError(t, err)
	require.Equal(t, []Head{{SetID: 1, Set: "world", Commit: kept.Ref().Hash, ReceivedAtNs: 20}}, heads,
		"the head the other set had is gone rather than left behind")
}

func TestAStoreReadsAHeadOnlyForASetItDoesNotRemember(t *testing.T) {
	storage := newRequestCounting(newMemBucket())
	store, err := NewPackStorage(WithArchiveStorage(storage), WithArchiveIndex(NewInMemoryIndex()))
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	tree := makeTestData(t, 1)[0]
	putAll(t, store, []*proto.Object{tree})
	storage.take()

	var latest *proto.Object
	for _, received := range []int64{10, 20, 15, 30} {
		latest = proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "world"})
		latest.Stamp(1, time.Unix(0, received))
		putAll(t, store, []*proto.Object{latest})
	}

	counts := storage.take()
	require.Equal(t, 1, counts["get .head"], formatCounts(counts))
	require.Equal(t, 3, counts["put .head"], formatCounts(counts))

	heads, err := store.Heads()
	require.NoError(t, err)
	require.Equal(t, []Head{{SetID: 1, Set: "world", Commit: latest.Ref().Hash, ReceivedAtNs: 30}}, heads)
}
