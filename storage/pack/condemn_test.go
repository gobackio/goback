package pack

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/bits-and-blooms/bitset"

	"github.com/stretchr/testify/require"
)

// retiredChain stores two chains and retires the first's commit, returning
// the retired chain and the kept one.
func retiredChain(t *testing.T, store *PackStorage) ([]*proto.Object, []*proto.Object) {
	t.Helper()

	blobs := makeTestData(t, 40)
	gone := makeChain(blobs[:20])
	kept := makeChain(blobs[20:])
	putAll(t, store, append(append([]*proto.Object{}, gone...), kept...))

	require.NoError(t, store.Delete(context.Background(), gone[len(gone)-1].Ref()))
	require.NoError(t, store.Flush())

	return gone, kept
}

func TestCollectKeepsACopyNewerThanItsTombstone(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, _ := retiredChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	blob := gone[0]
	requirePresent(t, store, gone[:1], false)
	require.NoError(t, store.Put(ctx, blob))
	require.NoError(t, store.Flush())
	requirePresent(t, store, gone[:1], true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone), second.ReclaimedObjects, "only the copy older than the tombstone goes")
	require.EqualValues(t, 1, second.Condemned, "the newer copy is condemned anew")
	requireStored(t, store, gone[:1], true)
	requirePresent(t, store, gone[:1], false)
}

func TestCollectKeepsWhatAnUntombstoneTookBack(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, _ := retiredChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	blob := gone[0].Ref()
	require.NoError(t, store.Delete(ctx, proto.TombstoneRef(blob)))
	require.NoError(t, store.Flush())
	requirePresent(t, store, gone[:1], true)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone)-1, second.ReclaimedObjects)
	requireStored(t, store, gone[:1], true)
}

func TestCollectDropsOnlyWhatATombstoneStoredBeforeTheHorizonCondemns(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 20)
	chain := makeChain(blobs)
	putAll(t, store, chain)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, chain[len(chain)-1].Ref()))
	require.NoError(t, store.Flush())

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.Zero(t, second.ReclaimedObjects, "the retire came after the previous horizon")
	require.EqualValues(t, len(chain)-1, second.Condemned)

	third, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(chain), third.ReclaimedObjects)
	requireStored(t, store, chain, false)
}

func TestCollectDropsOnlyCopiesOlderThanTheTombstone(t *testing.T) {
	created := time.Now().Truncate(time.Microsecond)
	tomb := Version{Time: created, Offset: 10}
	sum := keyOf(makeRef().Hash)

	run := &gcRun{tombs: newTombTargets([]refKey{sum})}
	run.tombs.tombstoned(0, stampOf(tomb), true)
	ga := &gcArchive{a: &archive{created: created}}
	unmarked := &gcFile{Previous: bitset.New(1), Current: bitset.New(1)}

	older := &IndexRecord{Sum: sum, Offset: 9, Type: uint32(proto.ObjectType_BLOB)}
	newer := &IndexRecord{Sum: sum, Offset: 11, Type: uint32(proto.ObjectType_BLOB)}

	require.True(t, run.droppable(ga, unmarked, 0, older))
	require.False(t, run.droppable(ga, unmarked, 0, newer))
}

func TestASessionBegunAfterASealTheProcessWroteBeginsAtIt(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.writeSeal(seal{Generation: 7}))

	s := &backup.Session{}
	_, err := store.BeginSession(context.Background(), s)
	require.NoError(t, err)

	begin, err := store.markers.SessionBegin(s.ID)
	require.NoError(t, err)
	require.EqualValues(t, 7, begin.Sealed)
}
