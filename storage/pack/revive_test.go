package pack

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestARevivedCommitKeepsWhatACollectionCondemnedUnderIt(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, kept := retiredChain(t, store)
	commit := gone[len(gone)-1]

	first, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	require.EqualValues(t, len(gone)-1, first.Condemned)
	requirePresent(t, store, gone, false)

	revivals, err := store.Revive(ctx, []*proto.Ref{commit.Ref()}, false)
	require.NoError(t, err)
	require.Len(t, revivals, 1)
	require.True(t, revivals[0].Whole())
	requirePresent(t, store, gone, true)

	for i, ahead := range []time.Duration{48 * time.Hour, 96 * time.Hour, 144 * time.Hour} {
		report, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
		require.Empty(t, report.SweepSkipped, "collection %d", i)
	}

	requireStored(t, store, gone, true)
	requireStored(t, store, kept, true)
	requirePresent(t, store, gone, true)
}

func TestACompactionKeepsTheRevivalOfACommitASessionStored(t *testing.T) {
	store := newTestStore(t, t.TempDir(), WithMaxSize(256*1024), WithCompaction(CompactionConfig{MinimumCandidates: 0}))
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 20))
	commit := chain[len(chain)-1]

	session, _ := beginSession(t, store, "agent")
	for _, obj := range chain {
		require.NoError(t, store.Put(session, obj))
	}

	require.NoError(t, store.Delete(ctx, commit.Ref()))
	require.NoError(t, store.Flush())

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	revivals, err := store.Revive(ctx, []*proto.Ref{commit.Ref()}, false)
	require.NoError(t, err)
	require.True(t, revivals[0].Whole())

	compact(t, ctx, store)

	revived, err := store.Revived(ctx, commit.Ref())
	require.NoError(t, err)
	require.True(t, revived)

	for i, ahead := range []time.Duration{48 * time.Hour, 96 * time.Hour, 144 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, chain, true)
	requirePresent(t, store, chain, true)
}

func TestARevivalTakesNothingBackUnderACommitMissingAnObject(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 10))
	lost, commit := chain[0], chain[len(chain)-1]
	putAll(t, store, chain[1:])

	whole := makeChain(makeTestData(t, 10))
	putAll(t, store, whole)

	for _, c := range []*proto.Object{commit, whole[len(whole)-1]} {
		require.NoError(t, store.Delete(ctx, c.Ref()))
	}
	require.NoError(t, store.Flush())
	tombstones := countTombstones(t, store)

	dry, err := store.Revive(ctx, []*proto.Ref{commit.Ref(), whole[len(whole)-1].Ref()}, true)
	require.NoError(t, err)
	require.Equal(t, 1, dry[0].MissingCount)
	require.True(t, bytes.Equal(lost.Ref().Hash, dry[0].Missing[0].Hash))
	require.True(t, dry[1].Whole())
	require.Equal(t, tombstones, countTombstones(t, store), "a dry run writes nothing")

	revivals, err := store.Revive(ctx, []*proto.Ref{commit.Ref(), whole[len(whole)-1].Ref()}, false)
	require.NoError(t, err)
	require.Equal(t, dry, revivals)

	requireUntombed(t, store, []*proto.Object{commit}, false)
	requireUntombed(t, store, whole[len(whole)-1:], true)
	requirePresent(t, store, []*proto.Object{commit}, false)
	requirePresent(t, store, whole[len(whole)-1:], true)
}

func TestARevivalRefusesWhatIsNoCommit(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	chain := makeChain(makeTestData(t, 4))
	putAll(t, store, chain)

	_, err := store.Revive(context.Background(), []*proto.Ref{chain[0].Ref()}, true)
	require.ErrorContains(t, err, "not a commit")
}

func TestHoldsSeesATombstonedCommitButNoUnknownOne(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := makeChain(makeTestData(t, 3))
	putAll(t, store, chain)
	commit := chain[len(chain)-1]

	require.NoError(t, store.Delete(ctx, commit.Ref()))
	require.NoError(t, store.Flush())

	held, err := store.Holds(ctx, []*proto.Ref{commit.Ref(), makeChain(makeTestData(t, 1))[0].Ref()})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, held)
}
