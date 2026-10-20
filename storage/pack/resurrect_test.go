package pack

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// unreferencedChain stores a chain without its commit, so a collection
// condemns all of it, and returns the chain with the commit last.
func unreferencedChain(t *testing.T, store *PackStorage) []*proto.Object {
	t.Helper()

	chain := makeChain(makeTestData(t, 20))
	putAll(t, store, chain[:len(chain)-1])

	return chain
}

func requireUntombed(t *testing.T, store *PackStorage, objects []*proto.Object, untombed bool) {
	t.Helper()

	for _, obj := range objects {
		untomb := proto.TombstoneRef(proto.TombstoneRef(obj.Ref()))

		found, err := store.index.LocateCopies([]*proto.Ref{untomb}, Scope{})
		require.NoError(t, err)
		require.Equalf(t, untombed, len(found[string(untomb.Hash)]) > 0, "un-tombstone of %x", obj.Ref().Hash)
	}
}

func TestACommitTakesBackItselfAndTheTombstonesOfWhatItSkipped(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 10)
	putAll(t, store, blobs)

	// stored again after its tombstone, so a session skips it with the
	// tombstone still standing
	require.NoError(t, store.Delete(ctx, blobs[0].Ref()))
	require.NoError(t, store.Flush())
	putAll(t, store, blobs[:1])

	chain := makeChain(blobs)
	own := chain[len(blobs):]
	commit := own[len(own)-1]

	sctx, _ := beginSession(t, store, "agent-a")
	for _, obj := range own {
		require.NoError(t, store.Put(sctx, obj))
	}

	requireUntombed(t, store, []*proto.Object{commit, blobs[0]}, true)
	requireUntombed(t, store, blobs[1:], false)
	requireUntombed(t, store, own[:len(own)-1], false)
}

func TestACommitThatDeduplicatedStoresItsUntombstoneInItsOwnArchive(t *testing.T) {
	base := t.TempDir()
	store := newGCStore(t, base)
	ctx := context.Background()

	blobs := makeTestData(t, 10)
	putAll(t, store, blobs)

	chain := makeChain(blobs)
	own := chain[len(blobs):]
	commit := own[len(own)-1]

	sctx, session := beginSession(t, store, "agent-a")
	for _, obj := range own {
		require.NoError(t, store.Put(sctx, obj))
	}

	var names []string
	for _, archive := range archiveFiles(t, filepath.Join(base, session.ID)) {
		names = append(names, session.ID+"/"+strings.TrimSuffix(filepath.Base(archive), ArchiveSuffix))
	}

	untomb := proto.TombstoneRef(proto.TombstoneRef(commit.Ref()))
	found, err := store.index.LocateCopies([]*proto.Ref{commit.Ref(), untomb}, Scope{})
	require.NoError(t, err)
	require.Contains(t, names, found[string(commit.Ref().Hash)][0].Archive)
	require.Contains(t, names, found[string(untomb.Hash)][0].Archive)

	requirePresent(t, store, chain, true)
	requireStored(t, store, chain, true)
	require.NoError(t, store.Close())

	rebuilt := newGCStore(t, base)
	t.Cleanup(func() { _ = rebuilt.Close() })

	requirePresent(t, rebuilt, chain, true)
	requireStored(t, rebuilt, chain, true)
	requireUntombed(t, rebuilt, []*proto.Object{commit}, true)

	_, err = rebuilt.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)
	requirePresent(t, rebuilt, chain, true)
}

func TestTheUntombstoneOfACommitDoesNotUndoItsRetirement(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 10)
	putAll(t, store, blobs)

	chain := makeChain(blobs)
	own := chain[len(blobs):]
	commit := own[len(own)-1]

	sctx, _ := beginSession(t, store, "agent-a")
	for _, obj := range own {
		require.NoError(t, store.Put(sctx, obj))
	}

	requireUntombed(t, store, []*proto.Object{commit}, true)

	require.NoError(t, store.Delete(ctx, commit.Ref()))
	require.NoError(t, store.Flush())

	for i, ahead := range []time.Duration{0, 48 * time.Hour, 96 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requirePresent(t, store, []*proto.Object{commit}, false)
	requireStored(t, store, chain, false)
}

func TestACommitCopiesWhatACollectionCondemnedUnderWhatItReliedOn(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := unreferencedChain(t, store)
	commit := chain[len(chain)-1]

	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, chain[:len(chain)-1], true)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Put(sctx, commit))

	for i, ahead := range []time.Duration{48 * time.Hour, 72 * time.Hour, 96 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, chain, true)
	requirePresent(t, store, chain, true)
}

func TestACommitFailsWhenACollectionTookWhatItReliedOn(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := unreferencedChain(t, store)
	commit := chain[len(chain)-1]

	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, chain[:len(chain)-1], true)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.NotZero(t, second.ReclaimedObjects)

	require.ErrorIs(t, store.Put(sctx, commit), backup.ErrDanglingRef, "what the collection took is missing, for the session to store again")
	requirePresent(t, store, chain[len(chain)-1:], false)
}

func TestASessionThatBeganAfterTheSealFindsWhatItCondemnsAbsent(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chain := unreferencedChain(t, store)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, chain[:len(chain)-1], false)

	putAll(t, store, chain[:len(chain)-1])
	require.NoError(t, store.Put(sctx, chain[len(chain)-1]))

	for i, ahead := range []time.Duration{48 * time.Hour, 72 * time.Hour, 96 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, chain, true)
}

func TestAnUntombstoneKeepsWhatItTakesBackOnlyWhileItStands(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, _ := retiredChain(t, store)
	blob := gone[0]

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, proto.TombstoneRef(blob.Ref())))
	require.NoError(t, store.Flush())

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, len(gone)-1, second.ReclaimedObjects)
	requireStored(t, store, gone[:1], true)

	for generation := 3; generation <= 10; generation++ {
		_, err := store.Collect(ctx, gcOptions(t, time.Duration(generation)*24*time.Hour))
		require.NoError(t, err)
	}

	requireStored(t, store, gone[:1], false)
	require.Zero(t, countTombstones(t, store))
}

func TestACollectionKeepsTheSealsOfTheSessionsThatBeganBeforeThem(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	sctx, _ := beginSession(t, store, "agent-a")

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
	}

	generations, err := store.sealGenerations()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, generations)

	require.NoError(t, store.EndSession(sctx))

	_, err = store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)

	generations, err = store.sealGenerations()
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, generations)
}

func TestACollectionFindsTheTombstonesNoNewerThanTheCopiesTheyCondemn(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	obj := makeTestData(t, 1)[0]
	require.NoError(t, store.Put(ctx, obj))
	require.NoError(t, store.Delete(ctx, obj.Ref()))
	require.NoError(t, store.Flush())

	found, err := store.index.LocateCopies([]*proto.Ref{proto.TombstoneRef(obj.Ref())}, Scope{})
	require.NoError(t, err)
	tomb, err := store.newest(found[string(proto.TombstoneRef(obj.Ref()).Hash)])
	require.NoError(t, err)

	run := &gcRun{ps: store}
	key := keyOf(obj.Ref().Hash)

	older := Version{Time: tomb.Time.Add(-time.Second)}
	stale, err := run.notNewer(map[refKey]Version{key: older})
	require.NoError(t, err)
	require.Empty(t, stale)

	stale, err = run.notNewer(map[refKey]Version{key: *tomb})
	require.NoError(t, err)
	require.Contains(t, stale, key)
}

func TestAnUntombstoneKeepsEverythingUnderWhatItTakesBack(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	gone, _ := retiredChain(t, store)
	tree := gone[len(gone)-2]

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, proto.TombstoneRef(tree.Ref())))
	require.NoError(t, store.Flush())

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, second.ReclaimedObjects, "only the retired commit")
	requireStored(t, store, gone[:len(gone)-1], true)
}
