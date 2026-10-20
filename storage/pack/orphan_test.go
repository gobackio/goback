package pack

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestAnotherSessionsUntombstonesVouchForNothingUntilItCommits(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blobs := makeTestData(t, 2)
	x, y := blobs[0], blobs[1]
	putAll(t, store, blobs)

	keeper := commitOver(x.Ref())
	putAll(t, store, []*proto.Object{keeper})

	// a live backup, so every mark has roots to walk
	putAll(t, store, makeChain(makeTestData(t, 2)))

	failing, lost := beginSession(t, store, "agent-c")
	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: y.Ref(), Length: 1}, {Ref: x.Ref(), Offset: 1, Length: 1}}})
	require.NoError(t, store.Put(failing, file))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, keeper.Ref()))
	require.NoError(t, store.Flush())

	_, err = store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	requireStored(t, store, []*proto.Object{y}, false)
	requireStored(t, store, []*proto.Object{x}, true)

	var relied *proto.Object

	gcAfterBatch = func(int) error {
		gcAfterBatch = nil

		// it finds y gone before it takes anything back
		var missing *backup.MissingError
		require.ErrorAs(t, store.Put(failing, commitOver(file.Ref())), &missing)
		require.Len(t, missing.Refs, 1)
		require.Equal(t, y.Ref().Hash, missing.Refs[0].Hash)
		_, err := store.LookupSession(ctx, lost.ID)
		require.NoError(t, err, "a session missing what it relied on may store it again")

		relying, _ := beginSession(t, store, "agent-b")
		has, err := store.Has(relying, x.Ref())
		require.NoError(t, err)
		require.False(t, has, "x is condemned, and the un-tombstone that took it back never committed")

		require.NoError(t, store.Put(relying, x))
		own := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: x.Ref(), Length: 2}}})
		require.NoError(t, store.Put(relying, own))
		relied = commitOver(own.Ref())

		return store.Put(relying, relied)
	}
	t.Cleanup(func() { gcAfterBatch = nil })

	_, err = store.Collect(ctx, gcOptions(t, 72*time.Hour))
	require.NoError(t, err)

	requireStored(t, store, []*proto.Object{x, relied}, true)
}

func TestATombstoneASessionStoresCountsOnceItCommits(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	blobs := makeTestData(t, 2)
	obj, other := blobs[0], blobs[1]
	putAll(t, store, blobs)

	sctx, _ := beginSession(t, store, "agent-a")
	require.NoError(t, store.Delete(sctx, obj.Ref()))
	requirePresent(t, store, []*proto.Object{obj}, true)

	require.NoError(t, store.Put(sctx, commitOver(other.Ref())))

	requirePresent(t, store, []*proto.Object{obj}, false)
}

func TestMarksKeepWhatAPendingUntombstoneTakesBack(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	x := makeTestData(t, 1)[0]
	keeper := commitOver(x.Ref())
	putAll(t, store, []*proto.Object{x, keeper})
	putAll(t, store, makeChain(makeTestData(t, 2)))

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, keeper.Ref()))
	require.NoError(t, store.Flush())

	sctx, _ := beginSession(t, store, "agent-a")
	own := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: x.Ref(), Length: 1}}})
	require.NoError(t, store.Put(sctx, own))

	committed := make(chan struct{})
	collected := make(chan error, 1)

	// the session has read the seals; two collections run before it commits
	commitAfterResurrect = func() {
		commitAfterResurrect = nil

		_, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
		require.NoError(t, err)

		marking := make(chan struct{})
		gcAfterBatch = func(int) error {
			gcAfterBatch = nil
			close(marking)
			<-committed

			return nil
		}

		go func() {
			_, err := store.Collect(ctx, gcOptions(t, 72*time.Hour))
			collected <- err
		}()

		<-marking
	}
	t.Cleanup(func() { commitAfterResurrect, gcAfterBatch = nil, nil })

	relied := commitOver(own.Ref())
	require.NoError(t, store.Put(sctx, relied))
	close(committed)

	require.NoError(t, <-collected)
	requireStored(t, store, []*proto.Object{x, own, relied}, true)
}
