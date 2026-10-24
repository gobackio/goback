package pack

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/badger"

	"github.com/stretchr/testify/require"
)

// setCommit is a commit of set 1 over tree, received at the nth second.
func setCommit(tree *proto.Ref, n int64) *proto.Object {
	return proto.NewObject(&proto.Commit{Tree: tree, Timestamp: n, BackupSet: "world", SetId: 1, ReceivedAtNs: n * int64(time.Second)})
}

// TestSeveralCommitsInOneSessionAskTheBucketWhatOneCommitAsks pins that a
// session committing several commits makes the requests a session
// committing one makes: one archive, one un-tombstone archive, one begin
// and end, the seals twice and one head.
func TestSeveralCommitsInOneSessionAskTheBucketWhatOneCommitAsks(t *testing.T) {
	counts := map[int]map[string]int{}

	for _, n := range []int{1, 5} {
		bucket := newMemBucket()
		storage := newRequestCounting(bucket)

		cache, err := badger.New(t.TempDir())
		require.NoError(t, err)

		store, err := NewPackStorage(
			WithArchiveStorage(storage),
			WithArchiveIndex(NewInMemoryIndex()),
			WithMetadataCache(cache),
			WithIndexCache(t.TempDir()),
			WithCloseBeforeRead(true),
		)
		require.NoError(t, err)
		require.NoError(t, store.Open())
		t.Cleanup(func() { _ = store.Close() })

		previous := makeChain(makeTestData(t, 10))
		ctx, _ := beginSession(t, store, "agent")
		for _, obj := range previous {
			require.NoError(t, store.Put(ctx, obj))
		}

		storage.take()

		ctx, _ = beginSession(t, store, "agent")

		var commits []*proto.Object

		for i := range n {
			// one new file beside the trees the previous commit stored
			blob := makeTestData(t, 1)
			file := makeGCFiles(blob)
			root := treeOf(append(file, previous[len(previous)-3]))

			for _, obj := range append(append(blob, file...), root) {
				require.NoError(t, store.Put(ctx, obj))
			}

			commits = append(commits, setCommit(root.Ref(), int64(10+i)))
		}

		require.NoError(t, store.PutCommits(ctx, commits))

		counts[n] = storage.take()
		t.Logf("%d commits in one session: %s", n, formatCounts(counts[n]))

		heads, err := store.Heads()
		require.NoError(t, err)
		require.Len(t, heads, 1)
		require.Equal(t, commits[n-1].Ref().Hash, heads[0].Commit, "the set's head is its newest commit")

		for _, commit := range commits {
			requirePresent(t, store, []*proto.Object{commit}, true)
		}
	}

	require.Equal(t, counts[1], counts[5])
}

func TestACommitOfSeveralMissingAnythingStoresNoneAndNamesWhatEachLacks(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	ctx, _ := beginSession(t, store, "agent")

	whole := makeChain(makeTestData(t, 5))
	for _, obj := range whole[:len(whole)-1] {
		require.NoError(t, store.Put(ctx, obj))
	}

	lacking := makeChain(makeTestData(t, 5))
	for _, obj := range lacking[1 : len(lacking)-1] {
		require.NoError(t, store.Put(ctx, obj))
	}

	commits := []*proto.Object{setCommit(lacking[len(lacking)-2].Ref(), 1), setCommit(whole[len(whole)-2].Ref(), 2)}

	var missing *backup.MissingError
	require.ErrorAs(t, store.PutCommits(ctx, commits), &missing)
	require.Len(t, missing.Commits, 2)
	require.Len(t, missing.Commits[0], 1)
	require.Equal(t, lacking[0].Ref().Hash, missing.Commits[0][0].Hash)
	require.Empty(t, missing.Commits[1])
	require.Equal(t, missing.Commits[0], missing.Refs)

	requirePresent(t, store, commits, false)

	heads, err := store.Heads()
	require.NoError(t, err)
	require.Empty(t, heads)

	require.NoError(t, store.Put(ctx, lacking[0]))
	require.NoError(t, store.PutCommits(ctx, commits))

	requirePresent(t, store, commits, true)
}

// TestACommitOfSeveralCopiesWhatACollectionCondemnedUnderWhatItReliedOn
// collects between a session deduplicating and its commits landing.
func TestACommitOfSeveralCopiesWhatACollectionCondemnedUnderWhatItReliedOn(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	first, second := unreferencedChain(t, store), unreferencedChain(t, store)

	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, first[:len(first)-1], true)
	requirePresent(t, store, second[:len(second)-1], true)

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	require.NoError(t, store.PutCommits(sctx, []*proto.Object{first[len(first)-1], second[len(second)-1]}))

	for i, ahead := range []time.Duration{48 * time.Hour, 72 * time.Hour, 96 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, append(first, second...), true)
	requirePresent(t, store, append(first, second...), true)
}

// TestACommitOfSeveralLateForWhatACollectionTookIsRefusedUntilItIsSentAgain
// lets two collections take what one of the commits relied on before they
// land: they are refused, naming it, and land once it is sent again.
func TestACommitOfSeveralLateForWhatACollectionTookIsRefusedUntilItIsSentAgain(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	relied := unreferencedChain(t, store)

	sctx, _ := beginSession(t, store, "agent-a")
	requirePresent(t, store, relied[:len(relied)-1], true)

	own := makeChain(makeTestData(t, 5))
	for _, obj := range own[:len(own)-1] {
		require.NoError(t, store.Put(sctx, obj))
	}

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	second, err := store.Collect(ctx, gcOptions(t, 48*time.Hour))
	require.NoError(t, err)
	require.NotZero(t, second.ReclaimedObjects)

	commits := []*proto.Object{relied[len(relied)-1], own[len(own)-1]}

	var missing *backup.MissingError
	require.ErrorAs(t, store.PutCommits(sctx, commits), &missing)
	require.NotEmpty(t, missing.Commits[0], "what the collection took")
	require.Empty(t, missing.Commits[1])
	requirePresent(t, store, commits, false)

	for round := 0; missing != nil; round++ {
		require.Less(t, round, 10, "the refusals name what to send")

		byRef := map[string]*proto.Object{}
		for _, obj := range relied {
			byRef[string(obj.Ref().Hash)] = obj
		}

		for _, ref := range missing.Refs {
			require.NoError(t, store.Put(sctx, byRef[string(ref.Hash)]))
		}

		missing = nil
		if err := store.PutCommits(sctx, commits); err != nil {
			require.ErrorAs(t, err, &missing)
		}
	}

	for i, ahead := range []time.Duration{72 * time.Hour, 96 * time.Hour, 120 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, append(relied, own...), true)
}

// TestADelayedCommitAsksAgainOnceItsSessionBegan follows a client that
// decided what the store holds before a collection sealed it, began its
// session after: asking again finds it absent, so it is sent anew.
func TestADelayedCommitAsksAgainOnceItsSessionBegan(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	chains := [][]*proto.Object{unreferencedChain(t, store), unreferencedChain(t, store)}
	for _, chain := range chains {
		requirePresent(t, store, chain[:len(chain)-1], true)
	}

	_, err := store.Collect(ctx, gcOptions(t, 0))
	require.NoError(t, err)

	sctx, _ := beginSession(t, store, "agent-a")

	var commits []*proto.Object

	for _, chain := range chains {
		for _, obj := range chain[:len(chain)-1] {
			has, err := store.Has(sctx, obj.Ref())
			require.NoError(t, err)
			require.False(t, has, fmt.Sprintf("%x is condemned", obj.Ref().Hash))
			require.NoError(t, store.Put(sctx, obj))
		}

		commits = append(commits, chain[len(chain)-1])
	}

	require.NoError(t, store.PutCommits(sctx, commits))

	for i, ahead := range []time.Duration{48 * time.Hour, 72 * time.Hour, 96 * time.Hour} {
		_, err := store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err, "collection %d", i)
	}

	requireStored(t, store, append(chains[0], chains[1]...), true)
}
