package pack

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gobackio/goback/proto"
)

// A file a session took back while its tombstone stood, and that the
// newest commit then stops reaching, is condemned again: the collection's
// own new tombstone outranks the old un-tombstone, in the run and in the
// check before the sweep alike.
func TestAStaleUntombstoneOfAFileLetsTheFileGo(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	blob := makeTestData(t, 1)[0]
	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: blob.Ref(), Length: uint64(len(blob.GetBlob().GetData())) + 1}}})
	whole := proto.NewObject(&proto.Commit{Tree: treeOf([]*proto.Object{file}).Ref(), Timestamp: 2, BackupSet: "s"})

	empty := proto.NewObject(&proto.Tree{})
	first := proto.NewObject(&proto.Commit{Tree: empty.Ref(), Timestamp: 4, BackupSet: "s"})
	last := proto.NewObject(&proto.Commit{Tree: empty.Ref(), Timestamp: 5, BackupSet: "s"})

	// what keeps the un-tombstone's archive from being rewritten
	live := makeTestData(t, 4)
	liveFiles := makeGCFiles(live)
	liveTree := treeOf(liveFiles)
	liveCommit := proto.NewObject(&proto.Commit{Tree: liveTree.Ref(), Timestamp: 9, BackupSet: "keep"})

	marker := proto.NewObject(&proto.Blob{Data: []byte("tree2")})
	markerFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: marker.Ref(), Length: 6}}})
	tree2 := treeOf([]*proto.Object{file, markerFile})
	holding := proto.NewObject(&proto.Commit{Tree: tree2.Ref(), Timestamp: 3, BackupSet: "s"})

	shallow := map[string]bool{string(holding.Ref().Hash): true, string(first.Ref().Hash): true, string(last.Ref().Hash): true}
	collect := func(hours int) (*CollectReport, error) {
		opts := gcOptions(t, time.Duration(hours)*time.Hour)
		opts.DeadRatio = 0.25
		opts.Behind = true
		opts.Shallow = func(ref []byte) bool { return shallow[string(ref)] }

		return store.Collect(ctx, opts)
	}

	putAll(t, store, []*proto.Object{blob, file, treeOf([]*proto.Object{file}), whole})
	_, err := collect(0)
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, whole.Ref()))
	putAll(t, store, []*proto.Object{empty, first})
	_, err = collect(48)
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, proto.TombstoneRef(file.Ref())))
	for _, obj := range append(append(append([]*proto.Object{}, live...), liveFiles...), liveTree, liveCommit) {
		require.NoError(t, store.Put(ctx, obj))
	}
	require.NoError(t, store.Flush())
	require.NoError(t, store.Delete(ctx, first.Ref()))
	putAll(t, store, []*proto.Object{marker, markerFile, tree2, holding})

	for _, hours := range []int{96, 144, 192} {
		_, err = collect(hours)
		require.NoError(t, err)
	}

	require.NoError(t, store.Delete(ctx, holding.Ref()))
	putAll(t, store, []*proto.Object{last})

	for _, hours := range []int{240, 288, 336, 384} {
		_, err = collect(hours)
		require.NoError(t, err)
	}

	found, err := store.index.LocateCopies([]*proto.Ref{blob.Ref(), file.Ref()}, Scope{})
	require.NoError(t, err)
	require.Empty(t, found[string(blob.Ref().Hash)])
	require.Empty(t, found[string(file.Ref().Hash)])
}
