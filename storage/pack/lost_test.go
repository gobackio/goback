package pack

import (
	"context"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func storedCommits(t *testing.T, store *PackStorage) []*proto.Commit {
	t.Helper()

	var commits []*proto.Commit

	require.NoError(t, store.Walk(context.Background(), true, proto.ObjectType_COMMIT, func(obj *proto.Object) error {
		commits = append(commits, obj.GetCommit())
		return nil
	}))

	return commits
}

func node(name string, dir bool, ref *proto.Ref) *proto.TreeNode {
	stat := &proto.FileInfo{Name: []byte(name), Size: 1, Type: proto.NodeType_NODE_FILE}
	if dir {
		stat.Type = proto.NodeType_NODE_DIRECTORY
	}

	return &proto.TreeNode{Stat: stat, Ref: ref}
}

func TestACommitAskedToBeIncompleteIsStoredWithWhatTheStoreLacksMarkedLost(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	sctx, _ := beginSession(t, store, "agent")

	blobs := makeTestData(t, 2)
	kept, gone := blobs[0], blobs[1]
	require.NoError(t, store.Put(sctx, kept))

	keptFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: kept.Ref(), Length: 1}}})
	goneFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: gone.Ref(), Length: 1}}})
	sub := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{node("gone.txt", false, goneFile.Ref()), node("kept.txt", false, keptFile.Ref())}})
	root := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{node("kept.txt", false, keptFile.Ref()), node("sub", true, sub.Ref())}})

	for _, obj := range []*proto.Object{keptFile, goneFile, sub, root} {
		require.NoError(t, store.Put(sctx, obj))
	}

	commit := &proto.Commit{Timestamp: 42, Tree: root.Ref(), BackupSet: "world", Incomplete: true}
	require.NoError(t, store.Put(sctx, proto.NewObject(commit)))

	commits := storedCommits(t, store)
	require.Len(t, commits, 1)
	got := commits[0]
	require.True(t, got.Incomplete)
	require.EqualValues(t, 42, got.Timestamp)
	require.NotEqual(t, root.Ref().Hash, got.Tree.Hash)

	top, err := store.Get(context.Background(), got.Tree)
	require.NoError(t, err)
	require.True(t, top.GetTree().Incomplete)
	require.Equal(t, keptFile.Ref().Hash, top.GetTree().Nodes[0].Ref.Hash, "what the store holds is left as it was")

	below, err := store.Get(context.Background(), top.GetTree().Nodes[1].Ref)
	require.NoError(t, err)
	require.True(t, below.GetTree().Incomplete)

	lost := below.GetTree().Nodes[0]
	require.True(t, lost.Stat.Lost)
	require.Nil(t, lost.Ref)
	require.Equal(t, "gone.txt", string(lost.Stat.Name))
	require.False(t, below.GetTree().Nodes[1].Stat.Lost)
}

func TestACommitAskedToBeIncompleteThatLacksNothingIsStoredWhole(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	sctx, _ := beginSession(t, store, "agent")

	blob := makeTestData(t, 1)[0]
	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: blob.Ref(), Length: 1}}})
	root := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{node("a.txt", false, file.Ref())}})

	for _, obj := range []*proto.Object{blob, file, root} {
		require.NoError(t, store.Put(sctx, obj))
	}

	require.NoError(t, store.Put(sctx, proto.NewObject(&proto.Commit{Timestamp: 1, Tree: root.Ref(), BackupSet: "world", Incomplete: true})))

	commits := storedCommits(t, store)
	require.Len(t, commits, 1)
	require.False(t, commits[0].Incomplete)
	require.Equal(t, root.Ref().Hash, commits[0].Tree.Hash)
}

func TestACommitWithoutTheFlagIsStillRefusedForWhatItLacks(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	sctx, _ := beginSession(t, store, "agent")

	gone := makeTestData(t, 1)[0]
	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: gone.Ref(), Length: 1}}})
	require.NoError(t, store.Put(sctx, file))

	var missing *backup.MissingError
	require.ErrorAs(t, store.Put(sctx, commitOver(file.Ref())), &missing)
}

func TestWhatAnIncompleteCommitLeftAboveItsGapsIsNotTakenAsWholeByALaterCommit(t *testing.T) {
	store := newGCStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	blobs := makeTestData(t, 1)
	gone := blobs[0]
	goneFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: gone.Ref(), Length: 1}}})
	root := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{node("gone.txt", false, goneFile.Ref())}})

	sctx, _ := beginSession(t, store, "agent")
	require.NoError(t, store.Put(sctx, goneFile))
	require.NoError(t, store.Put(sctx, root))
	require.NoError(t, store.Put(sctx, proto.NewObject(&proto.Commit{Timestamp: 1, Tree: root.Ref(), BackupSet: "world", Incomplete: true})))

	sctx, _ = beginSession(t, store, "agent")
	err := store.Put(sctx, proto.NewObject(&proto.Commit{Timestamp: 2, Tree: root.Ref(), BackupSet: "world"}))

	var missing *backup.MissingError
	require.ErrorAs(t, err, &missing, "the tree is held, but not whole")
	require.Len(t, missing.Refs, 1)
	require.Equal(t, gone.Ref().Hash, missing.Refs[0].Hash)

	again := proto.NewObject(&proto.Commit{Timestamp: 2, Tree: root.Ref(), BackupSet: "world", Incomplete: true})
	require.NoError(t, store.Put(sctx, again))
	require.True(t, again.GetCommit().Incomplete, "it is marked lost again")

	sctx, _ = beginSession(t, store, "agent")
	require.NoError(t, store.Put(sctx, gone))
	require.NoError(t, store.Put(sctx, proto.NewObject(&proto.Commit{Timestamp: 3, Tree: root.Ref(), BackupSet: "world"})), "sent again, it is whole")

	found, err := store.index.LocateCopies([]*proto.Ref{root.Ref(), goneFile.Ref()}, Scope{})
	require.NoError(t, err)

	for _, copies := range found {
		require.False(t, incomplete(copies), "a check that found it whole clears the flag")
	}
}
