package sql

import (
	"context"
	"crypto/sha256"
	"runtime"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// localIndex is an index as the CLI opens it: commits stored through the
// index itself, everything kept until a policy is set.
type localIndex struct {
	t   *testing.T
	ctx context.Context
	x   *Index
}

func newLocalIndex(t *testing.T) *localIndex {
	t.Helper()

	x := openIndex(t, newMemStore())
	x.DefaultPolicy = &retention.KeepAll

	return &localIndex{t: t, ctx: auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "local"}), x: x}
}

// commit writes a commit with one file; a non-zero receivedAt is the
// index clock at receipt.
func (l *localIndex) commit(set, name string, timestamp, receivedAt int64, content string) *proto.Ref {
	l.t.Helper()

	if receivedAt != 0 {
		l.x.Now = func() time.Time { return time.Unix(0, receivedAt) }
		defer func() { l.x.Now = nil }()
	}

	blob := proto.NewObject(&proto.Blob{Data: []byte(content)})
	require.NoError(l.t, l.x.Put(l.ctx, blob))

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: blob.Ref(), Length: uint64(len(content))}}})
	require.NoError(l.t, l.x.Put(l.ctx, file))

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
		Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, MtimeNs: timestamp, Size: int64(len(content)), Mode: 0644},
		Ref:  file.Ref(),
	}}})
	require.NoError(l.t, l.x.Put(l.ctx, tree))

	commit := proto.NewObject(&proto.Commit{Timestamp: timestamp, Tree: tree.Ref(), BackupSet: set})
	require.NoError(l.t, l.x.Put(l.ctx, commit))
	_, err := l.x.BuildPendingPresence(l.ctx)
	require.NoError(l.t, err)

	return commit.Ref()
}

func TestSameSecondCommitsAreKept(t *testing.T) {
	l := newLocalIndex(t)

	now := time.Now().Unix()
	l.commit("a", "world.dat", now, 0, "first")
	l.commit("a", "world.dat", now, 0, "second")
	l.commit("b", "world.dat", now, 0, "other set")

	commits, err := l.x.CommitInfo(l.ctx, "a", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 2)

	files, err := l.x.FileInfo(l.ctx, "a", "world.dat", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, files, 2)

	files, err = l.x.FileInfo(l.ctx, "b", "world.dat", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, int64(len("other set")), files[0].Stat.Size)
}

func TestLatestCommitFollowsReceiptOrder(t *testing.T) {
	l := newLocalIndex(t)

	_, err := l.x.LatestCommit(l.ctx, "a")
	require.ErrorIs(t, err, backup.ErrNotFound)

	l.commit("a", "world.dat", 30, 100e9, "received first")
	l.commit("a", "world.dat", 20, 200e9, "agent clock ahead")
	l.commit("a", "world.dat", 10, 300e9, "received last")
	l.commit("b", "world.dat", 30, 0, "other set")

	ref, err := l.x.LatestCommit(l.ctx, "a")
	require.NoError(t, err)

	obj, err := l.x.Get(l.ctx, ref)
	require.NoError(t, err)
	require.EqualValues(t, 10, obj.GetCommit().Timestamp, "receipt order wins over the agent clock")
	require.EqualValues(t, 300e9, obj.GetCommit().ReceivedAtNs)

	commits, err := l.x.CommitInfo(l.ctx, "a", time.Unix(1000, 0), 10)
	require.NoError(t, err)
	require.Len(t, commits, 3)
	require.EqualValues(t, []int64{300e9, 200e9, 100e9}, []int64{commits[0].ReceivedAtNs, commits[1].ReceivedAtNs, commits[2].ReceivedAtNs})
}

func TestPutStampsReceiptTime(t *testing.T) {
	l := newLocalIndex(t)

	before := time.Now().UnixNano()
	l.commit("a", "world.dat", 1, 0, "x")

	ref, err := l.x.LatestCommit(l.ctx, "a")
	require.NoError(t, err)

	obj, err := l.x.Get(l.ctx, ref)
	require.NoError(t, err)
	require.GreaterOrEqual(t, obj.GetCommit().ReceivedAtNs, before-int64(time.Microsecond), "the index stamps the receipt time")
	require.NotZero(t, obj.GetCommit().SetId, "and the set id")
	require.True(t, obj.Ref().Equal(ref), "the stored ref covers the stamp")

	commits, err := l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 1)
	require.Equal(t, obj.GetCommit().ReceivedAtNs, commits[0].ReceivedAtNs, "the stamp survives the round trip")
}

// slowCommitStore holds the Put of a commit with timestamp 1 until
// release is closed, after closing stored.
type slowCommitStore struct {
	*memStore
	stored, release chan struct{}
}

func (s *slowCommitStore) Put(ctx context.Context, obj *proto.Object) error {
	if obj.GetCommit().GetTimestamp() == 1 {
		close(s.stored)
		<-s.release
	}

	return s.memStore.Put(ctx, obj)
}

// TestPutIndexesOrRefusesACommitStoredAfterANewerOne races two commits of
// one set: the one received first is stored last.
func TestPutIndexesOrRefusesACommitStoredAfterANewerOne(t *testing.T) {
	store := &slowCommitStore{memStore: newMemStore(), stored: make(chan struct{}), release: make(chan struct{})}
	x := openIndex(t, store)
	x.DefaultPolicy = &retention.KeepAll
	ctx := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "local"})

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, x.Put(ctx, tree))

	slow := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree.Ref(), BackupSet: "a"})
	done := make(chan error, 1)
	go func() { done <- x.Put(ctx, slow) }()

	<-store.stored
	require.NoError(t, x.Put(ctx, proto.NewObject(&proto.Commit{Timestamp: 2, Tree: tree.Ref(), BackupSet: "a"})))
	close(store.release)

	if err := <-done; err == nil {
		_, err = x.GetCommitDetail(ctx, "a", slow.Ref())
		require.NoError(t, err, "a commit Put took is indexed")
	}
}

func TestPutRejectsDanglingCommit(t *testing.T) {
	l := newLocalIndex(t)

	sum := sha256.Sum256([]byte("nowhere"))
	commit := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: &proto.Ref{Hash: sum[:]}, BackupSet: "a"})
	err := l.x.Put(l.ctx, commit)
	require.ErrorIs(t, err, backup.ErrDanglingRef)

	_, err = l.x.LatestCommit(l.ctx, "a")
	require.ErrorIs(t, err, backup.ErrNotFound)

	// trees and files are checked on Put as well, so a stored tree always
	// has its children
	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
		Stat: &proto.FileInfo{Name: []byte("gone.dat"), Size: 1, Mode: 0644},
		Ref:  &proto.Ref{Hash: sum[:]},
	}}})
	require.ErrorIs(t, l.x.Put(l.ctx, tree), backup.ErrDanglingRef)

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: &proto.Ref{Hash: sum[:]}, Length: 1}}})
	require.ErrorIs(t, l.x.Put(l.ctx, file), backup.ErrDanglingRef)

	ok, err := l.x.ObjectStore.Has(l.ctx, file.Ref())
	require.NoError(t, err)
	require.False(t, ok, "a rejected object must not be stored")
}

// TestFileInfoOrdersVersionsByBackupTime backs up a version whose mtime is
// older than the previous version's and expects it to come first anyway,
// and to be absent as of a time before it was backed up.
func TestFileInfoOrdersVersionsByBackupTime(t *testing.T) {
	l := newLocalIndex(t)

	l.commit("a", "world.dat", 100, 1e9, "first")
	l.commit("a", "world.dat", 50, 2e9, "reverted")

	files, err := l.x.FileInfo(l.ctx, "a", "world.dat", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.EqualValues(t, len("reverted"), files[0].Stat.Size, "the last backup comes first")
	require.EqualValues(t, 50, files[0].Stat.MtimeNs)

	files, err = l.x.FileInfo(l.ctx, "a", "world.dat", time.Unix(0, 1.5e9), 10)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.EqualValues(t, len("first"), files[0].Stat.Size)
}

func TestCommitInfoSkipsCheckpoints(t *testing.T) {
	l := newLocalIndex(t)

	l.commit("a", "world.dat", 1, 0, "complete")

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, l.x.Put(l.ctx, tree))
	checkpoint := proto.NewObject(&proto.Commit{Timestamp: 2, Tree: tree.Ref(), BackupSet: "a", Partial: true})
	require.NoError(t, l.x.Put(l.ctx, checkpoint))

	commits, err := l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 1)
	require.EqualValues(t, 1, commits[0].Timestamp)

	latest, err := l.x.LatestCommit(l.ctx, "a")
	require.NoError(t, err)
	require.True(t, latest.Equal(checkpoint.Ref()), "the walker resumes from the checkpoint")
}

// TestIndexesASplitRoot stores a root tree that is only splits and expects
// the files under them to be indexed.
func TestIndexesASplitRoot(t *testing.T) {
	l := newLocalIndex(t)

	file := proto.NewObject(&proto.File{Inline: []byte("x")})
	require.NoError(t, l.x.Put(l.ctx, file))

	var splits []*proto.Ref
	for _, name := range []string{"a.dat", "b.dat"} {
		sub := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
			Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, MtimeNs: 1, Size: 1, Mode: 0644},
			Ref:  file.Ref(),
		}}})
		require.NoError(t, l.x.Put(l.ctx, sub))
		splits = append(splits, sub.Ref())
	}

	root := proto.NewObject(&proto.Tree{Splits: splits})
	require.NoError(t, l.x.Put(l.ctx, root))
	require.NoError(t, l.x.Put(l.ctx, proto.NewObject(&proto.Commit{Timestamp: 1, Tree: root.Ref(), BackupSet: "a"})))

	for _, name := range []string{"a.dat", "b.dat"} {
		files, err := l.x.FileInfo(l.ctx, "a", name, time.Now(), 10)
		require.NoError(t, err)
		require.Len(t, files, 1, name)
	}
}

func TestRefusesARootThatRepeatsASplitBeforeExpandingIt(t *testing.T) {
	l := newLocalIndex(t)

	file := proto.NewObject(&proto.File{Inline: []byte("x")})
	require.NoError(t, l.x.Put(l.ctx, file))

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
		Stat: &proto.FileInfo{Name: []byte("a.dat"), Type: proto.NodeType_NODE_FILE, MtimeNs: 1, Size: 1, Mode: 0644},
		Ref:  file.Ref(),
	}}})
	require.NoError(t, l.x.Put(l.ctx, tree))

	for range 3 {
		splits := make([]*proto.Ref, 60)
		for i := range splits {
			splits[i] = tree.Ref()
		}

		tree = proto.NewObject(&proto.Tree{Splits: splits})
		require.NoError(t, l.x.Put(l.ctx, tree))
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	err := l.x.Put(l.ctx, proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree.Ref(), BackupSet: "a"}))
	require.Error(t, err, "a split twice names its nodes twice")

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20), "the commit expanded the splits")
}

func TestUnknownSetIsEmpty(t *testing.T) {
	l := newLocalIndex(t)
	l.commit("a", "world.dat", 1, 0, "x")

	commits, err := l.x.CommitInfo(l.ctx, "nowhere", time.Now(), 10)
	require.NoError(t, err)
	require.Empty(t, commits, "an unknown set is empty, not an error")
}
