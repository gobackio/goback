package goback

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/upload"

	"github.com/stretchr/testify/require"
)

// servedAgent serves a new store and returns an agent's client of it.
func servedAgent(t *testing.T) *storage.Client {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true, Schedule: maintenance.DefaultSchedule})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = agent.Close()
		cancel()
		<-served
		_ = repo.Close()
	})

	client, ok := agent.Index.(*storage.Client)
	require.True(t, ok)

	return client
}

func fileNamed(name, content string) (*proto.Object, *proto.TreeNode) {
	obj := proto.NewObject(&proto.File{Inline: []byte(content)})

	return obj, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, Size: int64(len(content)), Mode: 0o644}, Ref: obj.Ref()}
}

func snapshotOf(set string, at int64, tree *proto.Object) *proto.Object {
	return proto.NewObject(&proto.Commit{Timestamp: at, Tree: tree.Ref(), BackupSet: set, AgentId: "agent"})
}

func TestAnAgentCommitsSeveralSnapshotsOfSeveralSetsInOneSession(t *testing.T) {
	client := servedAgent(t)
	ctx := context.Background()

	sctx, err := client.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	a, aNode := fileNamed("a.txt", "one")
	b, bNode := fileNamed("b.txt", "two")
	first := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode}})
	second := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode, bNode}})

	require.NoError(t, client.PutObjects(sctx, []*proto.Object{a, first, second}))

	commits := []*proto.Object{snapshotOf("world", 1, first), snapshotOf("world", 2, second), snapshotOf("other", 3, first)}

	var missing *backup.MissingError
	require.ErrorAs(t, client.PutCommits(sctx, commits), &missing)
	require.Len(t, missing.Commits, 3)
	require.Empty(t, missing.Commits[0])
	require.Len(t, missing.Commits[1], 1)
	require.Equal(t, b.Ref().Hash, missing.Commits[1][0].Hash)
	require.Empty(t, missing.Commits[2])

	_, err = client.LatestCommit(ctx, "world")
	require.ErrorIs(t, err, backup.ErrNotFound, "none of them was stored")

	require.NoError(t, client.PutObjects(sctx, []*proto.Object{b}))
	require.NoError(t, client.PutCommits(sctx, commits))

	require.Less(t, commits[0].GetCommit().GetReceivedAtNs(), commits[1].GetCommit().GetReceivedAtNs())
	require.True(t, commits[1].GetCommit().GetParent().Equal(commits[0].Ref()), "a commit takes the one of its set before it as its parent")
	require.Nil(t, commits[2].GetCommit().GetParent())

	for set, want := range map[string]*proto.Object{"world": commits[1], "other": commits[2]} {
		latest, err := client.LatestCommit(ctx, set)
		require.NoError(t, err)
		require.Equal(t, want.Ref().Hash, latest.Hash, set)
	}

	world, err := client.CommitInfo(ctx, "world", time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, world, 2)

	require.ErrorIs(t, client.TouchSession(sctx), backup.ErrNoSession, "the commits ended the session")
}

func TestAnUploadFileCommitsEverySnapshotItHolds(t *testing.T) {
	client := servedAgent(t)
	ctx := context.Background()

	sctx, err := client.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	a, aNode := fileNamed("a.txt", "one")
	b, bNode := fileNamed("b.txt", "two")
	first := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode}})
	second := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode, bNode}})
	commits := []*proto.Object{snapshotOf("world", 1, first), snapshotOf("world", 2, second)}

	var buf bytes.Buffer

	w, err := upload.NewWriter(&buf)
	require.NoError(t, err)

	for _, obj := range []*proto.Object{a, b, first, second, commits[0], commits[1]} {
		require.NoError(t, w.Write(obj))
	}

	done, err := client.Upload(sctx, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, backup.UploadDone, done.State)
	require.Len(t, done.Commits, 2)
	require.Equal(t, done.Commits[1].Ref().Hash, done.Commit.Ref().Hash)

	latest, err := client.LatestCommit(ctx, "world")
	require.NoError(t, err)
	require.Equal(t, done.Commits[1].Ref().Hash, latest.Hash)
}
