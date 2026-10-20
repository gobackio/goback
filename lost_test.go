package goback

import (
	"context"
	"net"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/stretchr/testify/require"
)

func TestAnAgentThatLacksContentCommitsWhatItHasWithTheRestMarkedLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true, Schedule: maintenance.DefaultSchedule})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = agent.Close() })

	sctx, err := agent.Store.(backup.SessionStore).BeginSession(ctx, &backup.Session{AgentID: "agent", Set: "world"})
	require.NoError(t, err)

	kept := proto.NewObject(&proto.Blob{Data: []byte("kept")})
	gone := proto.NewObject(&proto.Blob{Data: []byte("gone")})
	keptFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: kept.Ref(), Length: 4}}})
	goneFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: gone.Ref(), Length: 4}}})
	root := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{
		{Stat: &proto.FileInfo{Name: []byte("gone.txt"), Size: 4, Type: proto.NodeType_NODE_FILE}, Ref: goneFile.Ref()},
		{Stat: &proto.FileInfo{Name: []byte("kept.txt"), Size: 4, Type: proto.NodeType_NODE_FILE}, Ref: keptFile.Ref()},
	}})

	for _, obj := range []*proto.Object{kept, keptFile, goneFile, root} {
		require.NoError(t, agent.Store.Put(sctx, obj))
	}

	commit := proto.NewObject(&proto.Commit{Timestamp: 42, Tree: root.Ref(), AgentId: "agent", BackupSet: "world", Incomplete: true})
	require.NoError(t, agent.Store.Put(sctx, commit))
	require.True(t, commit.GetCommit().Incomplete, "the agent has the commit as stored")
	require.NotEqual(t, root.Ref().Hash, commit.GetCommit().Tree.Hash)

	latest, err := agent.Index.LatestCommit(ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(commit.Ref()))

	tree, err := backup.LoadTree(ctx, agent.Store, commit.GetCommit().Tree)
	require.NoError(t, err)
	require.True(t, tree.Incomplete)
	require.True(t, tree.Nodes[0].Stat.Lost)
	require.False(t, tree.Nodes[1].Stat.Lost)

	sctx, err = agent.Store.(backup.SessionStore).BeginSession(ctx, &backup.Session{AgentID: "agent", Set: "world"})
	require.NoError(t, err)

	var missing *backup.MissingError
	err = agent.Store.Put(sctx, proto.NewObject(&proto.Commit{Timestamp: 43, Tree: root.Ref(), AgentId: "agent", BackupSet: "world"}))
	require.ErrorAs(t, err, &missing, "what the commit left above its gap is not taken as whole")
	require.Equal(t, gone.Ref().Hash, missing.Refs[0].Hash)
}
