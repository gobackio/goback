package goback

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"

	"github.com/stretchr/testify/require"
)

func TestAPlaintextAgentNeverDedupsAgainstASealedForgeryOfItsContent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = agent.Close()
		cancel()
		<-served
		_ = repo.Close()
	})

	client := agent.Index.(*storage.Client)
	honest := proto.NewObject(&proto.Blob{Data: []byte("a popular server jar")})
	empty := proto.NewObject(&proto.Tree{})

	junk := proto.NewObject(&proto.Stored{Type: proto.ObjectType_BLOB, Data: []byte("junk"), Encryption: proto.Encryption_SEALED, KeyId: []byte{0xba, 0xd0}})

	sctx, err := client.BeginSession(ctx, &backup.Session{Set: "evil"})
	require.NoError(t, err)
	require.NoError(t, client.PutObjects(sctx, []*proto.Object{empty, junk}))
	require.NoError(t, client.PutCommits(sctx, []*proto.Object{snapshotOf("evil", 1, empty)}))

	has, err := repo.Store.Has(ctx, honest.Ref())
	require.NoError(t, err)
	require.False(t, has, "the junk is stored under the ref its bytes hash to")

	sctx, err = client.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: honest.Ref(), Length: uint64(len(honest.GetBlob().GetData()))}}})

	missing, err := client.PutFile(sctx, file, []*proto.Ref{honest.Ref()})
	require.NoError(t, err)
	require.Len(t, missing, 1, "the forgery does not count as the part")

	require.NoError(t, client.Put(sctx, honest))

	missing, err = client.PutFile(sctx, file, []*proto.Ref{honest.Ref()})
	require.NoError(t, err)
	require.Empty(t, missing)

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte("server.jar"), Mode: 0o644, Size: int64(len(honest.GetBlob().GetData()))}, Ref: file.Ref()}}})
	require.NoError(t, client.PutObjects(sctx, []*proto.Object{tree}))
	require.NoError(t, client.PutCommits(sctx, []*proto.Object{snapshotOf("world", 2, tree)}))

	reader, err := backup.NewBackupReader(repo.Store).ReadFile(ctx, file.Ref())
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, honest.GetBlob().GetData(), got, "the honest copy is served")
}
