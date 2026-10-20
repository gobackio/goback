package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/upload"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// uploadFile is an upload file of a tree of n files, each of one 16 KB part,
// and the tree's ref.
func uploadFile(t *testing.T, n int) ([]byte, *proto.Ref) {
	t.Helper()

	var buf bytes.Buffer

	w, err := upload.NewWriter(&buf)
	require.NoError(t, err)

	nodes := make([]*proto.TreeNode, 0, n)
	for i := range n {
		part := proto.NewObject(&proto.Blob{Data: bytes.Repeat([]byte{byte(i)}, 16<<10)})
		file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Ref: part.Ref(), Length: 16 << 10}}})
		require.NoError(t, w.Write(part))
		require.NoError(t, w.Write(file))
		nodes = append(nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(fmt.Sprintf("%03d", i)), Type: proto.NodeType_NODE_FILE}, Ref: file.Ref()})
	}

	tree := proto.NewObject(&proto.Tree{Nodes: nodes})
	require.NoError(t, w.Write(tree))

	return buf.Bytes(), tree.Ref()
}

func TestRemoteUploadIngestsTheFileInline(t *testing.T) {
	_, dial := startPackServer(t)
	client := dial("node-1")

	sctx, err := client.BeginSession(context.Background(), &backup.Session{Set: "world"})
	require.NoError(t, err)

	file, tree := uploadFile(t, 300)

	done, err := client.Upload(sctx, bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	require.Equal(t, backup.UploadDone, done.State)
	require.Equal(t, int64(601), done.Objects)
	require.Equal(t, int64(601), done.Stored)

	again, err := client.Upload(sctx, bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	require.Equal(t, int64(601), again.Objects)
	require.Zero(t, again.Stored)

	commit := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree, AgentId: "node-1", BackupSet: "world"})
	require.NoError(t, client.Put(sctx, commit))

	_, err = client.Upload(context.Background(), bytes.NewReader(file), int64(len(file)))
	require.ErrorIs(t, err, backup.ErrNoSession)
}

func TestRemoteUploadIsRefusedPastItsDeclaredSize(t *testing.T) {
	_, dial := startPackServer(t)
	client := dial("node-1")

	sctx, err := client.BeginSession(context.Background(), &backup.Session{Set: "world"})
	require.NoError(t, err)

	file, _ := uploadFile(t, 10)

	_, err = client.Upload(sctx, bytes.NewReader(file), int64(len(file))-1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = client.Upload(sctx, bytes.NewReader(nil), MaxUpload+1)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRemoteSendUploadTakesOnlyAnIdIssuedToTheCallersSession(t *testing.T) {
	_, dial := startPackServer(t)
	ctx := context.Background()
	a, b := dial("node-1"), dial("node-2")

	first, err := a.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)
	second, err := a.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)
	other, err := b.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	file, _ := uploadFile(t, 10)
	size := int64(len(file))

	begun, err := a.store.BeginUpload(a.outgoing(first), &proto.BeginUploadRequest{Size: size})
	require.NoError(t, err)
	id := begun.GetUploadId()

	_, err = a.sendUpload(first, "unknown", bytes.NewReader(file), size)
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = b.sendUpload(other, id, bytes.NewReader(file), size)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another caller")

	_, err = a.sendUpload(second, id, bytes.NewReader(file), size)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another session")

	_, err = a.sendUpload(first, id, bytes.NewReader(file), size)
	require.NoError(t, err, "refusals leave the id to its caller")

	_, err = a.sendUpload(first, id, bytes.NewReader(file), size)
	require.Equal(t, codes.NotFound, status.Code(err), "an id takes one file")
}

func TestRemoteSendUploadWithoutSessionsTakesOnlyTheCallersId(t *testing.T) {
	_, dial := startServer(t)
	ctx := context.Background()
	a, b := dial("node-1"), dial("node-2")

	file, _ := uploadFile(t, 3)
	size := int64(len(file))

	begun, err := a.store.BeginUpload(a.outgoing(ctx), &proto.BeginUploadRequest{Size: size})
	require.NoError(t, err)

	_, err = b.sendUpload(ctx, begun.GetUploadId(), bytes.NewReader(file), size)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

// located answers every upload with a location.
type located struct {
	*Server
	url string
}

func (l located) BeginUpload(context.Context, *proto.BeginUploadRequest) (*proto.BeginUploadResponse, error) {
	return &proto.BeginUploadResponse{
		UploadId: "up-1",
		Location: &proto.UploadLocation{Url: l.url, Header: map[string]string{"X-Goog-Content-Length-Range": "0,1000"}},
	}, nil
}

func TestRemoteUploadPutsTheFileWhereTheStoreSays(t *testing.T) {
	var (
		method, header string
		length         int64
		body           []byte
	)

	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, header, length = r.Method, r.Header.Get("X-Goog-Content-Length-Range"), r.ContentLength
		body, _ = io.ReadAll(r.Body)
	}))
	t.Cleanup(bucket.Close)

	remote := NewServer(NewStore(newMemIndex(), nil))
	client := serveRemote(t, remote, located{Server: remote, url: bucket.URL})(testSecret, "node-1")
	client.HTTP = bucket.Client()

	file, _ := uploadFile(t, 3)

	placed, err := client.Upload(context.Background(), bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	require.Equal(t, backup.UploadStatus{ID: "up-1", State: backup.UploadWaiting, BytesTotal: int64(len(file))}, placed)
	require.Equal(t, http.MethodPut, method)
	require.Equal(t, "0,1000", header)
	require.Equal(t, int64(len(file)), length)
	require.Equal(t, file, body)

	_, err = client.UploadStatus(context.Background(), []string{"up-1"})
	require.ErrorIs(t, err, backup.ErrNotImplemented)
}
