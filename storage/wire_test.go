package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wire is a store server that records the objects its clients send and
// the ones its Get answers with.
type wire struct {
	*Server

	mtx    sync.Mutex
	sent   []*proto.Stored
	served []*proto.Stored
}

func (w *wire) record(to *[]*proto.Stored, stored ...*proto.Stored) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	*to = append(*to, stored...)
}

func (w *wire) Put(ctx context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	w.record(&w.sent, request.GetObject())

	return w.Server.Put(ctx, request)
}

func (w *wire) PutCommits(ctx context.Context, request *proto.PutCommitsRequest) (*proto.PutCommitsResponse, error) {
	w.record(&w.sent, request.GetCommits()...)

	return w.Server.PutCommits(ctx, request)
}

func (w *wire) PutObjects(stream proto.Store_PutObjectsServer) error {
	return w.Server.PutObjects(&recordedStream{Store_PutObjectsServer: stream, wire: w})
}

func (w *wire) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	resp, err := w.Server.Get(ctx, request)
	if err == nil {
		w.record(&w.served, resp.GetObject())
	}

	return resp, err
}

type recordedStream struct {
	proto.Store_PutObjectsServer
	wire *wire
}

func (r *recordedStream) Recv() (*proto.PutObjectsRequest, error) {
	request, err := r.Store_PutObjectsServer.Recv()
	if err == nil {
		r.wire.record(&r.wire.sent, request.GetObjects()...)
	}

	return request, err
}

// sentAs is what the client sent of type t, the last of them.
func (w *wire) sentAs(t *testing.T, kind proto.ObjectType) *proto.Stored {
	t.Helper()

	w.mtx.Lock()
	defer w.mtx.Unlock()

	for i := len(w.sent) - 1; i >= 0; i-- {
		if w.sent[i].GetType() == kind {
			return w.sent[i]
		}
	}

	t.Fatalf("the client sent no %s", kind)

	return nil
}

// decoded is the object stored holds.
func decoded(t *testing.T, stored *proto.Stored) *proto.Object {
	t.Helper()

	obj, err := stored.Object()
	require.NoError(t, err)

	return obj
}

// serveWire serves a pack store behind a recording server.
func serveWire(t *testing.T) (*wire, *Client, *pack.PackStorage) {
	t.Helper()

	packs := testPackStore(t)
	remote := NewServer(NewStore(&packIndex{PackStorage: packs, latest: map[string]*proto.Ref{}}, nil))
	w := &wire{Server: remote}

	return w, serveRemote(t, remote, w)(testSecret, "node-1"), packs
}

// storedHash is the hash of the bytes the pack keeps for ref.
func storedHash(t *testing.T, packs *pack.PackStorage, kind proto.ObjectType, ref *proto.Ref) []byte {
	t.Helper()

	require.NoError(t, packs.Flush())

	var hash []byte

	require.NoError(t, packs.WalkHeaders(context.Background(), kind, func(hdr *proto.ObjectHeader) error {
		if hdr.Ref.Equal(ref) {
			hash = hdr.StoredHash
		}

		return nil
	}))
	require.NotNil(t, hash, "the pack holds no %s %x", kind, ref.GetHash())

	return hash
}

// logLines is compressible text, as a log or a database's binlog is.
func logLines(n int) []byte {
	var buf bytes.Buffer
	for i := 0; buf.Len() < n; i++ {
		fmt.Fprintf(&buf, "2026-10-25T12:%02d:%02d INFO player %d joined world %d at %d,%d\n", i/60%60, i%60, i*7919%1000, i%3, i*31%4096, i*17%256)
	}

	return buf.Bytes()[:n]
}

func TestAPlainBlobTravelsCompressedAndIsStoredAsReceived(t *testing.T) {
	ctx := context.Background()
	w, client, packs := serveWire(t)

	blob := proto.NewObject(&proto.Blob{Data: logLines(64 << 10)})
	require.NoError(t, client.Put(ctx, blob))

	sent := w.sentAs(t, proto.ObjectType_BLOB)
	require.Equal(t, proto.Compression_ZSTD, sent.Compression)
	require.Less(t, len(sent.Data), len(blob.GetBlob().Data)/2)
	require.Equal(t, proto.StoredHash(sent.Data), storedHash(t, packs, proto.ObjectType_BLOB, blob.Ref()))

	batched := proto.NewObject(&proto.Blob{Data: logLines(48 << 10)})
	require.NoError(t, client.PutObjects(ctx, []*proto.Object{batched}))

	sent = w.sentAs(t, proto.ObjectType_BLOB)
	require.Equal(t, proto.Compression_ZSTD, sent.Compression)
	require.Equal(t, proto.StoredHash(sent.Data), storedHash(t, packs, proto.ObjectType_BLOB, batched.Ref()))
}

func TestTheStoreKeepsTheBytesItReceivedInsteadOfCompressingAgain(t *testing.T) {
	ctx := context.Background()
	_, client, packs := serveWire(t)

	payload := logLines(64 << 10)
	best, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	require.NoError(t, err)

	data := best.EncodeAll(payload, nil)
	ours, _ := proto.Encode(payload)
	require.NotEqual(t, ours, data, "the client's bytes differ from what the store would write")

	blob := proto.NewObject(&proto.Blob{Data: payload})
	sent := &proto.Stored{Type: proto.ObjectType_BLOB, Data: data, Compression: proto.Compression_ZSTD}

	_, err = client.store.Put(client.outgoing(ctx), &proto.PutRequest{Object: sent, Ref: blob.Ref()})
	require.NoError(t, err)
	require.Equal(t, proto.StoredHash(data), storedHash(t, packs, proto.ObjectType_BLOB, blob.Ref()))

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: uint64(len(payload)), Ref: blob.Ref()}}})
	require.NoError(t, client.Put(ctx, file))

	var got *proto.Object
	require.NoError(t, client.ReadParts(ctx, file.Ref(), nil, func(_ int, obj *proto.Object) error {
		got = obj
		return nil
	}))
	require.Equal(t, payload, got.GetBlob().Data)
	require.Equal(t, data, got.GetStored().Data, "the part comes back as it was stored")
}

func TestAReadHandsOutTheStoredBytes(t *testing.T) {
	ctx := context.Background()
	w, client, _ := serveWire(t)

	file := proto.NewObject(&proto.File{Inline: logLines(4 << 10)})
	payload, err := file.Canonical()
	require.NoError(t, err)

	best, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	require.NoError(t, err)

	data := best.EncodeAll(payload, nil)
	_, err = client.store.Put(client.outgoing(ctx), &proto.PutRequest{
		Object: &proto.Stored{Type: proto.ObjectType_FILE, Data: data, Compression: proto.Compression_ZSTD},
		Ref:    file.Ref(),
	})
	require.NoError(t, err)

	got, err := client.Get(ctx, file.Ref())
	require.NoError(t, err)
	require.Equal(t, file.GetFile().Inline, got.GetFile().Inline)

	w.mtx.Lock()
	defer w.mtx.Unlock()

	require.Len(t, w.served, 1)
	require.Equal(t, data, w.served[0].Data, "the store hands out what it keeps, not a fresh encoding")
}

func TestAnObjectWhoseDecodedBytesDoNotMatchItsRefIsRefused(t *testing.T) {
	ctx := context.Background()
	_, client, _ := serveWire(t)

	claimed := proto.NewObject(&proto.Blob{Data: logLines(8 << 10)})
	data, compression := proto.Encode(logLines(9 << 10))
	sent := &proto.Stored{Type: proto.ObjectType_BLOB, Data: data, Compression: compression}

	_, err := client.store.Put(client.outgoing(ctx), &proto.PutRequest{Object: sent, Ref: claimed.Ref()})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	missing, err := client.Missing(ctx, []*proto.Ref{claimed.Ref()})
	require.NoError(t, err)
	require.Len(t, missing, 1, "nothing answers for the ref the client claimed")
}

func TestAPayloadThatIsNotItsObjectsCanonicalEncodingIsRefused(t *testing.T) {
	ctx := context.Background()
	_, client, _ := serveWire(t)

	file := proto.NewObject(&proto.File{Inline: []byte("hello")})
	payload, err := file.Canonical()
	require.NoError(t, err)

	// a chunker field written out at its default decodes to the same file
	padded := append(payload, 0x18, 0x00)
	data, compression := proto.Encode(padded)

	_, err = client.store.Put(client.outgoing(ctx), &proto.PutRequest{
		Object: &proto.Stored{Type: proto.ObjectType_FILE, Data: data, Compression: compression},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	missing, err := client.Missing(ctx, []*proto.Ref{file.Ref()})
	require.NoError(t, err)
	require.Len(t, missing, 1)
}

// bomb is a zstd frame of a few bytes that claims a gigabyte of content.
func bomb() []byte {
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xc0, 0x00}
	frame = binary.LittleEndian.AppendUint64(frame, 1<<30)

	return append(frame, 0x01, 0x00, 0x00)
}

func TestAPayloadThatDecompressesPastTheBoundIsRefused(t *testing.T) {
	ctx := context.Background()
	_, client, _ := serveWire(t)

	sent := &proto.Stored{Type: proto.ObjectType_BLOB, Data: bomb(), Compression: proto.Compression_ZSTD}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	_, err := client.store.Put(client.outgoing(ctx), &proto.PutRequest{Object: sent})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	stream, err := client.store.PutObjects(client.outgoing(ctx))
	require.NoError(t, err)
	_ = stream.Send(&proto.PutObjectsRequest{Objects: []*proto.Stored{sent}})
	_, err = stream.CloseAndRecv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "the server allocated what the payload claims")
}

func TestIncompressibleDataTravelsRaw(t *testing.T) {
	ctx := context.Background()
	w, client, packs := serveWire(t)

	data := make([]byte, 32<<10)
	_, _ = rand.Read(data)

	blob := proto.NewObject(&proto.Blob{Data: data})
	require.NoError(t, client.Put(ctx, blob))

	sent := w.sentAs(t, proto.ObjectType_BLOB)
	require.Equal(t, proto.Compression_NONE, sent.Compression)
	require.Equal(t, data, sent.Data)
	require.Equal(t, proto.StoredHash(data), storedHash(t, packs, proto.ObjectType_BLOB, blob.Ref()))
}

func TestTreesFilesAndCommitsTravelCompressed(t *testing.T) {
	ctx := context.Background()
	w, client, _ := serveWire(t)

	blob := proto.NewObject(&proto.Blob{Data: []byte("level.dat")})
	require.NoError(t, client.Put(ctx, blob))

	file := proto.NewObject(&proto.File{Inline: logLines(4 << 10)})
	require.NoError(t, client.Put(ctx, file))

	tree := &proto.Tree{}
	for i := range 200 {
		tree.Nodes = append(tree.Nodes, &proto.TreeNode{
			Stat: &proto.FileInfo{Name: fmt.Appendf(nil, "r.%03d.0.mca", i), Mode: 0o644, Size: 4 << 10, MtimeNs: 1_700_000_000_000_000_000, Type: proto.NodeType_NODE_FILE},
			Ref:  file.Ref(),
		})
	}

	treeObj := proto.NewObject(tree)
	require.NoError(t, client.Put(ctx, treeObj))

	commit := proto.NewObject(&proto.Commit{
		Timestamp: 1_700_000_000,
		Tree:      treeObj.Ref(),
		BackupSet: "world",
		AgentId:   "node-1",
		Metadata:  map[string]string{"motd": string(logLines(1 << 10))},
	})
	require.NoError(t, client.Put(ctx, commit))

	for kind, object := range map[proto.ObjectType]*proto.Object{proto.ObjectType_FILE: file, proto.ObjectType_TREE: treeObj, proto.ObjectType_COMMIT: commit} {
		payload, err := object.Canonical()
		require.NoError(t, err)

		sent := w.sentAs(t, kind)
		require.Equal(t, proto.Compression_ZSTD, sent.Compression, kind.String())
		require.Less(t, len(sent.Data), len(payload), kind.String())
	}
}
