package postgres

import (
	"context"
	"crypto/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// meter counts the calls a store server takes, the bytes they move, the
// metadata objects Get answers with and the filters GetPresence sends.
type meter struct {
	mtx      sync.Mutex
	calls    map[string]int
	failed   map[string]int
	bytes    map[string]int64
	metadata int
	filters  int
}

func newMeter() *meter {
	return &meter{calls: map[string]int{}, failed: map[string]int{}, bytes: map[string]int64{}}
}

type methodKey struct{}

func (m *meter) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, methodKey{}, path.Base(info.FullMethodName))
}

func (m *meter) HandleRPC(ctx context.Context, s stats.RPCStats) {
	method, _ := ctx.Value(methodKey{}).(string)

	m.mtx.Lock()
	defer m.mtx.Unlock()

	switch s := s.(type) {
	case *stats.Begin:
		m.calls[method]++
	case *stats.End:
		if s.Error != nil {
			m.failed[method+" "+status.Code(s.Error).String()]++
		}
	case *stats.InPayload:
		m.bytes[method] += int64(s.WireLength)
	case *stats.OutPayload:
		m.bytes[method] += int64(s.WireLength)

		if resp, ok := s.Payload.(*proto.GetResponse); ok && resp.GetObject().Type().Metadata() {
			m.metadata++
		}

		if resp, ok := s.Payload.(*proto.GetPresenceResponse); ok && resp.GetFilter() != nil {
			m.filters++
		}
	}
}

func (m *meter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (m *meter) HandleConn(context.Context, stats.ConnStats) {}

// reads counts the metadata the server handed out: commits, trees and files
// by Get, and trees by GetTree.
func (m *meter) reads() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.metadata + m.calls["GetTree"]
}

func (m *meter) reset() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.calls, m.failed, m.bytes, m.metadata, m.filters = map[string]int{}, map[string]int{}, map[string]int64{}, 0, 0
}

const servedSecret = "s3cret"

// served is the fixture's index and store behind a store server on the
// loopback interface, and a client of it.
func (f *walFixture) served() (*storage.Client, *meter) {
	f.t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(f.t, err)

	m := newMeter()
	remote := storage.NewServer(storage.NewStore(f.index, f.store))
	srv := grpc.NewServer(
		grpc.StatsHandler(m),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(servedSecret), remote.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(servedSecret), remote.StreamInterceptor()),
	)
	proto.RegisterStoreServer(srv, remote)

	go func() { _ = srv.Serve(listener) }()
	f.t.Cleanup(srv.Stop)

	client, err := storage.NewPlaintextClient(listener.Addr().String(), auth.Credentials{Secret: servedSecret, AgentID: "a", Plaintext: true})
	require.NoError(f.t, err)

	return client, m
}

// receiver commits the fixture's spool through client, reading through
// objects, as a receiver of a store server does.
func (f *walFixture) receiver(client *storage.Client, objects backup.ObjectStore) *Receiver {
	return &Receiver{
		Spool:   f.spool,
		BaseSet: "db-base",
		Window:  14 * 24 * time.Hour,
		Walker: func() *backup.Walker {
			return &backup.Walker{Index: client, Objects: objects, Sessions: client, Set: "db-wal", AgentID: "a", Root: f.spool.Dir, Workers: 2}
		},
	}
}

// growing is segment n of timeline 1 while it is received. It grows by
// the same random block again and again, so its new parts repeat parts its
// previous versions hold.
type growing struct {
	f       *walFixture
	name    string
	path    string
	content []byte
	block   []byte
}

func (f *walFixture) growing(n uint64) *growing {
	dir := filepath.Join(f.spool.Dir, receivingDir)
	require.NoError(f.t, os.MkdirAll(dir, 0o700))

	block := make([]byte, 1<<20)
	_, _ = rand.Read(block)

	name := walName(1, n)

	return &growing{f: f, name: name, path: filepath.Join(dir, name), content: segmentContent(1, n, 4096), block: block}
}

// commit has r commit the spool and the segment once it grew by a block.
func (g *growing) commit(r *Receiver) *backup.WalkResult {
	g.f.t.Helper()

	g.content = append(g.content, g.block...)
	require.NoError(g.f.t, os.WriteFile(g.path, g.content, 0o600))

	files, err := g.f.spool.Files()
	require.NoError(g.f.t, err)

	result, err := r.commit(g.f.ctx, &cut{files: files, open: &OpenSegment{Name: g.name, Path: g.path, Size: int64(len(g.content))}})
	require.NoError(g.f.t, err)

	return result
}
