package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// packIndex is a backup.Index over a pack store: it stamps commits and
// remembers the latest per set, and leaves the rest to the store.
type packIndex struct {
	*pack.PackStorage
	mtx    sync.Mutex
	latest map[string]*proto.Ref
}

func (p *packIndex) Open() error  { return nil }
func (p *packIndex) Close() error { return nil }

func (p *packIndex) ReIndex(context.Context) (backup.ReIndexReport, error) {
	return backup.ReIndexReport{}, nil
}
func (p *packIndex) FileInfo(context.Context, string, string, time.Time, int) ([]*proto.TreeNode, error) {
	return nil, backup.ErrNotImplemented
}
func (p *packIndex) CommitInfo(context.Context, string, time.Time, int) ([]*proto.Commit, error) {
	return nil, backup.ErrNotImplemented
}

func (p *packIndex) LatestCommit(ctx context.Context, set string) (*proto.Ref, error) {
	if _, err := auth.Require(ctx); err != nil {
		return nil, err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	ref, ok := p.latest[set]
	if !ok {
		return nil, backup.ErrNotFound
	}

	return ref, nil
}

func (p *packIndex) Put(ctx context.Context, obj *proto.Object) error {
	err := backup.CheckReferences(ctx, p.PackStorage, obj)
	if err != nil {
		return err
	}

	received := time.Now()
	if carried := time.Unix(0, obj.ReceivedAtNs()); obj.ReceivedAtNs() != 0 && carried.Before(received) {
		received = carried
	}
	obj.Stamp(7, received)

	err = p.PackStorage.Put(ctx, obj)
	if err != nil {
		return err
	}

	if commit := obj.GetCommit(); commit != nil {
		p.mtx.Lock()
		p.latest[commit.BackupSet] = obj.Ref()
		p.mtx.Unlock()
	}

	return nil
}

// testPackStore opens a pack store, with sessions, on a temporary bucket.
func testPackStore(t *testing.T) *pack.PackStorage {
	t.Helper()

	bucket, err := fileblob.OpenBucket(t.TempDir(), nil)
	require.NoError(t, err)

	store, err := pack.NewPackStorage(
		pack.WithArchiveStorage(NewBucketStore(bucket)),
		pack.WithArchiveIndex(pack.NewInMemoryIndex()),
		pack.WithMaxParallel(4),
		pack.WithCloseBeforeRead(true),
	)
	require.NoError(t, err)
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func startPackServer(t *testing.T) (*pack.PackStorage, dialer) {
	t.Helper()

	store := testPackStore(t)
	index := &packIndex{PackStorage: store, latest: map[string]*proto.Ref{}}

	return store, startServerWith(t, index, store)
}

func TestRemoteSessionsScopeVisibility(t *testing.T) {
	store, dial := startPackServer(t)
	ctx := context.Background()

	a := dial("node-1")
	b := dial("node-2")

	sctx, err := a.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	session, ok := backup.SessionFromContext(sctx)
	require.True(t, ok)
	require.NotEmpty(t, session.ID)

	live, err := store.LookupSession(ctx, session.ID)
	require.NoError(t, err)
	require.Equal(t, "node-1", live.AgentID, "the server decides the session's agent")
	require.Equal(t, "world", live.Set)

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, a.Put(sctx, tree))

	_, err = a.Get(sctx, tree.Ref())
	require.NoError(t, err, "the session reads its own writes")

	_, err = a.Get(ctx, tree.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "the same caller outside the session does not")

	_, err = b.Get(ctx, tree.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "another agent does not")

	_, err = b.Get(backup.WithSession(ctx, &backup.Session{ID: session.ID}), tree.Ref())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a session id is bound to its caller")

	_, err = a.Get(backup.WithSession(ctx, &backup.Session{ID: "nope"}), tree.Ref())
	require.ErrorIs(t, err, backup.ErrNotFound, "an unknown session id is a not-found call")

	commit := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree.Ref(), AgentId: "node-1", BackupSet: "world"})
	require.NoError(t, a.Put(sctx, commit))

	_, err = b.Get(ctx, tree.Ref())
	require.NoError(t, err, "committed objects are visible to everyone")

	latest, err := a.LatestCommit(ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(commit.Ref()))

	_, err = store.LookupSession(ctx, session.ID)
	require.ErrorIs(t, err, backup.ErrNoSession, "the commit ended the session")

	after := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: &proto.FileInfo{Name: []byte("x")}, Ref: tree.Ref()}}})
	require.Error(t, a.Put(sctx, after))
	require.ErrorIs(t, a.EndSession(sctx), backup.ErrNoSession, "ending it again finds no session")

	_, err = a.Get(ctx, tree.Ref())
	require.NoError(t, err)

	err = b.EndSession(backup.WithSession(ctx, &backup.Session{ID: session.ID}))
	require.ErrorIs(t, err, backup.ErrNoSession)

	_, err = dial("").BeginSession(ctx, &backup.Session{Set: "world"})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestRemoteSessionsUnsupported(t *testing.T) {
	_, dial := startServer(t)

	_, err := dial("node-1").BeginSession(context.Background(), &backup.Session{Set: "world"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestRemoteWritesNeedASession(t *testing.T) {
	_, dial := startPackServer(t)
	client := dial("node-1")

	err := client.Put(context.Background(), proto.NewObject(&proto.Tree{}))
	require.ErrorIs(t, err, backup.ErrNoSession)

	sctx, err := client.BeginSession(context.Background(), &backup.Session{Set: "world"})
	require.NoError(t, err)
	require.NoError(t, client.Put(sctx, proto.NewObject(&proto.Tree{})))
}
