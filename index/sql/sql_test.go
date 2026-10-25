package sql

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/file"
	"github.com/gobackio/goback/index/sql/ent/tree"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// openIndex opens an empty index over store; the Postgres suite swaps it.
var openIndex = openMemory

var memoryIndexes atomic.Int64

func openMemory(t testing.TB, store backup.ObjectStore) *Index {
	t.Helper()

	x := NewMemory(fmt.Sprintf("test-%d", memoryIndexes.Add(1)), store)
	require.NoError(t, x.Open())
	t.Cleanup(func() { _ = x.Close() })

	return x
}

// memStore is an object store with tombstones and header walks, enough
// for the index.
type memStore struct {
	mu         sync.Mutex
	objects    map[string]*proto.Object
	tombstones map[string]struct{}
	erased     map[string]struct{}
	// revived are the tombstoned refs an un-tombstone newer than their
	// tombstone takes back
	revived   map[string]struct{}
	leases    []*proto.Ref
	flushed   int
	flushErr  error
	deleteErr error
	// asked counts the Has calls for each ref, and read the Get calls
	asked map[string]int
	read  map[string]int
	// beforeGet runs once, before the first Get of its ref
	beforeGet map[string]func()
	// getErr fails the next Get of its ref once
	getErr map[string]error
}

func newMemStore() *memStore {
	return &memStore{objects: map[string]*proto.Object{}, tombstones: map[string]struct{}{}, erased: map[string]struct{}{}, revived: map[string]struct{}{}, asked: map[string]int{}, read: map[string]int{}}
}

func (m *memStore) Erase(ctx context.Context, ref *proto.Ref) error {
	m.mu.Lock()
	m.erased[string(ref.Hash)] = struct{}{}
	m.mu.Unlock()

	return m.Delete(ctx, ref)
}

func (m *memStore) RestoreLeases(context.Context) ([]*proto.Ref, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*proto.Ref(nil), m.leases...), nil
}

func (m *memStore) Put(_ context.Context, obj *proto.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[string(obj.Ref().Hash)] = obj
	return nil
}

func (m *memStore) Get(_ context.Context, ref *proto.Ref) (*proto.Object, error) {
	m.mu.Lock()
	before := m.beforeGet[string(ref.Hash)]
	delete(m.beforeGet, string(ref.Hash))
	m.mu.Unlock()

	if before != nil {
		before()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.read[string(ref.Hash)]++

	if err := m.getErr[string(ref.Hash)]; err != nil {
		delete(m.getErr, string(ref.Hash))
		return nil, err
	}

	obj, ok := m.objects[string(ref.Hash)]
	if !ok {
		return nil, backup.ErrNotFound
	}
	return obj, nil
}

func (m *memStore) Has(_ context.Context, ref *proto.Ref) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.asked[string(ref.Hash)]++
	_, ok := m.objects[string(ref.Hash)]
	return ok, nil
}

// HasAll answers as a pack store does, unlike Has: an object tombstoned
// and not revived is absent.
func (m *memStore) HasAll(_ context.Context, refs []*proto.Ref) ([]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	has := make([]bool, len(refs))
	for i, ref := range refs {
		m.asked[string(ref.Hash)]++
		_, ok := m.objects[string(ref.Hash)]
		_, tombstoned := m.tombstones[string(ref.Hash)]
		_, revived := m.revived[string(ref.Hash)]
		has[i] = ok && (!tombstoned || revived)
	}

	return has, nil
}

func (m *memStore) Delete(_ context.Context, ref *proto.Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.tombstones[string(ref.Hash)] = struct{}{}
	delete(m.revived, string(ref.Hash))
	return nil
}

// Revive implements backup.Reviver over the objects the store holds.
func (m *memStore) Revive(_ context.Context, commits []*proto.Ref, dryRun bool) ([]backup.Revival, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	revivals := make([]backup.Revival, len(commits))

	for i, c := range commits {
		revivals[i].Commit = c

		for frontier := []*proto.Ref{c}; len(frontier) > 0; {
			ref := frontier[0]
			frontier = frontier[1:]

			obj, ok := m.objects[string(ref.Hash)]
			if !ok {
				revivals[i].Missing = append(revivals[i].Missing, ref)
				revivals[i].MissingCount++

				continue
			}

			frontier = append(frontier, backup.References(obj)...)
		}

		if !dryRun && revivals[i].Whole() {
			m.revived[string(c.Hash)] = struct{}{}
		}
	}

	return revivals, nil
}

// Revived implements backup.Reviver.
func (m *memStore) Revived(_ context.Context, commit *proto.Ref) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.revived[string(commit.Hash)]

	return ok, nil
}

// Holds implements backup.Reviver.
func (m *memStore) Holds(_ context.Context, commits []*proto.Ref) ([]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	held := make([]bool, len(commits))
	for i, c := range commits {
		_, held[i] = m.objects[string(c.Hash)]
	}

	return held, nil
}

func (m *memStore) Walk(_ context.Context, load bool, t proto.ObjectType, fn backup.ObjectReceiver) error {
	m.mu.Lock()
	objects := make([]*proto.Object, 0, len(m.objects))
	for _, obj := range m.objects {
		if t == proto.ObjectType_INVALID || obj.Type() == t {
			objects = append(objects, obj)
		}
	}
	m.mu.Unlock()

	for _, obj := range objects {
		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}

func (m *memStore) WalkHeaders(_ context.Context, t proto.ObjectType, fn func(*proto.ObjectHeader) error) error {
	m.mu.Lock()
	var headers []*proto.ObjectHeader
	if t == proto.ObjectType_TOMBSTONE || t == proto.ObjectType_INVALID {
		for target := range m.tombstones {
			ref := &proto.Ref{Hash: []byte(target)}
			headers = append(headers, &proto.ObjectHeader{Ref: proto.TombstoneRef(ref), TombstoneFor: ref, Type: proto.ObjectType_TOMBSTONE})
		}
	}
	for _, obj := range m.objects {
		if t == obj.Type() || (t == proto.ObjectType_INVALID) {
			headers = append(headers, &proto.ObjectHeader{Ref: obj.Ref(), Type: obj.Type()})
		}
	}
	m.mu.Unlock()

	for _, hdr := range headers {
		if err := fn(hdr); err != nil {
			return err
		}
	}
	return nil
}

func (m *memStore) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed++
	return m.flushErr
}

func (m *memStore) tombstoned(ref *proto.Ref) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.tombstones[string(ref.Hash)]
	return ok
}

type fixture struct {
	t     *testing.T
	ctx   context.Context
	store *memStore
	x     *Index
	clock time.Time
	epoch time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{
		t:     t,
		store: newMemStore(),
		clock: time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC),
		epoch: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}

	f.ctx = auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-1"})
	f.x = f.index()

	require.NoError(t, f.x.SetDefaultPolicy(f.ctx, &retention.Policy{KeepLast: 100}))

	return f
}

// index opens another empty index over the fixture's store, on the
// fixture's clock.
func (f *fixture) index() *Index {
	x := openIndex(f.t, f.store)
	x.Now = func() time.Time { return f.clock }

	return x
}

// presence builds the presence filters of commits still without one.
func (f *fixture) presence() {
	f.t.Helper()
	_, err := f.x.BuildPendingPresence(f.ctx)
	require.NoError(f.t, err)
}

// measure runs MeasureSets and returns the names of the sets it measured.
func (f *fixture) measure() []string {
	f.t.Helper()
	measured, err := f.x.MeasureSets(f.ctx)
	require.NoError(f.t, err)

	return setNames(measured)
}

func (f *fixture) advance(d time.Duration) {
	f.clock = f.clock.Add(d)
}

func (f *fixture) file(name, content string) *proto.TreeNode {
	obj := proto.NewObject(&proto.File{Inline: []byte(content)})
	require.NoError(f.t, f.store.Put(f.ctx, obj))

	return &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, MtimeNs: f.epoch.UnixNano(), Size: int64(len(content)), Mode: 0644},
		Ref:  obj.Ref(),
	}
}

func (f *fixture) symlink(name, target string) *proto.TreeNode {
	return &proto.TreeNode{
		Stat: &proto.FileInfo{
			Name:       []byte(name),
			Type:       proto.NodeType_NODE_SYMLINK,
			LinkTarget: []byte(target),
			MtimeNs:    f.epoch.UnixNano(),
			Mode:       uint32(os.ModeSymlink | 0777),
		},
	}
}

func (f *fixture) tree(nodes ...*proto.TreeNode) *proto.Object {
	sort.Slice(nodes, func(i, j int) bool { return string(nodes[i].Stat.Name) < string(nodes[j].Stat.Name) })

	obj := proto.NewObject(&proto.Tree{Nodes: nodes})
	require.NoError(f.t, f.store.Put(f.ctx, obj))

	return obj
}

func (f *fixture) dir(name string, nodes ...*proto.TreeNode) *proto.TreeNode {
	return &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755},
		Ref:  f.tree(nodes...).Ref(),
	}
}

// commit stores a commit through the index and waits for its presence
// filter, so every row the test inspects is settled.
func (f *fixture) commit(set string, root *proto.Object, partial bool) *proto.Ref {
	obj := proto.NewObject(&proto.Commit{
		Timestamp: f.clock.Unix(),
		Tree:      root.Ref(),
		BackupSet: set,
		AgentId:   "node-1",
		Partial:   partial,
	})

	require.NoError(f.t, f.x.Put(f.ctx, obj))
	f.presence()

	return obj.Ref()
}

func (f *fixture) pin(target *proto.Ref) (*proto.Ref, error) {
	obj := proto.NewObject(&proto.Pin{Target: target})
	err := f.x.Put(f.ctx, obj)

	return obj.Ref(), err
}

type rangeRow struct {
	path       string
	validFrom  time.Time
	validUntil *time.Time
}

// ranges lists the files or trees rows of an index by path and start.
func (f *fixture) ranges(x *Index, table string) []rangeRow {
	var out []rangeRow

	switch table {
	case "files":
		rows, err := x.client.File.Query().Order(ent.Asc(file.FieldPath), ent.Asc(file.FieldValidFrom)).All(f.ctx)
		require.NoError(f.t, err)
		for _, r := range rows {
			out = append(out, rangeRow{path: r.Path, validFrom: r.ValidFrom.UTC(), validUntil: utc(r.ValidUntil)})
		}
	case "trees":
		rows, err := x.client.Tree.Query().Order(ent.Asc(tree.FieldPath), ent.Asc(tree.FieldValidFrom)).All(f.ctx)
		require.NoError(f.t, err)
		for _, r := range rows {
			out = append(out, rangeRow{path: r.Path, validFrom: r.ValidFrom.UTC(), validUntil: utc(r.ValidUntil)})
		}
	default:
		f.t.Fatalf("no ranges in %s", table)
	}

	return out
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}

	return ptr(t.UTC())
}

func (f *fixture) commitRow(ref *proto.Ref) *ent.CommitRow {
	return f.commitRowIn(f.x, ref)
}

func (f *fixture) commitRowIn(x *Index, ref *proto.Ref) *ent.CommitRow {
	row, err := x.client.CommitRow.Query().Where(commitrow.Ref(ref.Hash)).Only(f.ctx)
	require.NoError(f.t, err)

	return row
}

func (f *fixture) deleted(x *Index, ref *proto.Ref) bool {
	deleted, err := isDeleted(f.ctx, x.client, ref.Hash)
	require.NoError(f.t, err)

	return deleted
}

func (f *fixture) setState(x *Index, name string) string {
	s, err := x.client.Set.Query().Where().All(f.ctx)
	require.NoError(f.t, err)

	for _, row := range s {
		if row.Name == name {
			return string(row.State)
		}
	}

	f.t.Fatalf("no set %q", name)
	return ""
}

func closedAt(t *testing.T, r rangeRow, at time.Time) {
	t.Helper()
	require.NotNil(t, r.validUntil, "%s should be closed", r.path)
	require.True(t, r.validUntil.Equal(at), "%s closed at %s, want %s", r.path, r.validUntil, at)
}

func sameInstant(t *testing.T, got *time.Time, want time.Time) {
	t.Helper()
	require.NotNil(t, got)
	require.True(t, got.Equal(want), "got %s, want %s", got, want)
}

func reindexErr(_ backup.ReIndexReport, err error) error {
	return err
}
