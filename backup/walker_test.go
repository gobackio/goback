package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// memIndex is an Index over memStore that rejects commits with unreachable
// trees, like the real indexes do on Put.
type memIndex struct {
	*memStore
	mtx       sync.Mutex
	latest    map[string]*proto.Ref
	commits   []*proto.Commit
	fetches   int64
	gated     []string
	putSetIDs []uint64
	rescan    bool
	damaged   []string

	// versions are the file nodes FileInfo answers, newest first, by
	// "<set>/<index path>"
	versions map[string][]*proto.TreeNode
	// policy is what BeginCommit grants
	policy *storekey.Policy
	// afterCommit runs once a commit is recorded
	afterCommit func(*proto.Commit)
	putErr      error
}

// fail makes every later Put return err; nil clears it.
func (m *memIndex) fail(err error) {
	m.mtx.Lock()
	m.putErr = err
	m.mtx.Unlock()
}

func newMemIndex(store *memStore) *memIndex {
	return &memIndex{memStore: store, latest: map[string]*proto.Ref{}, versions: map[string][]*proto.TreeNode{}}
}

func (m *memIndex) Open() error  { return nil }
func (m *memIndex) Close() error { return nil }

func (m *memIndex) FileInfo(_ context.Context, set, name string, _ time.Time, count int) ([]*proto.TreeNode, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	nodes := m.versions[set+"/"+name]
	if len(nodes) > count {
		nodes = nodes[:count]
	}

	return nodes, nil
}

func (m *memIndex) CommitInfo(context.Context, string, time.Time, int) ([]*proto.Commit, error) {
	return nil, ErrNotImplemented
}

func (m *memIndex) ReIndex(context.Context) (ReIndexReport, error) { return ReIndexReport{}, nil }

func (m *memIndex) LatestCommit(_ context.Context, set string) (*proto.Ref, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	ref, ok := m.latest[set]
	if !ok {
		return nil, ErrNotFound
	}

	return ref, nil
}

// BeginCommit implements CommitGate: every set is set 42.
func (m *memIndex) BeginCommit(_ context.Context, set string) (*CommitGrant, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.gated = append(m.gated, set)

	return &CommitGrant{Policy: m.policy, Rescan: m.rescan, Damaged: m.damaged}, nil
}

func (m *memIndex) Put(ctx context.Context, obj *proto.Object) error {
	m.mtx.Lock()
	putErr := m.putErr
	m.mtx.Unlock()
	if putErr != nil {
		return putErr
	}

	if commit := obj.GetCommit(); commit != nil {
		err := m.reachable(ctx, commit.Tree)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDanglingRef, err)
		}

		m.mtx.Lock()
		m.putSetIDs = append(m.putSetIDs, commit.SetId)
		m.mtx.Unlock()

		if commit.ReceivedAtNs == 0 {
			obj.Stamp(7, time.Now())
		}

		m.mtx.Lock()
		m.latest[commit.BackupSet] = obj.Ref()
		m.commits = append(m.commits, commit)
		hook := m.afterCommit
		m.mtx.Unlock()

		err = m.memStore.Put(ctx, obj)
		if err == nil && hook != nil {
			hook(commit)
		}

		return err
	}

	return m.memStore.Put(ctx, obj)
}

func (m *memIndex) reachable(ctx context.Context, ref *proto.Ref) error {
	tree, err := LoadTree(ctx, m.memStore, ref)
	if err != nil {
		return err
	}

	for _, node := range tree.Nodes {
		switch {
		case node.Stat.IsDir():
			if err := m.reachable(ctx, node.Ref); err != nil {
				return err
			}
		case node.Ref != nil:
			if ok, _ := m.memStore.Has(ctx, node.Ref); !ok {
				return fmt.Errorf("file %x missing", node.Ref.Hash)
			}
		}
	}

	return nil
}

// GetTree mirrors the server's breadth-first prefetch.
func (m *memIndex) GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error) {
	atomic.AddInt64(&m.fetches, 1)

	type pending struct {
		ref   *proto.Ref
		depth uint32
	}

	var out []*proto.Object
	queue := []pending{{ref: ref}}

	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]

		obj, err := m.memStore.Get(ctx, next.ref)
		if err != nil {
			return nil, err
		}

		out = append(out, obj)
		tree := obj.GetTree()

		for _, split := range tree.GetSplits() {
			queue = append(queue, pending{ref: split, depth: next.depth})
		}

		if next.depth >= maxDepth {
			continue
		}

		for _, node := range tree.GetNodes() {
			if node.Stat.IsDir() {
				queue = append(queue, pending{ref: node.Ref, depth: next.depth + 1})
			}
		}
	}

	return out, nil
}

// countingStore counts object uploads and exposes the index's prefetch.
type countingStore struct {
	*memStore
	index *memIndex
	puts  int64
}

func (c *countingStore) Put(ctx context.Context, obj *proto.Object) error {
	atomic.AddInt64(&c.puts, 1)
	return c.memStore.Put(ctx, obj)
}

func (c *countingStore) GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error) {
	return c.index.GetTree(ctx, ref, maxDepth)
}

type walkerFixture struct {
	t       *testing.T
	root    string
	store   *memStore
	objects *countingStore
	index   *memIndex
	walker  *Walker
}

func newWalkerFixture(t *testing.T) *walkerFixture {
	t.Helper()

	store := newMemStore()
	index := newMemIndex(store)
	f := &walkerFixture{
		t:       t,
		root:    t.TempDir(),
		store:   store,
		objects: &countingStore{memStore: store, index: index},
		index:   index,
	}

	f.walker = &Walker{
		Index:         f.index,
		Objects:       f.objects,
		Set:           "test",
		Metadata:      map[string]string{"env": "test"},
		AgentID:       "agent",
		Root:          f.root,
		FS:            fixtureFS,
		Workers:       4,
		ReadRetries:   3,
		PrefetchDepth: 2,
		clock:         tickingClock(),
	}

	return f
}

// tickingClock is wall time that advances a millisecond per reading.
func tickingClock() func() time.Time {
	var ticks int64

	return func() time.Time {
		return time.Now().Add(time.Duration(atomic.AddInt64(&ticks, 1)) * time.Millisecond)
	}
}

func (f *walkerFixture) write(rel string, content []byte) {
	f.t.Helper()

	full := filepath.Join(f.root, filepath.FromSlash(rel))
	require.NoError(f.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(f.t, os.WriteFile(full, content, 0o644))

	// NTFS publishes a directory's new mtime only once a handle on it is
	// closed; without this a later run sees the directory as changed
	for dir := filepath.Dir(full); len(dir) >= len(f.root); dir = filepath.Dir(dir) {
		handle, err := os.Open(dir)
		require.NoError(f.t, err)
		require.NoError(f.t, handle.Close())
	}
}

func (f *walkerFixture) random(size int) []byte {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(f.t, err)
	return buf
}

// run performs one walk, leaving enough of a gap that mtimes written before
// it sort strictly before its scan start on coarse clocks.
func (f *walkerFixture) run() *WalkResult {
	f.t.Helper()

	time.Sleep(30 * time.Millisecond)
	atomic.StoreInt64(&f.objects.puts, 0)

	result, err := f.walker.Run(context.Background())
	require.NoError(f.t, err)
	require.NotNil(f.t, result.Ref)

	latest, err := f.index.LatestCommit(context.Background(), "test")
	require.NoError(f.t, err)
	require.True(f.t, latest.Equal(result.Ref))

	return result
}

func (f *walkerFixture) tree(ref *proto.Ref) map[string]*proto.TreeNode {
	f.t.Helper()

	tree, err := LoadTree(context.Background(), f.store, ref)
	require.NoError(f.t, err)

	nodes := make(map[string]*proto.TreeNode, len(tree.Nodes))
	for _, node := range tree.Nodes {
		nodes[string(node.Stat.Name)] = node
	}

	return nodes
}

func TestWalkerAsksTheGate(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))

	f.run()

	require.Equal(t, []string{"test"}, f.index.gated, "BeginCommit is asked once per run")
	require.Equal(t, []uint64{0}, f.index.putSetIDs, "the set id is the index's to stamp")
}

func TestWalkerAdoptsTheStorePolicyFromTheGrant(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	f.walker.Key = key

	// a policy the operator never set leaves the key file in charge
	f.index.policy = &storekey.Policy{Version: 0, Mode: storekey.ModeNone}
	result := f.run()
	require.EqualValues(t, 1, result.Commit.PolicyVersion)
	require.Equal(t, storekey.ModeSealed, key.Policy.Mode)

	served := storekey.DefaultPolicy()
	served.Version = 3
	f.index.policy = &served

	f.write("a.txt", []byte("two"))
	result = f.run()
	require.EqualValues(t, 3, result.Commit.PolicyVersion, "the commit records the policy it was written under")
	require.Equal(t, served, key.Policy)

	// an agent without a key cannot honour a policy that encrypts
	f.walker.Key = nil
	_, err = f.walker.Run(context.Background())
	require.ErrorContains(t, err, "no store key")

	f.index.policy.Mode = storekey.ModeNone
	_, err = f.walker.Run(context.Background())
	require.NoError(t, err)

	f.walker.Key = key
	f.write("a.txt", []byte("three"))
	result = f.run()
	require.EqualValues(t, 3, result.Commit.PolicyVersion, "a policy without encryption is still the policy written under")
	require.Same(t, key, f.walker.Key, "the run leaves the caller's key in place")
}

func TestWalkerWithAKeySealsUnderAPolicyThatDoesNotEncrypt(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("small.txt", []byte("secret"))
	f.write("big.bin", f.random(200<<10))

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	f.walker.Key = key
	f.index.policy = &storekey.Policy{Version: 2, Mode: storekey.ModeNone, PresenceScope: "store"}

	result := f.run()
	require.EqualValues(t, 2, result.Commit.PolicyVersion)

	nodes := f.tree(result.Commit.Tree)
	require.Len(t, nodes, 2)
	require.NotContains(t, nodes, "small.txt")
	require.NotContains(t, nodes, "big.bin")

	for _, obj := range f.store.objects {
		require.Nil(t, obj.GetBlob(), "chunks are sealed")

		if file := obj.GetFile(); len(file.GetInline()) > 0 {
			require.Equal(t, proto.Encryption_SEALED, file.InlineEncryption)
		}
	}
}

// remember makes the index answer FileInfo for the root-level files of a
// commit, newest first.
func (f *walkerFixture) remember(commit *proto.Commit) {
	f.t.Helper()

	for name, node := range f.tree(commit.Tree) {
		if node.Stat.GetType() != proto.NodeType_NODE_FILE {
			continue
		}

		key := "test/" + IndexPath(nil, name)
		f.index.versions[key] = append([]*proto.TreeNode{node}, f.index.versions[key]...)
	}
}

func TestWalkerSkipsPartsOfRecentLiveVersions(t *testing.T) {
	f := newWalkerFixture(t)
	old := f.random(300 << 10)
	f.write("a.bin", old)
	f.remember(f.run().Commit)

	f.write("a.bin", f.random(300<<10))
	second := f.run()
	f.remember(second.Commit)
	require.Greater(t, atomic.LoadInt64(&f.objects.puts), int64(2), "new content uploads blobs")

	// content two versions back: its blobs are known through FileInfo
	f.write("a.bin", old)
	third := f.run()
	require.EqualValues(t, 1, third.Read)
	require.EqualValues(t, 2, atomic.LoadInt64(&f.objects.puts), "only the file object and the root tree")

	// beyond the last three versions the blobs are uploaded again
	f.index.versions = map[string][]*proto.TreeNode{}
	f.write("a.bin", append(old, 1))
	f.run()
	require.Greater(t, atomic.LoadInt64(&f.objects.puts), int64(2))
}

func TestWalkerUnchangedRunUploadsOnlyTheCommit(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("hello"))
	f.write("sub/b.bin", f.random(150<<10))
	f.write("sub/deep/c.txt", []byte("deep"))
	f.write("other/d.txt", []byte("other"))

	first := f.run()
	require.Nil(t, first.Base)
	require.EqualValues(t, 7, first.Commit.SetId, "the commit carries what the index stamped")
	require.NotZero(t, first.Commit.ReceivedAtNs)
	require.True(t, proto.NewObject(first.Commit).Ref().Equal(first.Ref), "the result ref is the stamped commit's")
	require.EqualValues(t, 4, first.Files)
	require.EqualValues(t, 4, first.Read)
	require.EqualValues(t, 0, first.Reused)
	require.False(t, first.Commit.Partial)
	require.Equal(t, "agent", first.Commit.AgentId)
	require.Equal(t, map[string]string{"env": "test"}, first.Commit.Metadata)
	require.NotZero(t, first.Commit.ScanStartNs)

	root := f.tree(first.Commit.Tree)
	require.Len(t, root, 3)
	require.True(t, root["sub"].Stat.IsDir())
	require.Equal(t, proto.NodeType_NODE_FILE, root["a.txt"].Stat.Type)

	second := f.run()
	require.True(t, second.Base.Equal(first.Ref))
	require.True(t, second.Commit.Parent.Equal(first.Ref))
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))
	require.EqualValues(t, 4, second.Reused)
	require.EqualValues(t, 0, second.Read)
	require.EqualValues(t, 0, atomic.LoadInt64(&f.objects.puts))
	require.False(t, second.Dirty())
}

func TestWalkerChangedFileRewritesOnlyItsPath(t *testing.T) {
	f := newWalkerFixture(t)
	big := f.random(2 << 20)
	f.write("sub/b.bin", big)
	f.write("sub/deep/c.txt", []byte("deep"))
	f.write("other/d.txt", []byte("other"))

	first := f.run()
	before := f.tree(first.Commit.Tree)

	f.write("sub/b.bin", append(big, f.random(10<<10)...))

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.EqualValues(t, 2, second.Reused)
	require.False(t, second.Commit.Tree.Equal(first.Commit.Tree))

	after := f.tree(second.Commit.Tree)
	require.True(t, after["other"].Ref.Equal(before["other"].Ref), "untouched directory must keep its tree")
	require.False(t, after["sub"].Ref.Equal(before["sub"].Ref))

	sub := f.tree(after["sub"].Ref)
	subBefore := f.tree(before["sub"].Ref)
	require.True(t, sub["deep"].Ref.Equal(subBefore["deep"].Ref))
	require.False(t, sub["b.bin"].Ref.Equal(subBefore["b.bin"].Ref))

	// the unchanged prefix of the file is served from known parts, so fewer
	// blobs than the file has parts get uploaded
	fileObj, err := f.store.Get(context.Background(), sub["b.bin"].Ref)
	require.NoError(t, err)
	parts := len(fileObj.GetFile().Parts)
	require.Greater(t, parts, 1)
	require.Less(t, int(atomic.LoadInt64(&f.objects.puts)), parts+3, "puts: blobs for new parts, file, sub tree, root tree")
}

func TestWalkerRacyRuleRereadsFilesTouchedDuringTheBaseRun(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("racy.txt", []byte("version-1"))

	future := time.Now().Add(time.Hour)
	full := filepath.Join(f.root, "racy.txt")
	require.NoError(t, os.Chtimes(full, future, future))

	first := f.run()
	before := f.tree(first.Commit.Tree)

	// same size, same mtime, different content: only the racy rule catches it
	f.write("racy.txt", []byte("version-2"))
	require.NoError(t, os.Chtimes(full, future, future))

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.EqualValues(t, 0, second.Reused)

	after := f.tree(second.Commit.Tree)
	require.False(t, after["racy.txt"].Ref.Equal(before["racy.txt"].Ref))
}

func TestWalkerDeletedAndAddedEntries(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("keep.txt", []byte("keep"))
	f.write("gone.txt", []byte("gone"))

	first := f.run()

	require.NoError(t, os.Remove(filepath.Join(f.root, "gone.txt")))
	f.write("new.txt", []byte("new"))

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.EqualValues(t, 1, second.Reused)

	after := f.tree(second.Commit.Tree)
	require.Len(t, after, 2)
	require.NotContains(t, after, "gone.txt")
	require.Contains(t, after, "new.txt")
	require.True(t, after["keep.txt"].Ref.Equal(f.tree(first.Commit.Tree)["keep.txt"].Ref))
}

func TestWalkerCarriesWhatLeftTheDiskWhileCarryAllows(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("1", []byte("one"))
	f.write("2", []byte("two"))
	first := f.run()

	require.NoError(t, os.Remove(filepath.Join(f.root, "1")))
	require.NoError(t, os.Remove(filepath.Join(f.root, "2")))
	f.write("3", []byte("three"))
	f.walker.Carry = func(name string) bool { return name != "1" }

	second := f.run()
	require.EqualValues(t, 1, second.Read)

	after := f.tree(second.Commit.Tree)
	require.Len(t, after, 2)
	require.Contains(t, after, "3")
	require.True(t, after["2"].Ref.Equal(f.tree(first.Commit.Tree)["2"].Ref))
}

func TestWalkerPutsRootInSubdirAndCarriesTheOtherEntries(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("1", []byte("one"))
	f.write("2", []byte("two"))
	f.walker.Subdir = "a"
	first := f.run()
	require.Len(t, f.tree(first.Commit.Tree), 1)
	require.Len(t, f.tree(f.tree(first.Commit.Tree)["a"].Ref), 2)

	require.NoError(t, os.Remove(filepath.Join(f.root, "1")))
	require.NoError(t, os.Remove(filepath.Join(f.root, "2")))
	f.write("3", []byte("three"))
	f.walker.Carry = func(name string) bool { return name == "a" || name == "2" }

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	a := f.tree(f.tree(second.Commit.Tree)["a"].Ref)
	require.Len(t, a, 2)
	require.Contains(t, a, "2", "Carry applies inside the subdir")

	f.walker.Subdir = "b"
	f.walker.Carry = func(name string) bool { return name == "a" }

	third := f.run()
	root := f.tree(third.Commit.Tree)
	require.Len(t, root, 2)
	require.True(t, root["a"].Ref.Equal(f.tree(second.Commit.Tree)["a"].Ref))

	b := f.tree(root["b"].Ref)
	require.Len(t, b, 1)
	require.True(t, b["3"].Ref.Equal(f.tree(f.tree(second.Commit.Tree)["a"].Ref)["3"].Ref))

	f.walker.Carry = func(string) bool { return false }

	fourth := f.run()
	require.Len(t, f.tree(fourth.Commit.Tree), 1, "a is dropped once Carry lets it go")
}

func TestWalkerCommitsAStreamedFile(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("ignored.txt", []byte("not walked"))

	content := f.random(3 << 20)
	finished := false
	f.walker.Stream = &Stream{Name: "dump.sql", File: bytes.NewReader(content), Finish: func() error {
		finished = true
		return nil
	}}

	result := f.run()
	require.True(t, finished)

	root := f.tree(result.Commit.Tree)
	require.Len(t, root, 1)
	require.EqualValues(t, len(content), root["dump.sql"].Stat.Size)

	read, err := NewBackupReader(f.store).ReadFile(context.Background(), root["dump.sql"].Ref)
	require.NoError(t, err)
	got, err := io.ReadAll(read)
	require.NoError(t, err)
	require.Equal(t, content, got)

	f.walker.Stream = &Stream{Name: "dump.sql", File: bytes.NewReader(content), Finish: func() error { return errors.New("cut short") }}
	_, err = f.walker.Run(context.Background())
	require.ErrorContains(t, err, "cut short")

	latest, err := f.index.LatestCommit(context.Background(), "test")
	require.NoError(t, err)
	require.True(t, latest.Equal(result.Ref), "a failed Finish leaves the set without a commit")
}

func TestWalkerCommitsATarStreamAsATree(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("ignored.txt", []byte("not walked"))

	content := f.random(3 << 20)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "base/5/16384", Typeflag: tar.TypeReg, Mode: 0o600, Size: int64(len(content))}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "base/", Typeflag: tar.TypeDir, Mode: 0o750}))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "pg_wal", Typeflag: tar.TypeSymlink, Linkname: "/wal"}))
	require.NoError(t, tw.Close())

	var seen []string
	f.walker.Stream = &Stream{
		Tar: bytes.NewReader(buf.Bytes()),
		Inspect: func(hdr *tar.Header) io.Writer {
			seen = append(seen, hdr.Name)
			return nil
		},
	}

	result := f.run()
	require.Equal(t, []string{"base/5/16384", "base/", "pg_wal"}, seen)

	root := f.tree(result.Commit.Tree)
	require.Len(t, root, 2)
	require.Equal(t, "/wal", string(root["pg_wal"].Stat.LinkTarget))
	require.Equal(t, os.ModeDir|0o750, os.FileMode(root["base"].Stat.Mode), "a directory listed after its contents keeps its own mode")

	base, err := OpenTree(context.Background(), f.store, root["base"].Ref, nil, NameToken(nil, nil, []byte("base")))
	require.NoError(t, err)
	require.Len(t, base.Nodes, 1)
	require.Equal(t, os.ModeDir|0o700, os.FileMode(base.Nodes[0].Stat.Mode), "a directory the stream never lists")

	five, err := OpenTree(context.Background(), f.store, base.Nodes[0].Ref, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, len(content), five.Nodes[0].Stat.Size)

	read, err := NewBackupReader(f.store).ReadFile(context.Background(), five.Nodes[0].Ref)
	require.NoError(t, err)
	got, err := io.ReadAll(read)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestWalkerRefusesAStreamEntryOutsideItsRoot(t *testing.T) {
	f := newWalkerFixture(t)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o600}))
	require.NoError(t, tw.Close())

	f.walker.Stream = &Stream{Tar: &buf}

	_, err := f.walker.Run(context.Background())
	require.ErrorContains(t, err, "outside its root")
}

func TestWalkerIncludeFilter(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("logs/x.log", []byte("log"))
	f.write("world/level.dat", []byte("level"))

	f.walker.Include = func(rel string) bool { return rel != "logs" }

	result := f.run()
	root := f.tree(result.Commit.Tree)
	require.Len(t, root, 1)
	require.Contains(t, root, "world")
}

func TestWalkerSymlinkIsRecordedNotFollowed(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("target.txt", []byte("target"))

	err := os.Symlink("target.txt", filepath.Join(f.root, "link"))
	if err != nil && runtime.GOOS == "windows" {
		t.Skipf("symlinks unavailable: %v", err)
	}
	require.NoError(t, err)

	first := f.run()
	require.EqualValues(t, 1, first.Files)

	root := f.tree(first.Commit.Tree)
	link := root["link"]
	require.Equal(t, proto.NodeType_NODE_SYMLINK, link.Stat.Type)
	require.Equal(t, "target.txt", string(link.Stat.LinkTarget))
	require.Nil(t, link.Ref)

	second := f.run()
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))
	require.EqualValues(t, 0, atomic.LoadInt64(&f.objects.puts))
}

func TestWalkerSplitTreesRoundTripAndPrefetch(t *testing.T) {
	f := newWalkerFixture(t)

	const count = 700
	for i := 0; i < count; i++ {
		f.write(fmt.Sprintf("many/file-%04d.txt", i), []byte(fmt.Sprintf("content %d", i)))
	}

	first := f.run()
	require.EqualValues(t, count, first.Read)

	root := f.tree(first.Commit.Tree)
	manyObj, err := f.store.Get(context.Background(), root["many"].Ref)
	require.NoError(t, err)
	require.NotEmpty(t, manyObj.GetTree().Splits, "a directory this large is split")
	require.Empty(t, manyObj.GetTree().Nodes)

	many := f.tree(root["many"].Ref)
	require.Len(t, many, count)

	f.write("many/file-0350.txt", []byte("changed"))

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.EqualValues(t, count-1, second.Reused)
	require.Greater(t, atomic.LoadInt64(&f.index.fetches), int64(0), "base trees come through GetTree")

	after := f.tree(f.tree(second.Commit.Tree)["many"].Ref)
	require.Len(t, after, count)
	require.False(t, after["file-0350.txt"].Ref.Equal(many["file-0350.txt"].Ref))
	require.True(t, after["file-0000.txt"].Ref.Equal(many["file-0000.txt"].Ref))
}

func TestWalkerCheckpointsWritePartialCommits(t *testing.T) {
	f := newWalkerFixture(t)
	for i := 0; i < 5; i++ {
		f.write(fmt.Sprintf("dir-%d/file.txt", i), []byte(fmt.Sprint(i)))
	}

	f.walker.CheckpointInterval = time.Nanosecond

	result := f.run()
	require.Greater(t, result.Checkpoints, int64(0))
	require.False(t, result.Commit.Partial)

	partial := 0
	for _, commit := range f.index.commits {
		if commit.Partial {
			partial++
			require.EqualValues(t, result.Commit.ScanStartNs, commit.ScanStartNs)
		}
	}
	require.EqualValues(t, result.Checkpoints, partial)

	// the final commit is a valid diff base; directory mtimes may still
	// settle after the first run, so compare the subtrees rather than the root
	f.walker.CheckpointInterval = 0
	second := f.run()
	require.EqualValues(t, 5, second.Reused)
	require.EqualValues(t, 0, second.Read)

	before, after := f.tree(result.Commit.Tree), f.tree(second.Commit.Tree)
	for name, node := range before {
		require.True(t, after[name].Ref.Equal(node.Ref), name)
	}
}

func TestWalkerMarksQuiescedCleanRunsConsistent(t *testing.T) {
	f := newWalkerFixture(t)
	for i := 0; i < 3; i++ {
		f.write(fmt.Sprintf("dir-%d/file.txt", i), []byte(fmt.Sprint(i)))
	}

	first := f.run()
	require.False(t, first.Commit.Consistent, "no pre hook, no claim")

	f.walker.Quiesced = true
	f.walker.CheckpointInterval = time.Nanosecond
	f.write("dir-0/file.txt", []byte("changed"))

	second := f.run()
	require.True(t, second.Commit.Consistent)
	require.Greater(t, second.Checkpoints, int64(0))

	for _, commit := range f.index.commits {
		if commit.Partial {
			require.False(t, commit.Consistent, "a checkpoint is never consistent")
		}
	}
}

func TestWalkerUnreadableFileKeepsPreviousVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits are not enforced on windows")
	}

	f := newWalkerFixture(t)
	f.write("secret.txt", []byte("v1"))

	first := f.run()
	before := f.tree(first.Commit.Tree)

	full := filepath.Join(f.root, "secret.txt")
	f.write("secret.txt", []byte("v2-longer"))
	require.NoError(t, os.Chmod(full, 0))
	t.Cleanup(func() { _ = os.Chmod(full, 0o644) })

	second := f.run()
	require.EqualValues(t, 1, second.Unreadable)
	require.True(t, second.Dirty())
	require.True(t, f.tree(second.Commit.Tree)["secret.txt"].Ref.Equal(before["secret.txt"].Ref))
}

func TestWalkerRejectsCommitWithDanglingTree(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))

	first := f.run()
	root := f.tree(first.Commit.Tree)
	require.NoError(t, f.store.Delete(context.Background(), root["a.txt"].Ref))

	commit := proto.NewObject(&proto.Commit{Timestamp: time.Now().Unix(), Tree: first.Commit.Tree, BackupSet: "test"})
	err := f.index.Put(context.Background(), commit)
	require.True(t, errors.Is(err, ErrDanglingRef), "got %v", err)
}

func TestHashFileMatchesStoredRef(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("big.bin", f.random(300<<10))

	result := f.run()
	root := f.tree(result.Commit.Tree)

	ref, err := HashFile(filepath.Join(f.root, "big.bin"), nil)
	require.NoError(t, err)
	require.True(t, ref.Equal(root["big.bin"].Ref))
}

// memSessions records the sessions a walker opens and closes.
type memSessions struct {
	mtx   sync.Mutex
	began []*Session
	ended []*Session
}

func (m *memSessions) BeginSession(ctx context.Context, s *Session) (context.Context, error) {
	s.ID = fmt.Sprintf("session-%d", len(m.began)+1)

	m.mtx.Lock()
	m.began = append(m.began, s)
	m.mtx.Unlock()

	return WithSession(ctx, s), nil
}

func (m *memSessions) EndSession(ctx context.Context) error {
	s, ok := SessionFromContext(ctx)
	if !ok {
		return errors.New("no session on the context")
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	if slices.Contains(m.ended, s) {
		return ErrNoSession
	}

	m.ended = append(m.ended, s)

	return nil
}

func TestWalkerRunsInASessionAndUploadsEachChunkOnce(t *testing.T) {
	content := make([]byte, 300<<10)
	_, err := rand.Read(content)
	require.NoError(t, err)

	single := newWalkerFixture(t)
	single.write("a.bin", content)
	singlePuts := single.run().Reused
	singlePuts = atomic.LoadInt64(&single.objects.puts)

	f := newWalkerFixture(t)
	sessions := &memSessions{}
	f.walker.Sessions = sessions
	f.write("a.bin", content)
	f.write("b.bin", content)

	result := f.run()
	require.EqualValues(t, 2, result.Files)
	require.Equal(t, singlePuts+1, atomic.LoadInt64(&f.objects.puts), "the second copy costs one file object, its chunks are not uploaded again")

	require.Len(t, sessions.began, 1)
	require.Len(t, sessions.ended, 1)
	require.Same(t, sessions.began[0], sessions.ended[0])
	require.Equal(t, "agent", sessions.began[0].AgentID)
	require.Equal(t, "test", sessions.began[0].Set)
}

// losingIndex refuses the first commit with ErrSessionLost and ends its
// session, as a store does when a collection took what the run reused;
// it refuses anything later put in an ended session.
type losingIndex struct {
	*memIndex
	sessions *memSessions
	lost     atomic.Bool
}

func (l *losingIndex) Put(ctx context.Context, obj *proto.Object) error {
	s, _ := SessionFromContext(ctx)

	l.sessions.mtx.Lock()
	ended := slices.Contains(l.sessions.ended, s)
	l.sessions.mtx.Unlock()

	if ended {
		return ErrNoSession
	}

	if obj.GetCommit() != nil && l.lost.CompareAndSwap(false, true) {
		_ = l.sessions.EndSession(ctx)
		return ErrSessionLost
	}

	return l.memIndex.Put(ctx, obj)
}

func TestWalkerRetriesALostSessionInANewOne(t *testing.T) {
	f := newWalkerFixture(t)
	sessions := &memSessions{}
	f.walker.Sessions = sessions
	f.walker.Index = &losingIndex{memIndex: f.index, sessions: sessions}
	f.walker.LostRetries = 1
	f.write("a.txt", []byte("a"))

	f.run()

	require.Len(t, sessions.began, 2)
	require.Equal(t, sessions.began, sessions.ended)
}

// TestWalkerCheckpointKeepsTheBaseRunsRacyWindow interrupts a run right
// after its first checkpoint, which carries a base node the run has not
// verified, and expects the next run to still re-read that file.
func TestWalkerCheckpointKeepsTheBaseRunsRacyWindow(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a-dir/file.txt", []byte("dir"))
	f.write("racy.txt", []byte("version-1"))

	// recorded with an mtime inside the first run's window
	racy := filepath.Join(f.root, "racy.txt")
	touched := time.Now().Add(100 * time.Millisecond)
	require.NoError(t, os.Chtimes(racy, touched, touched))

	first := f.run()
	require.GreaterOrEqual(t, touched.UnixNano(), first.Commit.ScanStartNs)
	before := f.tree(first.Commit.Tree)

	// rewritten within the same tick, same size, before the second run
	f.write("racy.txt", []byte("version-2"))
	require.NoError(t, os.Chtimes(racy, touched, touched))
	time.Sleep(200 * time.Millisecond)

	// the second run checkpoints after a-dir and then loses the store
	f.index.afterCommit = func(commit *proto.Commit) {
		if commit.Partial {
			f.index.fail(errors.New("store went away"))
		}
	}
	f.walker.CheckpointInterval = time.Nanosecond
	_, err := f.walker.Run(context.Background())
	require.ErrorContains(t, err, "store went away")
	f.index.afterCommit = nil
	f.index.fail(nil)

	latest, err := f.index.LatestCommit(context.Background(), "test")
	require.NoError(t, err)
	obj, err := f.store.Get(context.Background(), latest)
	require.NoError(t, err)
	require.True(t, obj.GetCommit().Partial, "the checkpoint is the diff base now")
	require.Equal(t, first.Commit.ScanStartNs, obj.GetCommit().ScanStartNs, "the checkpoint keeps the base run's scan start")

	f.walker.CheckpointInterval = 0
	third := f.run()
	after := f.tree(third.Commit.Tree)
	require.False(t, after["racy.txt"].Ref.Equal(before["racy.txt"].Ref), "the rewrite is backed up")
}

// TestWalkerRereadsAFileWrittenThroughAHeldHandle writes a file through a
// handle that stays open across runs, as a server log is written, and
// expects the second run to pick the new content up.
func TestWalkerRereadsAFileWrittenThroughAHeldHandle(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("held.log", []byte("aaaa"))

	handle, err := os.OpenFile(filepath.Join(f.root, "held.log"), os.O_WRONLY, 0)
	require.NoError(t, err)
	defer handle.Close()

	first := f.run()
	require.Equal(t, "aaaa", f.content(first.Commit.Tree, "held.log"))

	time.Sleep(50 * time.Millisecond)
	_, err = handle.WriteAt([]byte("bbbb"), 0)
	require.NoError(t, err)

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.Equal(t, "bbbb", f.content(second.Commit.Tree, "held.log"))
}

func TestWalkerRefusesAnUnlistableRootOnTheFirstRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions are ACLs on Windows")
	}

	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))
	require.NoError(t, os.Chmod(f.root, 0))
	t.Cleanup(func() { _ = os.Chmod(f.root, 0o755) })

	_, err := f.walker.Run(context.Background())
	require.Error(t, err)

	_, err = f.index.LatestCommit(context.Background(), "test")
	require.ErrorIs(t, err, ErrNotFound, "no empty commit was written")
}

// content returns the inline content of a small file in a commit's tree.
func (f *walkerFixture) content(tree *proto.Ref, name string) string {
	f.t.Helper()

	node := f.tree(tree)[name]
	require.NotNil(f.t, node, name)

	obj, err := f.store.Get(context.Background(), node.Ref)
	require.NoError(f.t, err)

	return string(obj.GetFile().GetInline())
}

func TestKnownPartsReachThePartsOfSplitFilesAndEarlierVersions(t *testing.T) {
	f := newWalkerFixture(t)
	ctx := context.Background()

	put := func(obj *proto.Object) *proto.Object {
		require.NoError(t, f.store.Put(ctx, obj))
		return obj
	}

	blob := func(data string) *proto.Ref {
		return put(proto.NewObject(&proto.Blob{Data: []byte(data)})).Ref()
	}

	part := func(ref *proto.Ref, offset uint64) *proto.FilePart {
		return &proto.FilePart{Ref: ref, Offset: offset, Length: 1}
	}

	a, b, c, d := blob("a"), blob("b"), blob("c"), blob("d")

	first := put(proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(a, 0)}}))
	second := put(proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(b, 0)}}))
	split := put(proto.NewObject(&proto.File{
		Splits:       []*proto.Ref{first.Ref(), second.Ref()},
		SplitLengths: []uint64{1, 1},
		SplitDepth:   1,
	}))

	older := put(proto.NewObject(&proto.File{Parts: []*proto.FilePart{part(c, 0), part(d, 1)}}))
	f.index.versions["test/big.bin"] = []*proto.TreeNode{{Ref: split.Ref()}, {Ref: older.Ref()}, {Ref: blob("not a file")}}

	node := &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("big.bin"), Type: proto.NodeType_NODE_FILE}, Ref: split.Ref()}

	known := f.walker.knownParts(ctx, "big.bin", node)

	want := map[string]struct{}{}
	for _, ref := range []*proto.Ref{a, b, c, d} {
		want[string(ref.Hash)] = struct{}{}
	}

	require.Equal(t, want, known)
}

// identityCache is a StatCache that compares what FileIdentity reads.
type identityCache struct {
	mtx  sync.Mutex
	seen map[string][2]any
}

func (c *identityCache) identity(info os.FileInfo) [2]any {
	ctime, id := FileIdentity(info)
	return [2]any{[2]int64{ctime, int64(id)}, [2]int64{info.ModTime().UnixNano(), info.Size()}}
}

func (c *identityCache) Lookup(path string, info os.FileInfo) (bool, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	seen, ok := c.seen[path]

	return ok, seen == c.identity(info)
}

func (c *identityCache) Store(path string, info os.FileInfo, _ *proto.Ref) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.seen[path] = c.identity(info)

	return nil
}

func TestTheStatCacheCatchesARewriteThatKeepsMtimeAndSize(t *testing.T) {
	f := newWalkerFixture(t)
	f.walker.FS = LocalFS{}
	f.walker.Cache = &identityCache{seen: map[string][2]any{}}
	f.write("a.txt", []byte("version-1"))

	path := filepath.Join(f.root, "a.txt")
	info, err := os.Stat(path)
	require.NoError(t, err)

	f.run()

	f.write("a.txt", []byte("version-2"))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))

	second := f.run()
	require.EqualValues(t, 1, second.Read)
	require.Equal(t, "version-2", f.content(second.Commit.Tree, "a.txt"))
}

func TestAnUnchangedDirectoryIsTakenFromThePreviousCommitUnread(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("quiet/a.txt", []byte("one"))
	f.write("busy/b.txt", []byte("one"))
	f.run()

	f.write("quiet/a.txt", []byte("two, which the hint says did not happen"))
	f.write("busy/b.txt", []byte("two"))
	f.write("fresh/c.txt", []byte("new"))
	f.walker.Unchanged = func(rel string) bool { return rel == "quiet" || rel == "fresh" }

	second := f.run()
	require.EqualValues(t, 2, second.Read, "busy/b.txt and fresh/c.txt, which the previous commit lacks")
	require.Equal(t, "one", f.content(f.tree(second.Commit.Tree)["quiet"].Ref, "a.txt"))
	require.Equal(t, "two", f.content(f.tree(second.Commit.Tree)["busy"].Ref, "b.txt"))

	f.walker.Verify = true
	third := f.run()
	require.Equal(t, "two, which the hint says did not happen", f.content(f.tree(third.Commit.Tree)["quiet"].Ref, "a.txt"))
}
