package backup

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// presenceStore hands out filters and confirms assumed parts against the
// underlying store, storing a file object only once every part exists.
type presenceStore struct {
	*countingStore
	filters presence.Set

	mtx       sync.Mutex
	confirms  int
	missing   int
	onConfirm func()
}

func (p *presenceStore) Presence(context.Context, string) (presence.Set, error) {
	return p.filters, nil
}

func (p *presenceStore) PutAssuming(ctx context.Context, obj *proto.Object, assumed []*proto.Ref) ([]*proto.Ref, error) {
	p.mtx.Lock()
	p.confirms++
	hook := p.onConfirm
	p.onConfirm = nil
	p.mtx.Unlock()

	if hook != nil {
		hook()
	}

	missing, err := Missing(ctx, p.memStore, assumed)
	if err != nil {
		return nil, err
	}

	if len(missing) > 0 {
		p.mtx.Lock()
		p.missing += len(missing)
		p.mtx.Unlock()

		return missing, nil
	}

	return nil, p.Put(ctx, obj)
}

// filterOf builds the filter of the latest commit's tree.
func (f *walkerFixture) filterOf(ref *proto.Ref) *presence.Filter {
	f.t.Helper()

	obj, err := f.store.Get(context.Background(), ref)
	require.NoError(f.t, err)

	filter, err := CollectPresence(context.Background(), f.store, obj.GetCommit().Tree)
	require.NoError(f.t, err)

	return filter
}

// fileParts returns the parts of the file at name in the root tree.
func (f *walkerFixture) fileParts(commit *proto.Ref, name string) []*proto.FilePart {
	f.t.Helper()

	obj, err := f.store.Get(context.Background(), commit)
	require.NoError(f.t, err)

	node := f.tree(obj.GetCommit().Tree)[name]
	require.NotNil(f.t, node)

	file, err := f.store.Get(context.Background(), node.Ref)
	require.NoError(f.t, err)

	return file.GetFile().GetParts()
}

func (f *walkerFixture) requireComplete(commit *proto.Ref, name string) []*proto.FilePart {
	f.t.Helper()

	parts := f.fileParts(commit, name)
	require.NotEmpty(f.t, parts)

	for _, part := range parts {
		has, err := f.store.Has(context.Background(), part.Ref)
		require.NoError(f.t, err)
		require.True(f.t, has, "part %x of %s is missing", part.Ref.Hash, name)
	}

	return parts
}

func withPresence(f *walkerFixture, filters presence.Set) *presenceStore {
	ps := &presenceStore{countingStore: f.objects, filters: filters}
	f.walker.Objects = ps

	return ps
}

func TestCollectPresenceListsEveryPart(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.bin", f.random(200<<10))
	f.write("dir/b.bin", f.random(150<<10))
	f.write("small.txt", []byte("inline"))

	result := f.run()
	filter := f.filterOf(result.Ref)

	partsA := f.fileParts(result.Ref, "a.bin")
	require.NotEmpty(t, partsA)
	for _, part := range partsA {
		require.True(t, filter.Test(part.Ref.Hash))
	}

	obj, err := f.store.Get(context.Background(), result.Ref)
	require.NoError(t, err)
	dir := f.tree(obj.GetCommit().Tree)["dir"]
	sub, err := LoadTree(context.Background(), f.store, dir.Ref)
	require.NoError(t, err)
	fileB, err := f.store.Get(context.Background(), sub.Nodes[0].Ref)
	require.NoError(t, err)

	total := uint64(len(partsA))
	for _, part := range fileB.GetFile().GetParts() {
		require.True(t, filter.Test(part.Ref.Hash))
		total++
	}

	require.Equal(t, total, filter.Entries(), "blob refs only, no trees or files")

	// no single absent ref is guaranteed to test negative, so what is
	// asserted is the rate the filter was sized for
	const absent = 1000

	positives := 0
	for i := 0; i < absent; i++ {
		if filter.Test(testRef("elsewhere-" + strconv.Itoa(i)).Hash) {
			positives++
		}
	}

	require.Less(t, positives, absent/10, "%d of %d unknown refs tested positive, far above the %v the filter is sized for",
		positives, absent, presence.FalsePositiveRate)
}

func TestWalkerPresenceSkipsConfirmedChunks(t *testing.T) {
	content := make([]byte, 300<<10)
	copy(content, []byte("presence"))

	f := newWalkerFixture(t)
	f.write("a.bin", content)
	first := f.run()
	parts := f.requireComplete(first.Ref, "a.bin")

	ps := withPresence(f, presence.Set{f.filterOf(first.Ref)})
	f.write("b.bin", content)

	second := f.run()
	f.requireComplete(second.Ref, "b.bin")
	require.EqualValues(t, 2, atomic.LoadInt64(&f.objects.puts), "one file object and one tree: the chunks were confirmed, not uploaded")
	require.EqualValues(t, len(parts), second.Assumed)
	require.Zero(t, second.Repaired)
	require.Equal(t, 1, ps.confirms)
	require.Zero(t, ps.missing)
}

func TestWalkerPresenceRepairsFalsePositives(t *testing.T) {
	content := make([]byte, 300<<10)
	copy(content, []byte("false positive"))

	// a filter from a store that holds the content
	donor := newWalkerFixture(t)
	donor.write("a.bin", content)
	filter := donor.filterOf(donor.run().Ref)

	for _, window := range []int{DefaultWindowBytes, 1} {
		f := newWalkerFixture(t)
		f.walker.WindowBytes = window
		ps := withPresence(f, presence.Set{filter})
		f.write("a.bin", content)

		result := f.run()
		parts := f.requireComplete(result.Ref, "a.bin")

		require.EqualValues(t, len(parts), result.Assumed, "window %d", window)
		require.EqualValues(t, len(parts), result.Repaired, "window %d", window)
		require.EqualValues(t, int64(len(parts))+2+descriptorPuts, atomic.LoadInt64(&f.objects.puts), "every chunk once, then file and tree")
		require.Equal(t, 2, ps.confirms, "the file is put again once the parts are there")
		require.Equal(t, len(parts), ps.missing)
	}
}

func TestWalkerPresenceRereadsFileChangedUnderConfirmation(t *testing.T) {
	content := make([]byte, 300<<10)
	copy(content, []byte("original"))
	changed := make([]byte, 320<<10)
	copy(changed, []byte("rewritten"))

	donor := newWalkerFixture(t)
	donor.write("a.bin", content)
	donorResult := donor.run()
	filter := donor.filterOf(donorResult.Ref)

	f := newWalkerFixture(t)
	f.walker.WindowBytes = 1
	ps := withPresence(f, presence.Set{filter})
	f.write("a.bin", content)

	path := filepath.Join(f.root, "a.bin")
	ps.onConfirm = func() {
		require.NoError(t, os.WriteFile(path, changed, 0o644))
	}

	result := f.run()
	parts := f.requireComplete(result.Ref, "a.bin")

	expected, err := HashFile(path, nil)
	require.NoError(t, err)

	obj, err := f.store.Get(context.Background(), result.Ref)
	require.NoError(t, err)
	require.True(t, f.tree(obj.GetCommit().Tree)["a.bin"].Ref.Equal(expected), "the recorded version is the rewritten file")

	var size uint64
	for _, part := range parts {
		size += part.Length
	}
	require.EqualValues(t, len(changed), size)
	require.Equal(t, 1, ps.confirms, "the rewritten content matches no filter, so the second read assumes nothing")
	require.Equal(t, len(donor.fileParts(donorResult.Ref, "a.bin")), ps.missing, "every original part was reported missing once")
}

func TestWalkerIgnoresFiltersWithoutConfirmer(t *testing.T) {
	content := make([]byte, 300<<10)
	copy(content, []byte("no confirmer"))

	donor := newWalkerFixture(t)
	donor.write("a.bin", content)
	filter := donor.filterOf(donor.run().Ref)

	f := newWalkerFixture(t)
	f.walker.Objects = &sourceOnly{countingStore: f.objects, filters: presence.Set{filter}}
	f.write("a.bin", content)

	result := f.run()
	parts := f.requireComplete(result.Ref, "a.bin")
	require.Zero(t, result.Assumed)
	require.EqualValues(t, int64(len(parts))+2+descriptorPuts, atomic.LoadInt64(&f.objects.puts))
}

type sourceOnly struct {
	*countingStore
	filters presence.Set
}

func (s *sourceOnly) Presence(context.Context, string) (presence.Set, error) {
	return s.filters, nil
}

func TestChunkWindowEvictsOldest(t *testing.T) {
	w := newChunkWindow(10)
	blob := func(name string) *proto.Object { return proto.NewObject(&proto.Blob{Data: []byte(name)}) }

	w.put("a", blob("a"), 4)
	w.put("b", blob("b"), 4)
	w.put("c", blob("c"), 4)
	require.Nil(t, w.take("a"), "evicted for c")
	require.NotNil(t, w.take("b"))
	require.Nil(t, w.take("b"), "taken once")

	w.put("big", blob("big"), 11)
	require.Nil(t, w.take("big"), "over budget, never kept")

	w.drop([]string{"c"})
	require.Nil(t, w.take("c"))
	require.Zero(t, w.used)

	var nilWindow *chunkWindow
	nilWindow.put("x", blob("x"), 1)
	require.Nil(t, nilWindow.take("x"))
}

func TestMissingKeepsOrder(t *testing.T) {
	store := newMemStore()
	a := proto.NewObject(&proto.Blob{Data: []byte("a")})
	require.NoError(t, store.Put(context.Background(), a))

	b, c := testRef("b"), testRef("c")
	missing, err := Missing(context.Background(), store, []*proto.Ref{b, a.Ref(), c})
	require.NoError(t, err)
	require.Equal(t, []*proto.Ref{b, c}, missing)
}

// batchStore reads records together and counts the objects asked for one
// at a time.
type batchStore struct {
	*memStore
	gets atomic.Int64
}

func (r *batchStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	r.gets.Add(1)
	return r.memStore.Get(ctx, ref)
}

func (r *batchStore) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	out := make([]*proto.Object, len(refs))
	for i, ref := range refs {
		out[i], _ = r.memStore.Get(ctx, ref)
	}

	return out, nil
}

func TestAFilterIsCollectedInRecordReadsWithoutGettingObjectsOneByOne(t *testing.T) {
	ctx := context.Background()
	store := &batchStore{memStore: newMemStore()}
	put := func(msg any) *proto.Ref {
		obj := proto.NewObject(msg)
		require.NoError(t, store.Put(ctx, obj))

		return obj.Ref()
	}

	var blobs []*proto.Ref

	fileIn := func(n int) *proto.TreeNode {
		blob := put(&proto.Blob{Data: []byte("blob " + strconv.Itoa(n))})
		blobs = append(blobs, blob)
		file := put(&proto.File{Parts: []*proto.FilePart{{Ref: blob, Length: 6}}})

		return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("f" + strconv.Itoa(n)), Type: proto.NodeType_NODE_FILE}, Ref: file}
	}

	var dirs []*proto.TreeNode
	for d := range 3 {
		sub := put(&proto.Tree{Nodes: []*proto.TreeNode{fileIn(2 * d), fileIn(2*d + 1)}})
		dirs = append(dirs, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte("d" + strconv.Itoa(d)), Type: proto.NodeType_NODE_DIRECTORY}, Ref: sub})
	}

	first := put(&proto.Tree{Nodes: append(dirs, fileIn(6))})
	second := put(&proto.Tree{Nodes: []*proto.TreeNode{fileIn(7)}})
	root := put(&proto.Tree{Splits: []*proto.Ref{first, second}})

	filter, err := CollectPresence(ctx, store, root)
	require.NoError(t, err)

	for _, blob := range blobs {
		require.True(t, filter.Test(blob.Hash), "every part of every file, a split tree's too")
	}

	require.Zero(t, store.gets.Load(), "every tree and file came from record reads")
}
