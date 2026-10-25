package pack

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/daead"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// legacy writes sealed data the way a store held it before Reseal: each
// blob under the ref of its plaintext's digest, sealed bound to that ref.
type legacy struct {
	t     *testing.T
	store *PackStorage
	key   *storekey.Key
	aead  tink.DeterministicAEAD
	// written holds what only the data written this way reaches
	written []*proto.Ref
}

func newLegacy(t *testing.T, store *PackStorage, key *storekey.Key) *legacy {
	t.Helper()

	data, err := key.Marshal()
	require.NoError(t, err)

	var file struct {
		Seal json.RawMessage `json:"seal"`
	}
	require.NoError(t, json.Unmarshal(data, &file))

	handle, err := storekey.ReadKeyset(file.Seal)
	require.NoError(t, err)

	aead, err := daead.New(handle)
	require.NoError(t, err)

	return &legacy{t: t, store: store, key: key, aead: aead}
}

func (l *legacy) blob(chunk []byte) *proto.Ref {
	l.t.Helper()

	ref := proto.HashPayload(proto.ObjectType_BLOB, l.key.Digest(chunk))
	stored, compression := proto.Encode(chunk)

	sealed, err := l.aead.EncryptDeterministically(stored, append([]byte("blob"), ref.Hash...))
	require.NoError(l.t, err)

	_, err = l.store.writeRecord(context.Background(), &proto.ObjectHeader{
		Ref: ref, Type: proto.ObjectType_BLOB, Compression: compression, Encryption: proto.Encryption_SEALED,
		KeyId: l.key.ID(), StoredHash: proto.StoredHash(sealed),
	}, sealed)
	require.NoError(l.t, err)

	l.written = append(l.written, ref)

	return ref
}

// file stores content as a file of parts of size bytes.
func (l *legacy) file(content []byte, size int) *proto.Ref {
	l.t.Helper()

	var parts []*proto.FilePart
	for offset := 0; offset < len(content); offset += size {
		chunk := content[offset:min(offset+size, len(content))]
		parts = append(parts, &proto.FilePart{Offset: uint64(offset), Length: uint64(len(chunk)), Ref: l.blob(chunk)})
	}

	return l.put(&proto.File{Parts: parts})
}

func (l *legacy) put(message any) *proto.Ref {
	l.t.Helper()

	obj := proto.NewObject(message)
	obj.KeyId = l.key.ID()
	require.NoError(l.t, l.store.Put(context.Background(), obj))

	l.written = append(l.written, obj.Ref())

	return obj.Ref()
}

// tree stores the nodes as the directory whose token is parent.
func (l *legacy) tree(parent []byte, nodes ...*proto.TreeNode) *proto.Ref {
	l.t.Helper()

	ref, err := backup.PutTree(context.Background(), l.store, nodes, l.key, parent)
	require.NoError(l.t, err)

	l.written = append(l.written, ref)

	return ref
}

func (l *legacy) commit(set string, setID uint64, at int64, tree, parent *proto.Ref) *proto.Ref {
	l.t.Helper()

	return l.put(&proto.Commit{
		Timestamp: at, Tree: tree, BackupSet: set, Parent: parent, AgentId: "agent", SetId: setID,
		ReceivedAtNs: at * int64(time.Second), Metadata: map[string]string{"env": "test"}, KeyId: l.key.ID(),
	})
}

func fileNode(name string, size int, ref *proto.Ref) *proto.TreeNode {
	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Mode: 0o644, Size: int64(size), Type: proto.NodeType_NODE_FILE}, Ref: ref}
}

func dirNode(name string, ref *proto.Ref) *proto.TreeNode {
	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Mode: 0o755, Type: proto.NodeType_NODE_DIRECTORY}, Ref: ref}
}

// resealFixture is a store holding sealed commits in two sets under two
// keys, a third set written in the clear, pins of a commit and of a tree,
// a commit in the trash and the content each commit should restore.
type resealFixture struct {
	base       string
	store      *PackStorage
	keys       map[string]*storekey.Key
	first      *legacy
	commits    map[string]*proto.Ref
	plain      *proto.Ref
	want       map[string]map[string][]byte
	pinnedTree *proto.Ref
}

func newResealFixture(t *testing.T, a, b *storekey.Key) *resealFixture {
	t.Helper()
	ctx := context.Background()

	f := &resealFixture{
		base:    t.TempDir(),
		keys:    map[string]*storekey.Key{string(a.ID()): a, string(b.ID()): b},
		commits: map[string]*proto.Ref{},
		want:    map[string]map[string][]byte{},
	}
	f.store = newTestStore(t, f.base)

	random := rand.New(rand.NewSource(1))
	data := func(size int, compressible bool) []byte {
		out := make([]byte, size)
		random.Read(out)

		if compressible {
			for i := range out {
				out[i] = 'a' + out[i]&7
			}
		}

		return out
	}

	big, meta, text, text2, other := data(200<<10, false), data(5000, true), data(100<<10, true), data(100<<10, true), data(150<<10, false)
	small := []byte("a small file kept inline")

	f.first = newLegacy(t, f.store, a)
	second := newLegacy(t, f.store, b)

	inline := proto.NewObject(&proto.File{Inline: a.SealInline(small), InlineEncryption: proto.Encryption_SEALED})
	inline.KeyId = a.ID()
	require.NoError(t, f.store.Put(ctx, inline))

	bigFile := f.first.file(big, 64<<10)
	withStream := fileNode("a.bin", len(big), bigFile)
	withStream.Stat.Streams = []*proto.Stream{{Name: []byte("meta"), Ref: f.first.file(meta, 4096), Size: int64(len(meta))}}

	subToken := backup.NameToken(a, nil, []byte("sub"))

	tree1 := f.first.tree(nil, withStream, fileNode("small.txt", len(small), inline.Ref()),
		dirNode("sub", f.first.tree(subToken, fileNode("b.txt", len(text), f.first.file(text, 32<<10)))))
	f.commits["c1"] = f.first.commit("world", 1, 100, tree1, nil)

	// a file with a hole is split, as a backup writes one
	head, tail := data(4096, false), data(4096, true)
	leaf := second.put(&proto.File{Parts: []*proto.FilePart{
		{Offset: 0, Length: 4096, Ref: second.blob(head)},
		{Offset: 4096, Length: 8192, Hole: 8192},
		{Offset: 12288, Length: 4096, Ref: second.blob(tail)},
	}})
	holed := second.put(&proto.File{Splits: []*proto.Ref{leaf}, SplitLengths: []uint64{16384}, SplitDepth: 1})

	tree3 := second.tree(nil, fileNode("c.bin", len(other), second.file(other, 64<<10)), fileNode("holed.bin", 16384, holed))
	f.commits["c3"] = second.commit("other", 2, 200, tree3, nil)

	f.pinnedTree = f.first.tree(subToken, fileNode("b.txt", len(text2), f.first.file(text2, 32<<10)))
	tree2 := f.first.tree(nil, withStream, fileNode("small.txt", len(small), inline.Ref()), dirNode("sub", f.pinnedTree))
	f.commits["c2"] = f.first.commit("world", 1, 300, tree2, f.commits["c1"])

	plainBlob := proto.NewObject(&proto.Blob{Data: []byte("written in the clear")})
	plainFile := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: uint64(len(plainBlob.GetBlob().Data)), Ref: plainBlob.Ref()}}})
	require.NoError(t, f.store.Put(ctx, plainBlob))
	require.NoError(t, f.store.Put(ctx, plainFile))

	plainTree, err := backup.PutTree(ctx, f.store, []*proto.TreeNode{fileNode("p.txt", 20, plainFile.Ref())}, nil, nil)
	require.NoError(t, err)

	plain := proto.NewObject(&proto.Commit{Timestamp: 250, Tree: plainTree, BackupSet: "plain", AgentId: "agent", SetId: 3, ReceivedAtNs: 250 * int64(time.Second), KeyId: []byte{}})
	require.NoError(t, f.store.Put(ctx, plain))
	f.plain = plain.Ref()

	require.NoError(t, f.store.Put(ctx, proto.NewObject(&proto.Pin{Target: f.commits["c1"], ReceivedAtNs: 1, Metadata: map[string]string{"why": "release"}})))
	require.NoError(t, f.store.Put(ctx, proto.NewObject(&proto.Pin{Target: f.pinnedTree, ReceivedAtNs: 2})))
	require.NoError(t, f.store.Put(ctx, proto.NewObject(&proto.Policy{Sequence: 7, WrittenAtNs: 1,
		Scope: &proto.Policy_Commit{Commit: &proto.CommitScope{Commit: f.commits["c2"], DeletedAtNs: 5}}})))
	require.NoError(t, f.store.Flush())

	f.want["c1"] = map[string][]byte{"a.bin": big, "a.bin:meta": meta, "small.txt": small, "sub/b.txt": text}
	f.want["c2"] = map[string][]byte{"a.bin": big, "a.bin:meta": meta, "small.txt": small, "sub/b.txt": text2}
	f.want["c3"] = map[string][]byte{"c.bin": other, "holed.bin": append(append(head, make([]byte, 8192)...), tail...)}

	f.first.written = append(f.first.written, second.written...)

	return f
}

func (f *resealFixture) opener(ctx context.Context, keyID []byte) (*storekey.Key, error) {
	return f.keys[string(keyID)], nil
}

func (f *resealFixture) reopen(t *testing.T) {
	t.Helper()

	require.NoError(t, f.store.Close())
	f.store = newTestStore(t, f.base)
}

// read returns the content of every file and stream the commit holds, by
// path, failing on a PRF-named blob below it.
func (f *resealFixture) read(t *testing.T, ref *proto.Ref) map[string][]byte {
	t.Helper()
	ctx := context.Background()

	obj, err := f.store.Get(ctx, ref)
	require.NoError(t, err)

	key := f.keys[string(obj.GetCommit().KeyId)]
	reader := backup.NewBackupReader(f.store).WithKey(key)
	out := map[string][]byte{}

	content := func(ref *proto.Ref) []byte {
		r, err := reader.ReadFile(ctx, ref)
		require.NoError(t, err)

		data, err := io.ReadAll(r)
		require.NoError(t, err)

		return data
	}

	var walk func(tree *proto.Ref, parent []byte, dir string)
	walk = func(tree *proto.Ref, parent []byte, dir string) {
		opened, err := backup.OpenTree(ctx, f.store, tree, key, parent)
		require.NoError(t, err)

		for _, node := range opened.Nodes {
			path := dir + string(node.Stat.Name)

			if node.Stat.Type == proto.NodeType_NODE_DIRECTORY {
				walk(node.Ref, backup.NameToken(key, parent, node.Stat.Name), path+"/")
				continue
			}

			out[path] = content(node.Ref)

			for _, stream := range node.Stat.Streams {
				out[path+":"+string(stream.Name)] = content(stream.Ref)
			}
		}
	}

	walk(obj.GetCommit().Tree, nil, "")

	seen := map[string]bool{}

	var reach func(ref *proto.Ref)
	reach = func(ref *proto.Ref) {
		if seen[string(ref.Hash)] {
			return
		}
		seen[string(ref.Hash)] = true

		obj, err := f.store.Get(ctx, ref)
		require.NoError(t, err)

		if sealed := obj.Sealed(); sealed != nil {
			require.True(t, sealed.SealedRef().Equal(ref), "blob %x is still PRF-named", ref.Hash)
		}

		for _, below := range backup.References(obj) {
			reach(below)
		}
	}

	reach(ref)

	return out
}

func (f *resealFixture) livePins(t *testing.T) map[string]*proto.Pin {
	t.Helper()

	pins := map[string]*proto.Pin{}
	require.NoError(t, f.store.Walk(context.Background(), true, proto.ObjectType_PIN, func(obj *proto.Object) error {
		if has, err := f.store.Has(context.Background(), obj.Ref()); err != nil || has {
			pins[string(obj.GetPin().Target.Hash)] = obj.GetPin()
			return err
		}

		return nil
	}))

	return pins
}

func resealKeys(t *testing.T) (*storekey.Key, *storekey.Key) {
	t.Helper()

	a, err := storekey.Generate("world")
	require.NoError(t, err)

	b, err := storekey.Generate("other")
	require.NoError(t, err)

	return a, b
}

func TestResealPutsEveryCommitUnderBlobsItsSealedBytesName(t *testing.T) {
	a, b := resealKeys(t)
	f := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = f.store.Close() })
	ctx := context.Background()

	report, err := f.store.Reseal(ctx, f.opener)
	require.NoError(t, err)
	require.Empty(t, report.Failed)
	require.Zero(t, report.Current)
	require.Equal(t, 2, report.Pins)
	require.Len(t, report.Resealed, 3, "every sealed commit, and none written in the clear")

	distinct := map[string]bool{}
	for _, ref := range f.first.written {
		if obj, err := f.store.Get(ctx, ref); err == nil && obj.Sealed() != nil {
			distinct[string(ref.Hash)] = true
		}
	}
	require.Equal(t, len(distinct), report.Blobs)

	replaced := map[string]*proto.Ref{}
	for _, pair := range report.Resealed {
		replaced[string(pair.Old.Hash)] = pair.New
	}

	for name, old := range f.commits {
		next := replaced[string(old.Hash)]
		require.NotNil(t, next, name)

		has, err := f.store.HasAll(ctx, []*proto.Ref{old, next})
		require.NoError(t, err)
		require.Equal(t, []bool{false, true}, has, "%s is replaced", name)

		require.Equal(t, f.want[name], f.read(t, next), "%s restores as it was", name)
	}

	commit := func(ref *proto.Ref) *proto.Commit {
		obj, err := f.store.Get(ctx, ref)
		require.NoError(t, err)
		return obj.GetCommit()
	}

	c1, c2 := commit(replaced[string(f.commits["c1"].Hash)]), commit(replaced[string(f.commits["c2"].Hash)])
	require.True(t, c2.Parent.Equal(replaced[string(f.commits["c1"].Hash)]), "a parent is replaced with its child")
	require.Equal(t, int64(300*time.Second), c2.ReceivedAtNs)
	require.Equal(t, int64(300), c2.Timestamp)
	require.EqualValues(t, 1, c2.SetId)
	require.Equal(t, map[string]string{"env": "test"}, c2.Metadata)
	require.Equal(t, f.keys[string(c1.KeyId)].ID(), c1.KeyId)

	has, err := f.store.Has(ctx, f.plain)
	require.NoError(t, err)
	require.True(t, has, "a commit written in the clear is left as it is")

	pins := f.livePins(t)
	require.Len(t, pins, 2)
	require.Equal(t, map[string]string{"why": "release"}, pins[string(replaced[string(f.commits["c1"].Hash)].Hash)].GetMetadata(), "the commit's pin moves")

	var trashed []*proto.CommitScope
	require.NoError(t, f.store.Walk(ctx, true, proto.ObjectType_POLICY, func(obj *proto.Object) error {
		trashed = append(trashed, obj.GetPolicy().GetCommit())
		return nil
	}))
	require.Len(t, trashed, 2)
	require.Contains(t, []string{string(trashed[0].Commit.Hash), string(trashed[1].Commit.Hash)}, string(replaced[string(f.commits["c2"].Hash)].Hash), "the trash state moves")

	heads, err := f.store.Heads()
	require.NoError(t, err)

	for _, head := range heads {
		if head.SetID == 1 {
			require.Equal(t, replaced[string(f.commits["c2"].Hash)].Hash, head.Commit, "the head moves")
		}
	}

	again, err := f.store.Reseal(ctx, f.opener)
	require.NoError(t, err)
	require.Empty(t, again.Resealed, "a resealed store is left as it is")
	require.Zero(t, again.Blobs)
	require.Zero(t, again.Pins)
	require.Equal(t, 3, again.Current)
}

// replacements maps each commit a report replaced to its replacement.
func replacements(reports ...*ResealReport) map[string]string {
	out := map[string]string{}
	for _, report := range reports {
		for _, pair := range report.Resealed {
			out[string(pair.Old.Hash)] = string(pair.New.Hash)
		}
	}

	return out
}

func TestAResealStoppedPartWayCarriesOnWhereItStopped(t *testing.T) {
	a, b := resealKeys(t)
	ctx := context.Background()

	whole := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = whole.store.Close() })

	want, err := whole.store.Reseal(ctx, whole.opener)
	require.NoError(t, err)

	f := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = f.store.Close() })

	stopped, stop := context.WithCancel(ctx)
	first, err := f.store.Reseal(stopped, func(ctx context.Context, keyID []byte) (*storekey.Key, error) {
		if string(keyID) == string(b.ID()) {
			stop()
			return nil, ctx.Err()
		}

		return f.opener(ctx, keyID)
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, first.Resealed, 1, "the commit before the stop is replaced")

	f.reopen(t)

	rest, err := f.store.Reseal(ctx, f.opener)
	require.NoError(t, err)
	require.Empty(t, rest.Failed)
	require.Equal(t, 1, rest.Current, "the replacement is found current")
	require.Equal(t, replacements(want), replacements(first, rest), "the rerun replaces the rest as one run would")

	for name, old := range f.commits {
		next := &proto.Ref{Hash: []byte(replacements(want)[string(old.Hash)])}
		require.Equal(t, f.want[name], f.read(t, next), name)
	}
}

func TestAResealFinishesAReplacementStoppedBeforeTheOldCommitWasRetired(t *testing.T) {
	a, b := resealKeys(t)
	ctx := context.Background()

	whole := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = whole.store.Close() })

	want, err := whole.store.Reseal(ctx, whole.opener)
	require.NoError(t, err)

	f := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = f.store.Close() })

	// the first replacement and all it reaches are stored, and nothing else
	// happened yet
	c1 := &proto.Ref{Hash: []byte(replacements(want)[string(f.commits["c1"].Hash)])}
	copied := map[string]bool{}

	var copyOver func(ref *proto.Ref)
	copyOver = func(ref *proto.Ref) {
		if copied[string(ref.Hash)] {
			return
		}
		copied[string(ref.Hash)] = true

		obj, err := whole.store.Get(ctx, ref)
		require.NoError(t, err)
		require.NoError(t, f.store.Put(ctx, obj))

		for _, below := range backup.References(obj) {
			copyOver(below)
		}
	}

	copyOver(c1)
	require.NoError(t, f.store.Flush())

	report, err := f.store.Reseal(ctx, f.opener)
	require.NoError(t, err)
	require.Empty(t, report.Failed)
	require.Equal(t, replacements(want), replacements(report))
	require.Equal(t, 2, report.Pins, "the old commit's pin moves once")
	require.Len(t, f.livePins(t), 2)

	has, err := f.store.HasAll(ctx, []*proto.Ref{f.commits["c1"], c1})
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, has)
}

func TestAResealLeavesACommitWithAPlantedBlobAsItWas(t *testing.T) {
	key, other := resealKeys(t)
	store := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	l := newLegacy(t, store, key)

	// a holder of the key sealed other content under the ref of this one
	planted := proto.HashPayload(proto.ObjectType_BLOB, key.Digest([]byte("the real content")))
	stored, compression := proto.Encode([]byte("junk"))
	sealed, err := l.aead.EncryptDeterministically(stored, append([]byte("blob"), planted.Hash...))
	require.NoError(t, err)
	_, err = store.writeRecord(ctx, &proto.ObjectHeader{Ref: planted, Type: proto.ObjectType_BLOB, Compression: compression,
		Encryption: proto.Encryption_SEALED, KeyId: key.ID(), StoredHash: proto.StoredHash(sealed)}, sealed)
	require.NoError(t, err)

	bad := l.commit("world", 1, 100, l.tree(nil, fileNode("real", 16, l.put(&proto.File{Parts: []*proto.FilePart{{Length: 16, Ref: planted}}}))), nil)
	good := l.commit("world", 1, 200, l.tree(nil, fileNode("fine", 4, l.file([]byte("fine"), 4))), bad)
	require.NoError(t, store.Flush())

	report, err := store.Reseal(ctx, func(context.Context, []byte) (*storekey.Key, error) { return key, nil })
	require.NoError(t, err)
	require.Len(t, report.Failed, 1)
	require.True(t, report.Failed[0].Ref.Equal(bad))
	require.ErrorIs(t, report.Failed[0].Err, storekey.ErrWrongKey)
	require.Len(t, report.Resealed, 1, "the other commit is replaced, over the one left as its parent")

	has, err := store.HasAll(ctx, []*proto.Ref{bad, good})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, has)

	_, err = store.Reseal(ctx, func(context.Context, []byte) (*storekey.Key, error) { return other, nil })
	require.NoError(t, err, "a key that is not the one asked for fails the commits, not the run")
}

func TestACollectionAfterAResealDropsWhatOnlyTheOldCommitsReached(t *testing.T) {
	a, b := resealKeys(t)
	f := newResealFixture(t, a, b)
	t.Cleanup(func() { _ = f.store.Close() })
	ctx := context.Background()

	report, err := f.store.Reseal(ctx, f.opener)
	require.NoError(t, err)

	for _, ref := range f.first.written {
		_, err := f.store.Get(ctx, ref)
		require.NoError(t, err, "%x is still stored", ref.Hash)
	}

	for _, ahead := range []time.Duration{0, 48 * time.Hour} {
		_, err := f.store.Collect(ctx, gcOptions(t, ahead))
		require.NoError(t, err)
	}

	for _, ref := range f.first.written {
		_, err := f.store.Get(ctx, ref)
		require.ErrorIs(t, err, backup.ErrNotFound, "%x is gone", ref.Hash)
	}

	for name, old := range f.commits {
		next := &proto.Ref{Hash: []byte(replacements(report)[string(old.Hash)])}
		require.Equal(t, f.want[name], f.read(t, next), name)
	}
}

func TestAResealTakesAReplacedParentOnlyFromItsOwnReplacement(t *testing.T) {
	key, _ := resealKeys(t)
	ctx := context.Background()
	opener := func(context.Context, []byte) (*storekey.Key, error) { return key, nil }

	// two commits of a set received at once, and a child of the first
	build := func(store *PackStorage) (p, q, c *proto.Ref) {
		l := newLegacy(t, store, key)
		p = l.commit("world", 1, 100, l.tree(nil, fileNode("p", 1, l.file([]byte("p"), 1))), nil)
		q = l.put(&proto.Commit{Timestamp: 100, Tree: l.tree(nil, fileNode("q", 1, l.file([]byte("q"), 1))), BackupSet: "world",
			AgentId: "agent", SetId: 1, ReceivedAtNs: 100 * int64(time.Second), KeyId: key.ID()})
		c = l.commit("world", 1, 200, l.tree(nil, fileNode("c", 1, l.file([]byte("c"), 1))), p)
		require.NoError(t, store.Flush())

		return p, q, c
	}

	whole := newTestStore(t, t.TempDir())
	t.Cleanup(func() { _ = whole.Close() })
	p, _, c := build(whole)

	want, err := whole.Reseal(ctx, opener)
	require.NoError(t, err)

	pNew := &proto.Ref{Hash: []byte(replacements(want)[string(p.Hash)])}

	// map order decides which commit received with the parent is met first
	for range 8 {
		store := newTestStore(t, t.TempDir())
		build(store)

		// an earlier run replaced the parent and stopped once it retired it
		var copyOver func(ref *proto.Ref)
		copyOver = func(ref *proto.Ref) {
			obj, err := whole.Get(ctx, ref)
			require.NoError(t, err)
			require.NoError(t, store.Put(ctx, obj))

			for _, below := range backup.References(obj) {
				copyOver(below)
			}
		}
		copyOver(pNew)
		require.NoError(t, store.Flush())
		require.NoError(t, store.Delete(ctx, p))
		require.NoError(t, store.Flush())

		report, err := store.Reseal(ctx, opener)
		require.NoError(t, err)
		require.Equal(t, replacements(want)[string(c.Hash)], replacements(report)[string(c.Hash)], "the child's parent is the parent's replacement")
		require.NoError(t, store.Close())
	}
}
