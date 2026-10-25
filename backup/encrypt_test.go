package backup

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func newKey(t *testing.T) *storekey.Key {
	t.Helper()

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	return key
}

// readAll restores every file under the tree through the reader.
func readAll(t *testing.T, reader *BackupReader, tree *proto.Ref) map[string][]byte {
	t.Helper()

	files := map[string][]byte{}
	err := reader.WalkTree(context.Background(), tree, nil, func(path string, info os.FileInfo, ref *proto.Ref) error {
		if info.IsDir() {
			return nil
		}

		r, err := reader.ReadFile(context.Background(), ref)
		if err != nil {
			return err
		}

		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		files[filepath.ToSlash(path)] = data
		return nil
	})
	require.NoError(t, err)

	return files
}

// fileObject finds a file's object under an encrypted tree by its plain path.
func fileObject(t *testing.T, store ObjectStore, key *storekey.Key, tree *proto.Ref, path string) *proto.File {
	t.Helper()

	reader := NewBackupReader(store).WithKey(key)
	dir, name := filepath.Split(path)

	var parts []string
	if dir != "" {
		parts = []string{filepath.Clean(dir)}
	}

	ref, parent, err := reader.GetTree(context.Background(), tree, parts)
	require.NoError(t, err)

	nodes, err := OpenTree(context.Background(), store, ref, key, parent)
	require.NoError(t, err)

	for _, node := range nodes.Nodes {
		if string(node.Stat.Name) == name {
			obj, err := store.Get(context.Background(), node.Ref)
			require.NoError(t, err)
			return obj.GetFile()
		}
	}

	t.Fatalf("%s not found", path)
	return nil
}

func TestWalkerWithKeySealsNamesAndContents(t *testing.T) {
	f := newWalkerFixture(t)
	key := newKey(t)
	f.walker.Key = key

	contents := map[string][]byte{
		"a.txt":     []byte("hello"),
		"sub/b.bin": f.random(150 << 10),
		"sub/c.dat": bytes.Repeat([]byte("game save data "), 20000),
	}
	for path, data := range contents {
		f.write(path, data)
	}

	first := f.run()
	require.EqualValues(t, 1, first.Commit.PolicyVersion)

	stored, err := LoadTree(context.Background(), f.store, first.Commit.Tree)
	require.NoError(t, err)
	for _, node := range stored.Nodes {
		require.NotContains(t, []string{"a.txt", "sub"}, string(node.Stat.Name), "stored names are tokens")
	}

	f.store.mtx.RLock()
	for _, obj := range f.store.objects {
		switch obj.Type() {
		case proto.ObjectType_BLOB:
			require.NotNil(t, obj.Sealed(), "every blob is sealed")
		}
	}
	f.store.mtx.RUnlock()

	require.Equal(t, contents, readAll(t, NewBackupReader(f.store).WithKey(key), first.Commit.Tree))

	// without the key the tree still walks, but names are tokens and
	// contents do not open
	plain := NewBackupReader(f.store)
	err = plain.WalkTree(context.Background(), first.Commit.Tree, nil, func(path string, info os.FileInfo, ref *proto.Ref) error {
		require.NotContains(t, []string{"a.txt", "sub"}, filepath.Base(path))
		if info.IsDir() {
			return nil
		}

		r, err := plain.ReadFile(context.Background(), ref)
		require.NoError(t, err)
		_, err = io.ReadAll(r)
		require.ErrorIs(t, err, storekey.ErrNoKey)
		return nil
	})
	require.NoError(t, err)

	err = NewBackupReader(f.store).WithKey(newKey(t)).WalkTree(context.Background(), first.Commit.Tree, nil, func(string, os.FileInfo, *proto.Ref) error { return nil })
	require.ErrorIs(t, err, storekey.ErrWrongKey)

	small := fileObject(t, f.store, key, first.Commit.Tree, "a.txt")
	require.Equal(t, proto.Encryption_SEALED, small.InlineEncryption)
	require.NotContains(t, string(small.Inline), "hello")

	// an unchanged run is detected through the opened base tree
	second := f.run()
	require.EqualValues(t, 3, second.Reused)
	require.EqualValues(t, 0, second.Read)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))

	f.write("sub/c.dat", []byte("changed"))
	contents["sub/c.dat"] = []byte("changed")

	third := f.run()
	require.EqualValues(t, 1, third.Read)
	require.Equal(t, contents, readAll(t, NewBackupReader(f.store).WithKey(key), third.Commit.Tree))

	ref, err := HashFile(filepath.Join(f.root, "sub", "b.bin"), key)
	require.NoError(t, err)
	require.True(t, ref.Equal(proto.NewObject(fileObject(t, f.store, key, third.Commit.Tree, "sub/b.bin")).Ref()))
}

func TestSealedBlobsDedupeUnderOneKeyOnly(t *testing.T) {
	data := make([]byte, 400<<10)
	rand.New(rand.NewSource(7)).Read(data)

	key := newKey(t)
	var files []*proto.File

	for _, k := range []*storekey.Key{key, key, newKey(t)} {
		f := newWalkerFixture(t)
		f.walker.Key = k
		f.write("shared.dat", data)

		result := f.run()
		files = append(files, fileObject(t, f.store, k, result.Commit.Tree, "shared.dat"))
	}

	require.True(t, files[0].Parts[0].Ref.Equal(files[1].Parts[0].Ref), "the same key stores a chunk under one ref")
	require.False(t, files[0].Parts[0].Ref.Equal(files[2].Parts[0].Ref), "another key does not")
}

func TestIndexPath(t *testing.T) {
	key := newKey(t)

	require.Equal(t, "a/b.txt", IndexPath(nil, "/a/b.txt/"))
	require.Equal(t, "", IndexPath(key, ""))

	sub := NameToken(key, nil, []byte("sub"))
	want := proto.JoinPath(proto.JoinPath("", sub), NameToken(key, sub, []byte("b.bin")))
	require.Equal(t, want, IndexPath(key, "sub/b.bin"))
	require.Equal(t, []byte("sub"), func() []byte {
		name, err := key.OpenField(nil, storekey.FieldName, proto.NameFromComponent(proto.PathComponent(sub)))
		require.NoError(t, err)
		return name
	}())
}

func TestFilesPutOutsideAWalkReadBack(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	key := newKey(t)

	contents := map[string][]byte{
		"small": []byte("hello\n"),
		"large": bytes.Repeat([]byte("0123456789abcdef"), 64<<10),
		// sealing grows it past what may be inline
		"at the inline limit": bytes.Repeat([]byte("x"), proto.InlineLimit),
	}

	var nodes []*proto.TreeNode
	for name, data := range contents {
		ref, err := PutFile(ctx, store, key, int64(len(data)), bytes.NewReader(data))
		require.NoError(t, err)

		nodes = append(nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Size: int64(len(data))}, Ref: ref})
	}

	tree, err := PutTree(ctx, store, SortNodes(nodes), key, nil)
	require.NoError(t, err)

	require.Equal(t, contents, readAll(t, NewBackupReader(store).WithKey(key), tree))
}

func TestAFileStoredOverItsPartsIsTheFileItsWriterStored(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 600<<10)
	rand.New(rand.NewSource(9)).Read(data)

	for _, key := range []*storekey.Key{nil, newKey(t)} {
		store := newMemStore()

		ref, err := PutFile(ctx, store, key, int64(len(data)), bytes.NewReader(data))
		require.NoError(t, err)

		obj, err := store.Get(ctx, ref)
		require.NoError(t, err)

		parts, err := FileParts(ctx, store, obj.GetFile())
		require.NoError(t, err)

		again, err := PutParts(ctx, store, key, parts)
		require.NoError(t, err)
		require.True(t, ref.Equal(again))
	}
}

func TestWalkerWithKeyReadsEverythingOverACommitWrittenWithoutIt(t *testing.T) {
	f := newWalkerFixture(t)
	contents := map[string][]byte{"a.txt": []byte("hello"), "sub/b.txt": []byte("world")}
	for path, data := range contents {
		f.write(path, data)
	}

	f.run()

	for _, key := range []*storekey.Key{newKey(t), newKey(t)} {
		f.walker.Key = key
		keyed := f.run()
		require.Nil(t, keyed.Commit.Parent)
		require.Equal(t, contents, readAll(t, NewBackupReader(f.store).WithKey(key), keyed.Commit.Tree))
	}
}

func TestACommitRecordsTheKeyItWasSealedUnder(t *testing.T) {
	f := newWalkerFixture(t)
	key := newKey(t)
	contents := map[string][]byte{"a.txt": []byte("hello"), "sub/b.txt": []byte("world")}
	for path, data := range contents {
		f.write(path, data)
	}

	plain := f.run().Commit
	require.NotNil(t, plain.KeyId)
	require.Empty(t, plain.KeyId)

	f.walker.Key = key
	sealed := f.run().Commit
	require.Equal(t, key.ID(), sealed.KeyId)

	f.walker.Key = nil
	again := f.run().Commit
	require.Nil(t, again.Parent, "a plain run does not diff against a sealed one")

	reader := NewBackupReader(f.store).WithKey(key)
	for _, commit := range []*proto.Commit{plain, sealed} {
		opened, err := reader.ForCommit(context.Background(), commit)
		require.NoError(t, err)
		require.Equal(t, contents, readAll(t, opened, commit.Tree))
	}

	_, err := NewBackupReader(f.store).WithKey(newKey(t)).ForCommit(context.Background(), sealed)
	require.ErrorIs(t, err, storekey.ErrWrongKey)
}

func TestACommitThatPredatesItsKeyIDIsOpenedAsItWasWritten(t *testing.T) {
	f := newWalkerFixture(t)
	key := newKey(t)
	f.write("a.txt", []byte("hello"))

	plain := f.run().Commit
	f.walker.Key = key
	sealed := f.run().Commit

	plain.KeyId, sealed.KeyId = nil, nil

	got, err := CommitKey(context.Background(), f.store, plain, key)
	require.NoError(t, err)
	require.Nil(t, got)

	got, err = CommitKey(context.Background(), f.store, sealed, key)
	require.NoError(t, err)
	require.Equal(t, key, got)
}

func TestARunThatCarriesEntriesRefusesACommitSealedDifferently(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("000000010000000000000001", []byte("wal"))
	f.walker.Carry = func(string) bool { return true }
	f.run()

	f.walker.Key = newKey(t)
	_, err := f.walker.Run(context.Background())
	require.ErrorIs(t, err, ErrSealChanged)
}
