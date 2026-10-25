package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// frontToBack is the local disk written only front to back and without
// links, as an FTP server is.
type frontToBack struct{ WriteFS }

func (frontToBack) Symlink(context.Context, string, string) error {
	return fmt.Errorf("no links here: %w", errors.ErrUnsupported)
}

func TestALinkTheFilesystemOrUserMayNotMakeIsUnsupported(t *testing.T) {
	require.ErrorIs(t, unsupportedLink(&os.LinkError{Op: "symlink", Err: linksRefused}), errors.ErrUnsupported)
	require.NotErrorIs(t, unsupportedLink(&os.LinkError{Op: "symlink", Err: os.ErrExist}), errors.ErrUnsupported)
}

// filesStore reads files in groups, as a store server does.
type filesStore struct{ *memStore }

func (s filesStore) ReadFiles(ctx context.Context, files []FileRead, objectsOnly bool, object func(int, *proto.Object) error, part func(int, int, *proto.Object) error) error {
	var objects []*proto.Object

	for j, read := range files {
		obj, err := s.Get(ctx, read.Ref)
		if err != nil {
			return err
		}

		if err := object(j, obj); err != nil {
			return err
		}

		objects = append(objects, obj)
	}

	if objectsOnly {
		return nil
	}

	sent := map[string]bool{}

	for j, obj := range objects {
		for i, p := range obj.GetFile().GetParts() {
			if p.Ref == nil || slices.Contains(files[j].Skip, i) || sent[string(p.Ref.Hash)] {
				continue
			}

			sent[string(p.Ref.Hash)] = true

			blob, err := s.Get(ctx, p.Ref)
			if err != nil {
				return err
			}

			if err := part(j, i, blob); err != nil {
				return err
			}
		}
	}

	return nil
}

// distinctParts stores a file of count small parts that all differ.
func distinctParts(t *testing.T, store ObjectStore, count int) ([]byte, *proto.Ref) {
	t.Helper()

	ctx := context.Background()

	var (
		data  []byte
		parts []*proto.FilePart
	)

	for i := range count {
		chunk := randomData(64, int64(i+1000))

		blob := proto.NewObject(&proto.Blob{Data: chunk})
		require.NoError(t, store.Put(ctx, blob))

		parts = append(parts, &proto.FilePart{Offset: uint64(len(data)), Length: uint64(len(chunk)), Ref: blob.Ref()})
		data = append(data, chunk...)
	}

	file := proto.NewObject(&proto.File{Parts: parts})
	require.NoError(t, store.Put(ctx, file))

	return data, file.Ref()
}

// gatedStore holds back the gate's part until want other objects were
// asked for.
type gatedStore struct {
	*memStore
	gate   *proto.Ref
	want   int64
	others atomic.Int64
}

func (g *gatedStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if !ref.Equal(g.gate) {
		g.others.Add(1)
		return g.memStore.Get(ctx, ref)
	}

	deadline := time.Now().Add(10 * time.Second)
	for g.others.Load() < g.want {
		if time.Now().After(deadline) {
			return nil, errors.New("nothing was fetched ahead of a slow part")
		}

		time.Sleep(time.Millisecond)
	}

	return g.memStore.Get(ctx, ref)
}

func TestAFileIsStreamedOntoATreeWrittenFrontToBack(t *testing.T) {
	ctx := context.Background()
	store := &gatedStore{memStore: newMemStore()}

	data, ref := distinctParts(t, store.memStore, 3*readWindow)
	obj, err := store.memStore.Get(ctx, ref)
	require.NoError(t, err)
	// the file, the first part and every part after the slow second one
	// that fits in a window sliding past the first
	store.gate, store.want = obj.GetFile().GetParts()[1].Ref, readWindow+1

	stat := statFor(data)
	path := filepath.Join(t.TempDir(), "world.dat")
	require.NoError(t, os.WriteFile(PartialPath(path), []byte("left by an earlier restore"), 0o600))

	restorer := &Restorer{Store: store, FS: frontToBack{LocalFS{}}}

	outcome, err := restorer.RestoreFile(ctx, path, stat, ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeWritten, outcome)
	requireRestored(t, path, data, stat)
	require.NoFileExists(t, PartialPath(path))
	require.EqualValues(t, len(data), restorer.Stats().BytesFromStore)

	_, err = (&Restorer{Store: store, FS: frontToBack{LocalFS{}}, Verify: true}).RestoreFile(ctx, path, stat, ref)
	require.ErrorContains(t, err, "read back")
}

func TestASalvagedStreamLeavesZeroesWhereAPartIsGone(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	data, ref := distinctParts(t, store, 10)
	parts := partsOf(t, store, ref)
	require.NoError(t, store.Delete(ctx, parts[4].Ref))

	path := filepath.Join(t.TempDir(), "world.dat")

	var holes []Hole
	restorer := &Restorer{Store: store, FS: frontToBack{LocalFS{}}, Salvage: true, OnHole: func(h Hole) { holes = append(holes, h) }}

	outcome, err := restorer.RestoreFile(ctx, path, statFor(data), ref)
	require.NoError(t, err)
	require.Equal(t, OutcomeSalvaged, outcome)
	require.Len(t, holes, 1)
	require.EqualValues(t, parts[4].Offset, holes[0].Offset)
	requireRestored(t, path, zeroed(data, parts[4]), statFor(data))
}

func TestRestoreTreeOntoATreeWrittenFrontToBack(t *testing.T) {
	f := newWalkerFixture(t)
	big := f.random(3*maxBlobSize + 17)
	f.write("world/region.mca", big)
	f.write("world/level.dat", []byte("level"))
	f.write("server.properties", []byte("motd=hi"))

	linked := os.Symlink("server.properties", filepath.Join(f.root, "link")) == nil

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	dst := t.TempDir()
	restorer := &Restorer{Store: filesStore{f.store}, FS: frontToBack{LocalFS{}}}

	require.NoError(t, restorer.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	for name, want := range map[string][]byte{"world/region.mca": big, "world/level.dat": []byte("level"), "server.properties": []byte("motd=hi")} {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(name)))
		require.NoError(t, err)
		require.Equal(t, want, got, name)
	}

	if linked {
		require.EqualValues(t, 1, restorer.Stats().Unsupported)
		require.NoFileExists(t, filepath.Join(dst, "link"))
	}
}

func TestPathsAreRestoredBesideTheirOriginalsUnderTheirNames(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))
	f.write("sub/b.txt", []byte("b"))
	f.write("sub/deep/c.txt", []byte("c"))
	f.write("other/d.txt", []byte("d"))

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	restored := func(p string, _ os.FileInfo) string { return path.Base(p) + ".restored" }

	dst := t.TempDir()
	require.NoError(t, (&Restorer{Store: f.store}).RestoreTree(context.Background(), result.Commit, dst, TreeOptions{
		Paths: []string{"sub/deep", "a.txt", "sub/deep/c.txt"},
		Name:  restored,
	}))

	require.FileExists(t, filepath.Join(dst, "a.txt.restored"))
	require.FileExists(t, filepath.Join(dst, "sub", "deep.restored", "c.txt"))
	require.NoFileExists(t, filepath.Join(dst, "sub", "b.txt"))
	require.NoDirExists(t, filepath.Join(dst, "other"))

	all := t.TempDir()
	require.NoError(t, (&Restorer{Store: f.store}).RestoreTree(context.Background(), result.Commit, all, TreeOptions{Name: restored}))

	require.FileExists(t, filepath.Join(all, "a.txt.restored"))
	require.FileExists(t, filepath.Join(all, "sub.restored", "deep", "c.txt"))
	require.FileExists(t, filepath.Join(all, "other.restored", "d.txt"))

	err = (&Restorer{Store: f.store}).RestoreTree(context.Background(), result.Commit, dst, TreeOptions{Paths: []string{"sub/missing"}})
	require.ErrorContains(t, err, "not in the commit")
}

func TestARestoreIntoAReadOnlyDirectoryGivesItBackItsMode(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	dst := t.TempDir()
	require.NoError(t, os.Chmod(dst, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dst, 0o755) })

	before, err := os.Lstat(dst)
	require.NoError(t, err)

	require.NoError(t, (&Restorer{Store: f.store}).RestoreTree(context.Background(), result.Commit, dst, TreeOptions{Paths: []string{"a.txt"}}))
	require.FileExists(t, filepath.Join(dst, "a.txt"))

	after, err := os.Lstat(dst)
	require.NoError(t, err)
	require.Equal(t, before.Mode(), after.Mode())
}
