package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, path string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
}

func entriesOf(paths ...string) []treeEntry {
	entries := make([]treeEntry, len(paths))
	for i, path := range paths {
		entries[i] = treeEntry{path: path}
	}

	return entries
}

func TestRestoreTreeWritesTheTreeAndDeletesWhatItDoesNotHold(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("one"))
	f.write("sub/b.txt", []byte("two"))

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	dst := t.TempDir()
	writeFile(t, filepath.Join(dst, "stale", "old.log"))
	writeFile(t, filepath.Join(dst, "gone.txt"))

	var removed []string

	restorer := &Restorer{Store: f.store}
	err = restorer.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{
		Delete:   true,
		Removing: func(path string) { removed = append(removed, path) },
	})
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	require.NoError(t, err)
	require.Equal(t, "two", string(content))
	require.ElementsMatch(t, []string{filepath.Join(dst, "stale"), filepath.Join(dst, "gone.txt")}, removed)
	require.NoDirExists(t, filepath.Join(dst, "stale"))
	require.NoFileExists(t, filepath.Join(dst, "gone.txt"))
}

func TestRestoreTreeRefusesAPathOutsideTheDirectory(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()

	file := proto.NewObject(&proto.File{Inline: []byte("x")})
	require.NoError(t, store.Put(ctx, file))

	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{
		Stat: &proto.FileInfo{Name: []byte("../evil"), Type: proto.NodeType_NODE_FILE, Size: 1},
		Ref:  file.Ref(),
	}}})
	require.NoError(t, store.Put(ctx, tree))

	dir := filepath.Join(t.TempDir(), "dst")

	err := (&Restorer{Store: store}).RestoreTree(ctx, &proto.Commit{BackupSet: "s", Tree: tree.Ref()}, dir, TreeOptions{})
	require.ErrorContains(t, err, "outside the directory")
	require.NoFileExists(t, filepath.Join(filepath.Dir(dir), "evil"))
}

func TestRemoveUnrestoredKeepsWhatTheRestoreWroteUnderAnotherSpelling(t *testing.T) {
	base := t.TempDir()
	writeFile(t, filepath.Join(base, "world", "level.dat"))
	writeFile(t, filepath.Join(base, "stale", "old.log"))

	// the commit spells the directory and file differently; on a
	// case-insensitive filesystem the restore wrote into the existing ones
	restored := entriesOf(filepath.Join(base, "World"), filepath.Join(base, "World", "Level.dat"))

	_, err := os.Lstat(filepath.Join(base, "WORLD"))
	insensitive := err == nil

	require.NoError(t, removeUnrestored(base, restored, false, nil))
	require.NoDirExists(t, filepath.Join(base, "stale"))

	if insensitive {
		require.FileExists(t, filepath.Join(base, "world", "level.dat"))
	} else {
		require.NoDirExists(t, filepath.Join(base, "world"))
	}
}

func TestRemoveUnrestoredKeepsThePartialsOfRestoredFiles(t *testing.T) {
	base := t.TempDir()
	level, gone := filepath.Join(base, "level.dat"), filepath.Join(base, "gone.dat")
	writeFile(t, PartialPath(level))
	writeFile(t, PartialPath(gone))

	require.NoError(t, removeUnrestored(base, entriesOf(level), false, nil))
	require.FileExists(t, PartialPath(level))
	require.NoFileExists(t, PartialPath(gone))
}

func TestRestoreDirReplacesWhatIsNotADirectory(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(base, "outside")
	require.NoError(t, os.Mkdir(outside, 0o755))

	link := filepath.Join(base, "world")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	require.NoError(t, restoreDir(link, 0o755))

	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "the link is replaced by a directory")
	require.Zero(t, info.Mode()&os.ModeSymlink)

	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing was written through the link")

	file := filepath.Join(base, "plain")
	writeFile(t, file)
	require.NoError(t, restoreDir(file, 0o755))
	info, err = os.Lstat(file)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}
