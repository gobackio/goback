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

// treeBuilder puts the nodes of hand-made trees in a store.
type treeBuilder struct {
	t     *testing.T
	store *memStore
}

func newTreeBuilder(t *testing.T) treeBuilder {
	return treeBuilder{t: t, store: newMemStore()}
}

func (b treeBuilder) file(name, content string, mode os.FileMode) *proto.TreeNode {
	file := proto.NewObject(&proto.File{Inline: []byte(content)})
	require.NoError(b.t, b.store.Put(context.Background(), file))

	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_FILE, Size: int64(len(content)), Mode: uint32(mode)}, Ref: file.Ref()}
}

func (b treeBuilder) link(name, target string) *proto.TreeNode {
	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_SYMLINK, LinkTarget: []byte(target), Mode: uint32(os.ModeSymlink | 0o777)}}
}

func (b treeBuilder) dir(name string, mode os.FileMode, nodes ...*proto.TreeNode) *proto.TreeNode {
	tree := proto.NewObject(&proto.Tree{Nodes: nodes})
	require.NoError(b.t, b.store.Put(context.Background(), tree))

	return &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(name), Type: proto.NodeType_NODE_DIRECTORY, Mode: uint32(os.ModeDir | mode)}, Ref: tree.Ref()}
}

func (b treeBuilder) commit(nodes ...*proto.TreeNode) *proto.Commit {
	tree := proto.NewObject(&proto.Tree{Nodes: nodes})
	require.NoError(b.t, b.store.Put(context.Background(), tree))

	return &proto.Commit{BackupSet: "s", Tree: tree.Ref()}
}

func TestRestoreTreeRefusesAnEntryNamedOutsideItsDirectory(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	outside := t.TempDir()

	pwned := func(name string) *proto.TreeNode { return b.file(name, "pwned", 0o644) }

	cases := map[string][]*proto.TreeNode{
		"parent":            {pwned("../pwned")},
		"through a link":    {b.link("x", outside), pwned("x/pwned")},
		"dot link":          {b.link(".", outside), pwned("pwned")},
		"dot link below":    {b.dir("a", 0o755, b.link(".", outside), pwned("pwned"))},
		"dot dot file":      {b.dir("a", 0o755, pwned(".."))},
		"dot file":          {pwned(".")},
		"nul":               {pwned("a\x00b")},
		"slashed directory": {b.dir("a/..", 0o755, pwned("pwned"))},
	}

	for name, nodes := range cases {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "dst")

			err := (&Restorer{Store: b.store}).RestoreTree(ctx, b.commit(nodes...), dir, TreeOptions{})
			require.Error(t, err)

			for _, d := range []string{outside, base} {
				entries, err := os.ReadDir(d)
				require.NoError(t, err)

				for _, e := range entries {
					require.Equal(t, "dst", e.Name(), "nothing is written beside the directory")
				}
			}

			info, err := os.Lstat(dir)
			if err == nil {
				require.True(t, info.IsDir())
			}
		})
	}
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

	require.NoError(t, (&Restorer{}).removeUnrestored(context.Background(), base, restored, nil))
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

	require.NoError(t, (&Restorer{}).removeUnrestored(context.Background(), base, entriesOf(level), nil))
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

	require.NoError(t, (&Restorer{}).restoreDir(context.Background(), link, 0o755))

	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "the link is replaced by a directory")
	require.Zero(t, info.Mode()&os.ModeSymlink)

	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing was written through the link")

	file := filepath.Join(base, "plain")
	writeFile(t, file)
	require.NoError(t, (&Restorer{}).restoreDir(context.Background(), file, 0o755))
	info, err = os.Lstat(file)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}
