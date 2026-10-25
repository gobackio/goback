package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// hardLinked is a tree of one file at data/a, recorded again as a hard
// link at data/b and at c.
func hardLinked(b treeBuilder) *proto.Commit {
	a := b.file("a", "shared content", 0o644)

	linkTo := func(name string) *proto.TreeNode {
		node := b.file(name, "shared content", 0o644)
		node.Stat.HardLink = []byte("data/a")

		return node
	}

	return b.commit(linkTo("c"), b.dir("data", 0o755, a, linkTo("b")))
}

func requireSameFile(t *testing.T, a, b string) {
	t.Helper()

	infoA, err := os.Lstat(a)
	require.NoError(t, err)

	infoB, err := os.Lstat(b)
	require.NoError(t, err)

	require.True(t, os.SameFile(infoA, infoB), "%s and %s are one file", a, b)
}

func TestRestoreTreeMakesTheFilesRecordedAsHardLinksLinks(t *testing.T) {
	b := newTreeBuilder(t)
	commit := hardLinked(b)
	dst := t.TempDir()

	var (
		mtx      sync.Mutex
		outcomes = map[string]Outcome{}
		listed   int64
	)

	r := &Restorer{Store: b.store}
	require.NoError(t, r.RestoreTree(context.Background(), commit, dst, TreeOptions{
		Listed: func(_, bytes int64) { listed = bytes },
		Done: func(file RestoredFile) error {
			mtx.Lock()
			defer mtx.Unlock()

			outcomes[file.Path] = file.Outcome

			return file.Err
		},
	}))

	requireSameFile(t, filepath.Join(dst, "data", "a"), filepath.Join(dst, "data", "b"))
	requireSameFile(t, filepath.Join(dst, "data", "a"), filepath.Join(dst, "c"))
	require.Equal(t, OutcomeLinked, outcomes[filepath.Join(dst, "c")])
	require.Equal(t, OutcomeWritten, outcomes[filepath.Join(dst, "data", "a")])
	require.EqualValues(t, 2, r.Stats().Linked)
	require.EqualValues(t, len("shared content"), listed, "the bytes are written once")

	again := &Restorer{Store: b.store}
	require.NoError(t, again.RestoreTree(context.Background(), commit, dst, TreeOptions{}))
	require.Zero(t, again.Stats().Linked)
	require.EqualValues(t, 3, again.Stats().Unchanged)
}

func TestAHardLinkWhoseFileIsNotRestoredIsWritten(t *testing.T) {
	b := newTreeBuilder(t)
	commit := hardLinked(b)
	dst := t.TempDir()

	r := &Restorer{Store: b.store}
	require.NoError(t, r.RestoreTree(context.Background(), commit, dst, TreeOptions{Paths: []string{"data/b", "c"}}))

	content, err := os.ReadFile(filepath.Join(dst, "data", "b"))
	require.NoError(t, err)
	require.Equal(t, "shared content", string(content))
	require.Zero(t, r.Stats().Linked)
	require.NoFileExists(t, filepath.Join(dst, "data", "a"))
}

// noLinksFS is the local disk where no hard link can be made, as on FAT.
type noLinksFS struct {
	LocalFS
}

func (noLinksFS) Link(context.Context, string, string) error {
	return errors.ErrUnsupported
}

func TestAHardLinkThatCannotBeMadeIsWritten(t *testing.T) {
	b := newTreeBuilder(t)
	dst := t.TempDir()

	r := &Restorer{Store: b.store, FS: noLinksFS{}}
	require.NoError(t, r.RestoreTree(context.Background(), hardLinked(b), dst, TreeOptions{}))

	for _, path := range []string{filepath.Join(dst, "data", "b"), filepath.Join(dst, "c")} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "shared content", string(content))
	}

	require.Zero(t, r.Stats().Linked)
	require.EqualValues(t, 3, r.Stats().Written)
}
