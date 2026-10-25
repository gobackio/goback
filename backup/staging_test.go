package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// writePartial leaves data in path's partial file, as a failed restore
// does.
func writePartial(t *testing.T, path string, data []byte) {
	t.Helper()

	staged := filepath.Join(filepath.Dir(path), StagingDir)
	require.NoError(t, os.MkdirAll(staged, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(staged, StagingMarker), nil, 0o600))
	require.NoError(t, os.WriteFile(PartialPath(path), data, 0o600))
}

func TestAnEntryNamedLikeAnOldPartialIsRestoredAsItself(t *testing.T) {
	b := newTreeBuilder(t)
	commit := b.commit(SortNodes([]*proto.TreeNode{
		b.file("x", "the file", 0o644),
		b.file(".x.goback.partial", "a file of its own", 0o644),
		b.dir("d", 0o755, b.file("y", "inside", 0o644)),
	})...)

	dst := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dst, ".x.goback.partial"), []byte("a file of its own"), 0o644))

	require.NoError(t, (&Restorer{Store: b.store, Workers: 1}).RestoreTree(context.Background(), commit, dst, TreeOptions{}))
	require.Equal(t, map[string][]byte{
		"x":                 []byte("the file"),
		".x.goback.partial": []byte("a file of its own"),
		"d/y":               []byte("inside"),
	}, readTree(t, dst), "no staging directory is left")
}

func TestAStagingDirectoryNoRestoreMadeIsLeftAlone(t *testing.T) {
	b := newTreeBuilder(t)
	commit := b.commit(b.file("x", "the file", 0o644))

	dst := t.TempDir()
	theirs := filepath.Join(dst, StagingDir, "x")
	writeFile(t, theirs)

	err := (&Restorer{Store: b.store}).RestoreTree(context.Background(), commit, dst, TreeOptions{})
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(dst, "x"))

	data, err := os.ReadFile(theirs)
	require.NoError(t, err)
	require.Equal(t, "x", string(data))
}

func TestAStagingDirectoryAFailedRestoreLeftIsResumedAndRemoved(t *testing.T) {
	b := newTreeBuilder(t)
	commit := b.commit(b.file("x", "the file", 0o644))

	dst := t.TempDir()
	writePartial(t, filepath.Join(dst, "x"), []byte("the f"))

	require.NoError(t, (&Restorer{Store: b.store}).RestoreTree(context.Background(), commit, dst, TreeOptions{}))
	require.Equal(t, map[string][]byte{"x": []byte("the file")}, readTree(t, dst))
}

func TestATreeEntryNamedAsTheStagingDirectoryIsLeftOut(t *testing.T) {
	b := newTreeBuilder(t)
	commit := b.commit(SortNodes([]*proto.TreeNode{
		b.dir(StagingDir, 0o755, b.file("x", "staged", 0o644)),
		b.file("x", "the file", 0o644),
	})...)

	dst := t.TempDir()
	var left []string

	restorer := &Restorer{Store: b.store}
	require.NoError(t, restorer.RestoreTree(context.Background(), commit, dst, TreeOptions{EscapeNames: true, Done: func(f RestoredFile) error {
		if f.Outcome == OutcomeUnsupportedName {
			left = append(left, filepath.Base(f.Path))
		}

		return f.Err
	}}))

	require.Equal(t, []string{StagingDir}, left)
	require.Equal(t, map[string][]byte{"x": []byte("the file")}, readTree(t, dst))
}
