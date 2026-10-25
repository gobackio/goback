package backup

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hookFS is the local disk with a hook run before each open and listing,
// and after each listing, given the path.
type hookFS struct {
	LocalFS
	beforeOpen    func(path string)
	beforeReadDir func(dir string)
	afterReadDir  func(dir string)
	// failRead holds the error every read of a file of that name fails with
	failRead map[string]error
}

func (h *hookFS) Open(ctx context.Context, path string) (File, error) {
	if h.beforeOpen != nil {
		h.beforeOpen(path)
	}

	file, err := h.LocalFS.Open(ctx, path)
	if err != nil || h.failRead[filepath.Base(path)] == nil {
		return file, err
	}

	return failingFile{File: file, err: h.failRead[filepath.Base(path)]}, nil
}

// failingFile is a File whose reads fail.
type failingFile struct {
	File
	err error
}

func (f failingFile) Read([]byte) (int, error) { return 0, f.err }

func (h *hookFS) ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error) {
	if h.beforeReadDir != nil {
		h.beforeReadDir(dir)
	}

	entries, err := h.LocalFS.ReadDir(ctx, dir)

	if h.afterReadDir != nil {
		h.afterReadDir(dir)
	}

	return entries, err
}

// firstAt runs do the first time a hook sees path.
func firstAt(path string, do func()) func(string) {
	var done atomic.Bool

	return func(p string) {
		if p == path && done.CompareAndSwap(false, true) {
			do()
		}
	}
}

func TestAFileGoneBeforeItIsOpenedIsLeftOut(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))
	f.write("gone.txt", []byte("gone"))

	gone := filepath.Join(f.root, "gone.txt")
	f.walker.FS = &hookFS{beforeOpen: firstAt(gone, func() { assert.NoError(t, os.Remove(gone)) })}

	result := f.run()
	require.False(t, result.Dirty())

	nodes := f.tree(result.Commit.Tree)
	require.Contains(t, nodes, "a.txt")
	require.NotContains(t, nodes, "gone.txt")
}

func TestADirectoryGoneBeforeItIsListedIsLeftOut(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("keep/a.txt", []byte("a"))
	f.write("tmp/b.txt", []byte("b"))

	first := f.run()
	require.Contains(t, f.tree(first.Commit.Tree), "tmp")

	tmp := filepath.Join(f.root, "tmp")
	f.walker.FS = &hookFS{beforeReadDir: firstAt(tmp, func() { assert.NoError(t, os.RemoveAll(tmp)) })}

	second := f.run()
	require.False(t, second.Dirty())

	nodes := f.tree(second.Commit.Tree)
	require.Contains(t, nodes, "keep")
	require.NotContains(t, nodes, "tmp")
}

func TestADirectoryReplacedByAFileBeforeItIsListedIsLeftOut(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("keep/a.txt", []byte("a"))
	f.write("d/b.txt", []byte("b"))

	d := filepath.Join(f.root, "d")
	f.walker.FS = &hookFS{beforeReadDir: firstAt(d, func() {
		assert.NoError(t, os.RemoveAll(d))
		assert.NoError(t, os.WriteFile(d, []byte("now a file"), 0o644))
	})}

	result := f.run()
	require.False(t, result.Dirty())

	nodes := f.tree(result.Commit.Tree)
	require.Contains(t, nodes, "keep")
	require.NotContains(t, nodes, "d")
}

func TestAnEntryGoneBetweenItsListingAndItsStatIsLeftOutClean(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))
	f.write("b.txt", []byte("b"))

	b := filepath.Join(f.root, "b.txt")
	f.walker.FS = &hookFS{afterReadDir: firstAt(f.root, func() { assert.NoError(t, os.Remove(b)) })}

	result := f.run()
	require.Zero(t, result.Unreadable)
	require.False(t, result.Dirty())
	require.NotContains(t, f.tree(result.Commit.Tree), "b.txt")
}

func TestAFileThatFailsToReadKeepsItsPreviousVersion(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("bad.bin", []byte("v1"))
	f.write("good.txt", []byte("good"))

	first := f.run()
	before := f.tree(first.Commit.Tree)

	f.write("bad.bin", []byte("v2, longer"))
	f.walker.FS = &hookFS{failRead: map[string]error{"bad.bin": &fs.PathError{Op: "read", Path: "bad.bin", Err: ioError}}}

	second := f.run()
	require.EqualValues(t, 1, second.Unreadable)
	require.True(t, second.Dirty())
	require.True(t, f.tree(second.Commit.Tree)["bad.bin"].Ref.Equal(before["bad.bin"].Ref))
}
