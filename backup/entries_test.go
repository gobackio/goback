package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hookFS is the local disk with a hook run before each open and listing,
// and after each listing, given the path.
type hookFS struct {
	LocalFS
	beforeOpen    func(path string)
	beforeLstat   func(path string)
	beforeReadDir func(dir string)
	afterReadDir  func(dir string)
	// failRead and failReadDir hold the error every read of a file, or
	// listing of a directory, of that name fails with
	failRead    map[string]error
	failReadDir map[string]error
	// calls counts opens and listings, and listing those under way
	calls, listing atomic.Int64
}

func (h *hookFS) Open(ctx context.Context, path string) (File, error) {
	h.calls.Add(1)

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

func (h *hookFS) Lstat(ctx context.Context, path string) (os.FileInfo, error) {
	if h.beforeLstat != nil {
		h.beforeLstat(path)
	}

	return h.LocalFS.Lstat(ctx, path)
}

func (h *hookFS) ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error) {
	h.calls.Add(1)
	h.listing.Add(1)
	defer h.listing.Add(-1)

	if h.beforeReadDir != nil {
		h.beforeReadDir(dir)
	}

	if err := h.failReadDir[filepath.Base(dir)]; err != nil {
		return nil, err
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

// slowTree writes dirs directories of root, each holding a file and a
// directory holding another, and returns a hookFS whose listings of them
// take a while and whose listing of zfail, which it writes too, fails.
func slowTree(f *walkerFixture, dirs int) *hookFS {
	for i := range dirs {
		f.write(fmt.Sprintf("slow%02d/file.txt", i), []byte("content"))
		f.write(fmt.Sprintf("slow%02d/inner/file.txt", i), []byte("content"))
	}
	f.write("zfail/file.txt", []byte("content"))

	return &hookFS{
		beforeReadDir: func(dir string) {
			if strings.HasPrefix(filepath.Base(dir), "slow") || filepath.Base(dir) == "inner" {
				time.Sleep(100 * time.Millisecond)
			}
		},
		failReadDir: map[string]error{"zfail": errors.New("the medium is gone")},
	}
}

func TestAFailedWalkStopsWhatItsDirectoriesStartedBeforeItReturns(t *testing.T) {
	f := newWalkerFixture(t)
	fsys := slowTree(f, dirWalkers)
	f.walker.FS = fsys
	f.walker.ScanWorkers = -1

	_, err := f.walker.Run(context.Background())
	require.ErrorContains(t, err, "the medium is gone")

	calls := fsys.calls.Load()
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, calls, fsys.calls.Load(), "nothing reads on once the run returned")
}

func TestAFailedWalkStopsItsReadAheadBeforeItReturns(t *testing.T) {
	f := newWalkerFixture(t)
	fsys := slowTree(f, 2*dirWalkers)
	f.walker.FS = fsys
	f.walker.ScanWorkers = 4

	_, err := f.walker.Run(context.Background())
	require.ErrorContains(t, err, "the medium is gone")
	require.Zero(t, fsys.listing.Load(), "no listing is under way once the run returned")
}

func TestAFileReplacedByASymlinkWhileItIsReadKeepsTheRead(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("conf.txt", []byte("setting=1"))

	other := filepath.Join(t.TempDir(), "other.txt")
	require.NoError(t, os.WriteFile(other, []byte("elsewhere"), 0o644))

	if err := os.Symlink(other, filepath.Join(t.TempDir(), "probe")); err != nil {
		t.Skip("symlinks cannot be made here:", err)
	}

	conf := filepath.Join(f.root, "conf.txt")
	f.walker.FS = &hookFS{beforeLstat: firstAt(conf, func() {
		assert.NoError(t, os.Remove(conf))
		assert.NoError(t, os.Symlink(other, conf))
	})}

	result := f.run()
	require.Equal(t, proto.NodeType_NODE_FILE, f.tree(result.Commit.Tree)["conf.txt"].Stat.Type)
	require.Equal(t, "setting=1", f.content(result.Commit.Tree, "conf.txt"))
}
