//go:build !windows

package backup

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ioError is how a read from a failing medium fails.
var ioError = syscall.EIO

// swapAt writes rel and has a fixture's walk run swap as it asks Include
// about rel, once it has listed and stat'ed the entries of rel's directory
// that sort before it and before it reads any of them.
func swapAt(f *walkerFixture, rel string, swap func()) {
	f.write(rel, []byte("after"))

	f.walker.ScanWorkers = -1
	f.walker.Include = func(r string) bool {
		if r == rel {
			swap()
		}

		return true
	}
}

func TestAFileSwappedForASymlinkAfterItsListingIsNotFollowed(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("mine.txt", []byte("mine"))

	secret := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secret, []byte("root only"), 0o600))

	mine := filepath.Join(f.root, "mine.txt")
	swapAt(f, "zz.txt", func() {
		assert.NoError(t, os.Remove(mine))
		assert.NoError(t, os.Symlink(secret, mine))
	})

	result := f.run()
	require.NotContains(t, f.tree(result.Commit.Tree), "mine.txt")
}

func TestADirectorySwappedForASymlinkAfterItsListingIsNotWalked(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("d/a.txt", []byte("a"))

	elsewhere := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(elsewhere, "id_rsa"), []byte("key"), 0o600))

	d := filepath.Join(f.root, "d")
	swapAt(f, "zz.txt", func() {
		assert.NoError(t, os.Rename(d, d+".aside"))
		assert.NoError(t, os.Symlink(elsewhere, d))
	})

	result := f.run()
	require.NotContains(t, f.tree(result.Commit.Tree), "d")
}

func TestAFileWhoseDirectoryIsSwappedForASymlinkAfterItsListingIsNotRead(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("d/secret.txt", []byte("mine"))

	elsewhere := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(elsewhere, "secret.txt"), []byte("root only"), 0o600))

	d := filepath.Join(f.root, "d")
	swapAt(f, "d/zz.txt", func() {
		assert.NoError(t, os.Rename(d, d+".aside"))
		assert.NoError(t, os.Symlink(elsewhere, d))
	})

	result := f.run()
	nodes := f.tree(result.Commit.Tree)
	require.Contains(t, nodes, "d")
	require.NotContains(t, f.tree(nodes["d"].Ref), "secret.txt")
}

func TestAFileSwappedForAFIFOAfterItsListingDoesNotHangTheWalk(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("a"))
	f.write("b.txt", []byte("b"))

	a := filepath.Join(f.root, "a.txt")
	swapAt(f, "zz.txt", func() {
		assert.NoError(t, os.Remove(a))
		assert.NoError(t, syscall.Mkfifo(a, 0o644))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := f.walker.Run(ctx)
	require.NoError(t, err)

	nodes := f.tree(result.Commit.Tree)
	require.NotContains(t, nodes, "a.txt")
	require.Contains(t, nodes, "b.txt")
}

func TestAFileReplacedInItsDirectoryAfterItsListingIsReadAsTheNewFile(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("doc.txt", []byte("old"))

	doc := filepath.Join(f.root, "doc.txt")
	swapAt(f, "zz.txt", func() {
		assert.NoError(t, os.WriteFile(doc+".tmp", []byte("saved, longer"), 0o644))
		assert.NoError(t, os.Rename(doc+".tmp", doc))
	})

	result := f.run()
	require.Equal(t, "saved, longer", f.content(result.Commit.Tree, "doc.txt"))
	require.EqualValues(t, len("saved, longer"), f.tree(result.Commit.Tree)["doc.txt"].Stat.Size)
}
