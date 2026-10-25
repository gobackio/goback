package watch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func fanotifyTracker(t *testing.T, root string) (*Tracker, func() *fanotifySource) {
	if s, err := openFanotify(root); errors.Is(err, ErrUnavailable) {
		t.Skipf("%v; fanotify needs root", err)
	} else {
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}

	var src *fanotifySource

	tr := NewTracker(func() (Source, error) {
		s, err := openFanotify(root)
		src = s
		return asSource(s, err)
	})
	t.Cleanup(func() { _ = tr.Close() })

	following(t, tr)

	return tr, func() *fanotifySource { return src }
}

func TestAFanotifyOverflowLeavesEveryRunInDoubt(t *testing.T) {
	root := t.TempDir()
	tr, src := fanotifyTracker(t, root)
	first := src()

	p, err := tr.Now()
	require.NoError(t, err)

	first.mtx.Lock()
	for i := range 20_000 {
		require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprintf("f%05d", i)), nil, 0o644))
	}
	first.mtx.Unlock()

	err = tr.Poll(Position{})
	require.ErrorIs(t, err, ErrInDoubt)
	require.Contains(t, err.Error(), "overflowed")

	_, ok := tr.After(p)
	require.False(t, ok)

	following(t, tr)
	require.NotSame(t, first, src())
}

func TestAFilesystemMountedUnderTheRootIsUnwatchedAndUnmountingItIsInDoubt(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "quiet"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "mnt"), 0o755))

	if err := unix.Mount("tmpfs", filepath.Join(root, "mnt"), "tmpfs", 0, ""); err != nil {
		t.Skipf("mounting a tmpfs: %v; it needs root", err)
	}

	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(filepath.Join(root, "mnt"), 0)
		}
	})

	tr, _ := fanotifyTracker(t, root)

	p, err := tr.Now()
	require.NoError(t, err)
	require.NoError(t, tr.Poll(Position{}))

	unchanged, ok := tr.After(p)
	require.True(t, ok)
	require.True(t, unchanged("quiet"))
	require.False(t, unchanged("mnt"), "fanotify follows only the root's own filesystem")
	require.Contains(t, tr.State().Unwatched, "mnt")

	require.NoError(t, unix.Unmount(filepath.Join(root, "mnt"), 0))
	mounted = false

	require.ErrorIs(t, tr.Poll(Position{}), ErrInDoubt)
	_, ok = tr.After(p)
	require.False(t, ok)
}
