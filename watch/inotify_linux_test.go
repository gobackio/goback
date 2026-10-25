package watch

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func following(t *testing.T, tr *Tracker) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for !tr.Following() {
		require.True(t, time.Now().Before(deadline), "the tracker never followed the root: %+v", tr.State())
		_ = tr.Poll(Position{})
		time.Sleep(10 * time.Millisecond)
	}
}

func inotifyTracker(t *testing.T, root string) (*Tracker, func() *inotifySource) {
	var src *inotifySource

	tr := NewTracker(func() (Source, error) {
		s, err := openInotify(root)
		src = s
		return s, err
	})
	t.Cleanup(func() { _ = tr.Close() })

	following(t, tr)

	return tr, func() *inotifySource { return src }
}

func TestAnOverflowLeavesEveryRunInDoubtUntilTheTreeIsWatchedAgain(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "a"), 0o755))

	tr, src := inotifyTracker(t, root)
	first := src()

	p, err := tr.Now()
	require.NoError(t, err)

	files := [2]*os.File{}
	for i := range files {
		files[i], err = os.Create(filepath.Join(root, "a", fmt.Sprintf("f%d", i)))
		require.NoError(t, err)
		defer files[i].Close()
	}

	// with the source held, the kernel's queue fills
	first.mtx.Lock()
	for i := range 40_000 {
		_, err := files[i%2].Write([]byte{'x'})
		require.NoError(t, err)
	}
	first.mtx.Unlock()

	err = tr.Poll(Position{})
	require.ErrorIs(t, err, ErrInDoubt)
	require.Contains(t, err.Error(), "overflowed")

	_, ok := tr.After(p)
	require.False(t, ok, "a run before the overflow is in doubt")
	require.True(t, tr.Changed(p))

	following(t, tr)
	require.NotSame(t, first, src(), "a new source watches the tree again")

	fresh, err := tr.Now()
	require.NoError(t, err)
	unchanged, ok := tr.After(fresh)
	require.True(t, ok)
	require.True(t, unchanged("a"))
}

func TestASubtreeWithoutAWatchCountsAsChanged(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"quiet", "big/one", "big/two/three"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0o755))
	}

	defer func(real func(int, string, uint32) (int, error)) { addWatch = real }(addWatch)
	addWatch = func(fd int, path string, mask uint32) (int, error) {
		if strings.Contains(path, string(filepath.Separator)+"two") {
			return -1, unix.ENOSPC
		}
		return unix.InotifyAddWatch(fd, path, mask)
	}

	tr, _ := inotifyTracker(t, root)

	p, err := tr.Now()
	require.NoError(t, err)
	require.NoError(t, tr.Poll(Position{}))

	unchanged, ok := tr.After(p)
	require.True(t, ok)
	require.True(t, unchanged("quiet"))
	require.True(t, unchanged("big/one"))
	require.False(t, unchanged("big/two"))
	require.False(t, unchanged("big/two/three"))
	require.False(t, unchanged("big"))
	require.Contains(t, tr.State().Unwatched["big/two"], "max_user_watches")

	s := &Schedule{Store: &MemoryStore{}, Trigger: nopTrigger{}}
	s.Watch("pc/files", tr, Options{MinGap: time.Minute})
	s.Check()
	require.NoError(t, s.End(s.Begin("pc/files", nil, nil), []byte("c1"), nil))

	run := s.Begin("pc/files", []byte("c1"), nil)
	require.Empty(t, run.Full)
	require.False(t, run.Unchanged("big/two"), "the subtree is read on every run")
	require.True(t, run.Unchanged("quiet"))
}

type nopTrigger struct{}

func (nopTrigger) RunNow(string) {}
