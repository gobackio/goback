package watch_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/watch"
	"github.com/stretchr/testify/require"
)

// listing is the local disk, recording the directories a walk lists.
type listing struct {
	backup.LocalFS

	root   string
	mtx    sync.Mutex
	listed []string
}

func (l *listing) ReadDir(ctx context.Context, dir string) ([]fs.DirEntry, error) {
	rel, _ := filepath.Rel(l.root, dir)
	if rel == "." {
		rel = ""
	}

	l.mtx.Lock()
	l.listed = append(l.listed, filepath.ToSlash(rel))
	l.mtx.Unlock()

	return l.LocalFS.ReadDir(ctx, dir)
}

func (l *listing) take() []string {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	out := l.listed
	l.listed = nil
	sort.Strings(out)

	// the scanner may list a directory ahead of the walk, which lists it again
	return slices.Compact(out)
}

// treeFixture backs up a directory followed by a watch.Schedule.
type treeFixture struct {
	t     *testing.T
	ctx   context.Context
	repo  *goback.Repo
	root  string
	fs    *listing
	tr    *watch.Tracker
	sched *watch.Schedule
}

func newTreeFixture(t *testing.T, tracker func(root string) *watch.Tracker) *treeFixture {
	ctx := context.Background()

	repo, err := goback.Open(ctx, goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	f := &treeFixture{t: t, ctx: ctx, repo: repo, root: root, fs: &listing{root: root}}
	f.tr = tracker(root)
	t.Cleanup(func() { _ = f.tr.Close() })

	f.sched = &watch.Schedule{Store: &watch.MemoryStore{}, Trigger: &triggers{}}
	f.sched.Watch("pc/files", f.tr, watch.Options{MinGap: time.Minute, FullReadEvery: 7 * 24 * time.Hour})

	return f
}

func (f *treeFixture) write(name, content string) {
	path := filepath.Join(f.root, filepath.FromSlash(name))
	require.NoError(f.t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(f.t, os.WriteFile(path, []byte(content), 0o644))

	// older than any run's scan start, or the next run reads it again as
	// racily clean
	old := time.Now().Add(-time.Hour)
	require.NoError(f.t, os.Chtimes(path, old, old))
}

// follow polls until the tracker follows the root.
func (f *treeFixture) follow() {
	deadline := time.Now().Add(20 * time.Second)

	for !f.tr.Following() {
		require.True(f.t, time.Now().Before(deadline), "the tracker never followed the root: %+v", f.tr.State())
		_ = f.tr.Poll(watch.Position{})
		time.Sleep(10 * time.Millisecond)
	}
}

// backup runs the set once through the schedule and returns what it read
// and the directories it listed.
func (f *treeFixture) backup() (watch.Run, *backup.WalkResult, []string) {
	latest, err := f.repo.Index.LatestCommit(f.ctx, "pc/files")
	if err != nil {
		latest = nil
	}

	run := f.sched.Begin("pc/files", latest.GetHash(), nil)

	result, err := job.Path(f.ctx, &job.Env{Store: f.repo.Index, Index: f.repo.Index, AgentID: "pc"}, job.PathSettings{
		Set: "pc/files", Dir: f.root, FS: f.fs, ProgressInterval: -1, Unchanged: run.Unchanged,
	})
	require.NoError(f.t, err)

	commit := result.Ref
	if commit == nil {
		commit = result.Base
	}

	require.NoError(f.t, f.sched.End(run, commit.GetHash(), nil))

	return run, result, f.fs.take()
}

func (f *treeFixture) held(name string) bool {
	versions, err := f.repo.Index.FileInfo(f.ctx, "pc/files", name, time.Now().Add(time.Hour), 1)
	require.NoError(f.t, err)

	return len(versions) > 0
}

func (f *treeFixture) seed() {
	for _, name := range []string{"a/b/c/deep.txt", "a/b/other.txt", "a/x/y.txt", "d/e/f.txt", "top.txt"} {
		f.write(name, name)
	}
}

// backends runs test with a tracker of each Linux backend, fanotify only
// where the process may use it.
func backends(t *testing.T, test func(t *testing.T, follow func(root string) *watch.Tracker)) {
	for _, b := range []struct {
		name string
		open func(root string) (watch.Source, error)
	}{{"inotify", watch.OpenInotify}, {"fanotify", watch.OpenFanotify}} {
		t.Run(b.name, func(t *testing.T) {
			probe, err := b.open(t.TempDir())
			if errors.Is(err, watch.ErrUnavailable) {
				t.Skipf("%v; fanotify needs root", err)
			}
			require.NoError(t, err)
			require.NoError(t, probe.Close())

			test(t, func(root string) *watch.Tracker {
				return watch.NewTracker(func() (watch.Source, error) { return b.open(root) })
			})
		})
	}
}

func allDirs() []string {
	return []string{"", "a", "a/b", "a/b/c", "a/x", "d", "d/e"}
}

func TestAnIdleTreeListsNoDirectoryButItsRoot(t *testing.T) {
	backends(t, anIdleTreeListsNoDirectoryButItsRoot)
}

func anIdleTreeListsNoDirectoryButItsRoot(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()

	first, _, listed := f.backup()
	require.Equal(t, watch.FullFirst, first.Full)
	require.Equal(t, allDirs(), listed)

	second, result, listed := f.backup()
	require.Empty(t, second.Full)
	require.Equal(t, []string{""}, listed, "the walk always lists the root, and nothing under it")
	require.Zero(t, result.Read)
}

func TestADeepChangeListsOnlyItsPath(t *testing.T) {
	backends(t, aDeepChangeListsOnlyItsPath)
}

func aDeepChangeListsOnlyItsPath(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()
	f.backup()

	f.write("a/b/c/deep.txt", "edited deep down")

	run, result, listed := f.backup()
	require.Empty(t, run.Full)
	require.Equal(t, []string{"", "a", "a/b", "a/b/c"}, listed)
	require.EqualValues(t, 1, result.Read)
}

func TestANewDirectoryAndWhatIsCreatedInItAreCaught(t *testing.T) {
	backends(t, aNewDirectoryAndWhatIsCreatedInItAreCaught)
}

func aNewDirectoryAndWhatIsCreatedInItAreCaught(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()
	f.backup()

	outside := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "moved", "in"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "moved", "in", "m.txt"), []byte("m"), 0o644))

	for i := range 20 {
		f.write(filepath.Join("d", "new", "deeper", strings.Repeat("n", i+1)+".txt"), "new")
	}
	require.NoError(t, os.Rename(filepath.Join(outside, "moved"), filepath.Join(f.root, "a", "x", "moved")))

	run, result, listed := f.backup()
	require.Empty(t, run.Full)
	require.EqualValues(t, 21, result.Read)
	require.Subset(t, listed, []string{"d", "d/new", "d/new/deeper", "a/x", "a/x/moved", "a/x/moved/in"})
	require.NotContains(t, listed, "a/b")
	require.True(t, f.held("a/x/moved/in/m.txt"))

	f.write("a/x/moved/in/m.txt", "edited after the move")
	f.write("d/new/deeper/n.txt", "edited in a new directory")

	_, result, listed = f.backup()
	require.EqualValues(t, 2, result.Read, "the new directories are watched")
	require.Equal(t, []string{"", "a", "a/x", "a/x/moved", "a/x/moved/in", "d", "d/new", "d/new/deeper"}, listed)
}

func TestADirectoryMovedWithinTheTreeIsFollowedAtItsNewPath(t *testing.T) {
	backends(t, aDirectoryMovedWithinTheTreeIsFollowedAtItsNewPath)
}

func aDirectoryMovedWithinTheTreeIsFollowedAtItsNewPath(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()
	f.backup()

	require.NoError(t, os.Rename(filepath.Join(f.root, "a", "b"), filepath.Join(f.root, "d", "b")))
	f.backup()

	f.write("d/b/c/deep.txt", "edited after the move")

	_, result, listed := f.backup()
	require.EqualValues(t, 1, result.Read)
	require.Equal(t, []string{"", "d", "d/b", "d/b/c"}, listed)
}

func TestAChangeToAHardLinkedFileReadsEverything(t *testing.T) {
	backends(t, aChangeToAHardLinkedFileReadsEverything)
}

func aChangeToAHardLinkedFileReadsEverything(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	require.NoError(t, os.Link(filepath.Join(f.root, "a", "x", "y.txt"), filepath.Join(f.root, "d", "e", "y.txt")))
	f.follow()
	f.backup()

	f.write("a/x/y.txt", "edited through one link")

	_, _, listed := f.backup()
	require.Equal(t, allDirs(), listed, "the other link's directory hears nothing")
}

func TestTheRootMovingAwayLeavesEveryRunInDoubt(t *testing.T) {
	backends(t, theRootMovingAwayLeavesEveryRunInDoubt)
}

func theRootMovingAwayLeavesEveryRunInDoubt(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()
	f.backup()

	p, err := f.tr.Now()
	require.NoError(t, err)

	away := f.root + ".away"
	require.NoError(t, os.Rename(f.root, away))
	t.Cleanup(func() { _ = os.RemoveAll(away) })

	require.ErrorIs(t, f.tr.Poll(watch.Position{}), watch.ErrInDoubt)

	_, ok := f.tr.After(p)
	require.False(t, ok)

	require.NoError(t, os.Mkdir(f.root, 0o755))
	f.follow()

	require.True(t, slices.Contains([]string{"inotify", "fanotify"}, f.tr.State().Backend))
}

func TestADirectoryReplacedByAnotherOfTheSameNameIsReadWhole(t *testing.T) {
	backends(t, aDirectoryReplacedByAnotherOfTheSameNameIsReadWhole)
}

func aDirectoryReplacedByAnotherOfTheSameNameIsReadWhole(t *testing.T, follow func(root string) *watch.Tracker) {
	f := newTreeFixture(t, follow)
	f.seed()
	f.follow()
	f.backup()

	other := filepath.Join(t.TempDir(), "b")
	require.NoError(t, os.MkdirAll(filepath.Join(other, "c"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(other, "c", "deep.txt"), []byte("another tree's file"), 0o644))

	require.NoError(t, os.Rename(filepath.Join(f.root, "a", "b"), filepath.Join(t.TempDir(), "gone")))
	require.NoError(t, os.Rename(other, filepath.Join(f.root, "a", "b")))

	_, result, listed := f.backup()
	require.Equal(t, []string{"", "a", "a/b", "a/b/c"}, listed)
	require.EqualValues(t, 1, result.Read)
	require.EqualValues(t, 2, result.Files, "top.txt and the new deep.txt; a/b/other.txt went with the old directory")
}
