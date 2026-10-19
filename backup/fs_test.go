package backup

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// fixtureFS is the FS every walker fixture reads: nil, or LocalFS when
// GOBACK_TEST_FS is local.
var fixtureFS = func() FS {
	if os.Getenv("GOBACK_TEST_FS") == "local" {
		return LocalFS{}
	}

	return nil
}()

// descriptorPuts is how many objects a fixture's first run stores for the
// security descriptor its files share: one where LocalFS records them.
var descriptorPuts = func() int64 {
	if _, ok := any(LocalFS{}).(MetadataFS); ok {
		return 1
	}

	return 0
}()

// descriptorBytes is the size of the distinct security descriptors LocalFS
// records for paths, 0 where it records none.
func descriptorBytes(t *testing.T, paths ...string) int64 {
	t.Helper()

	fsys, ok := any(LocalFS{}).(MetadataFS)
	if !ok {
		return 0
	}

	distinct := map[string]bool{}
	for _, path := range paths {
		sd, err := fsys.Security(context.Background(), path)
		require.NoError(t, err)

		distinct[string(sd)] = true
	}

	var size int64
	for sd := range distinct {
		size += int64(len(sd))
	}

	return size
}

func TestThePackageTestsPassWithLocalFSSetExplicitly(t *testing.T) {
	if fixtureFS != nil {
		t.Skip("this is the run with LocalFS set")
	}

	args := []string{"-test.run", "^Test", "-test.count=1"}
	if testing.Short() {
		args = append(args, "-test.short")
	}

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "GOBACK_TEST_FS=local")

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

// memFS is an FS held in memory, as a remote driver would present one:
// mtimes truncated to its resolution and stamped by its own clock.
type memFS struct {
	mtx    sync.Mutex
	nodes  map[string]*memNode
	traits Traits
	// behind is how far the FS's clock runs behind the wall time a write
	// is given at
	behind time.Duration
	// coarse makes the server underneath report whole seconds, as a
	// listing without fractional times does
	coarse bool
	opens  int
}

type memNode struct {
	data   []byte
	mtime  time.Time
	mode   os.FileMode
	target string
}

func newMemFS(traits Traits) *memFS {
	return &memFS{
		nodes:  map[string]*memNode{"/": {mode: os.ModeDir | 0o755}},
		traits: traits,
	}
}

// write puts a file at p, written at the wall time at, making its parents.
func (m *memFS) write(p, data string, at time.Time) {
	m.put(p, &memNode{data: []byte(data), mtime: at.Add(-m.behind), mode: 0o644})
}

func (m *memFS) symlink(p, target string, at time.Time) {
	m.put(p, &memNode{target: target, mtime: at.Add(-m.behind), mode: os.ModeSymlink | 0o777})
}

func (m *memFS) put(p string, node *memNode) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for dir := path.Dir(p); m.nodes[dir] == nil; dir = path.Dir(dir) {
		m.nodes[dir] = &memNode{mode: os.ModeDir | 0o755, mtime: node.mtime}
	}

	m.nodes[p] = node
}

func (m *memFS) info(p string) (os.FileInfo, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	node := m.nodes[p]
	if node == nil {
		return nil, fs.ErrNotExist
	}

	mtime := node.mtime
	if m.coarse {
		mtime = mtime.Truncate(time.Second)
	}

	if m.traits.Resolution > 0 {
		mtime = mtime.Truncate(m.traits.Resolution)
	}

	return &memInfo{name: path.Base(p), size: int64(len(node.data)), mode: node.mode, mtime: mtime}, nil
}

func (m *memFS) ReadDir(_ context.Context, dir string) ([]fs.DirEntry, error) {
	m.mtx.Lock()
	var entries []fs.DirEntry
	for p, node := range m.nodes {
		if p != "/" && path.Dir(p) == dir {
			entries = append(entries, &memEntry{fs: m, path: p, mode: node.mode})
		}
	}
	m.mtx.Unlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	return entries, nil
}

func (m *memFS) Stat(_ context.Context, p string) (os.FileInfo, error) {
	return m.info(p)
}

func (m *memFS) Lstat(_ context.Context, p string) (os.FileInfo, error) {
	return m.info(p)
}

func (m *memFS) Open(_ context.Context, p string) (File, error) {
	info, err := m.info(p)
	if err != nil {
		return nil, err
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.opens++

	return &memFile{r: bytes.NewReader(m.nodes[p].data), info: info}, nil
}

func (m *memFS) Readlink(_ context.Context, p string) (string, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.nodes[p].target, nil
}

func (m *memFS) Join(dir, name string) string {
	return path.Join(dir, name)
}

func (m *memFS) Owners(os.FileInfo) (string, string) {
	return "game", "game"
}

func (m *memFS) Traits() Traits {
	return m.traits
}

func (m *memFS) openCount() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	return m.opens
}

type memEntry struct {
	fs   *memFS
	path string
	mode os.FileMode
}

func (e *memEntry) Name() string               { return path.Base(e.path) }
func (e *memEntry) IsDir() bool                { return e.mode.IsDir() }
func (e *memEntry) Type() fs.FileMode          { return e.mode.Type() }
func (e *memEntry) Info() (fs.FileInfo, error) { return e.fs.info(e.path) }

type memInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	mtime time.Time
}

func (i *memInfo) Name() string       { return i.name }
func (i *memInfo) Size() int64        { return i.size }
func (i *memInfo) Mode() os.FileMode  { return i.mode }
func (i *memInfo) ModTime() time.Time { return i.mtime }
func (i *memInfo) IsDir() bool        { return i.mode.IsDir() }
func (i *memInfo) Sys() any           { return nil }

// memFile has no ReadAt, as a remote file has none.
type memFile struct {
	r    *bytes.Reader
	info os.FileInfo
}

func (f *memFile) Read(p []byte) (int, error) { return f.r.Read(p) }
func (f *memFile) Close() error               { return nil }
func (f *memFile) Stat() (os.FileInfo, error) { return f.info, nil }

// newMemFSFixture is a walker fixture over a memFS rooted at /, with a
// clock the test sets before each run.
func newMemFSFixture(t *testing.T, traits Traits) (*walkerFixture, *memFS, *time.Time) {
	t.Helper()

	f := newWalkerFixture(t)
	fsys := newMemFS(traits)
	now := new(time.Time)

	f.walker.FS = fsys
	f.walker.Root = "/"
	f.walker.clock = func() time.Time { return *now }

	return f, fsys, now
}

func TestAWalkOverAnFSCommitsItsTree(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{Resolution: time.Second})
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	latin1 := "caf\xe9.txt"

	fsys.write("/world/level.dat", "level", at)
	fsys.write("/"+latin1, "not utf-8", at)
	fsys.symlink("/plugins", "../shared/plugins", at)

	*now = at.Add(time.Minute)
	first := f.run()
	require.EqualValues(t, 2, first.Read)

	root := f.tree(first.Commit.Tree)
	require.Equal(t, "not utf-8", f.content(first.Commit.Tree, latin1))
	require.Equal(t, "../shared/plugins", string(root["plugins"].Stat.LinkTarget))
	require.Equal(t, "game", string(root[latin1].Stat.User))
	require.Equal(t, "level", f.content(root["world"].Ref, "level.dat"))

	*now = at.Add(2 * time.Minute)
	second := f.run()
	require.EqualValues(t, 2, second.Reused)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))
}

func TestAWalkOverAnFSPutsItsRootInSubdir(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{})
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	fsys.write("/server.properties", "motd", at)

	f.walker.Subdir = "config"
	*now = at.Add(time.Minute)
	result := f.run()

	root := f.tree(result.Commit.Tree)
	require.Equal(t, "motd", f.content(root["config"].Ref, "server.properties"))
}

func TestAWalkOverAnotherFSTakesNoGrowingFiles(t *testing.T) {
	f, _, _ := newMemFSFixture(t, Traits{})
	f.walker.Growing = []Growing{{Name: "wal.partial", Path: "/wal", Size: 1}}

	_, err := f.walker.Run(context.Background())
	require.Error(t, err)

	_, err = f.index.LatestCommit(context.Background(), "test")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestHashFileFSMatchesTheStoredRef(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{})
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	fsys.write("/big.bin", strings.Repeat("goback", 100_000), at)

	*now = at.Add(time.Minute)
	result := f.run()

	ref, err := HashFileFS(context.Background(), fsys, "/big.bin", nil)
	require.NoError(t, err)
	require.True(t, ref.Equal(f.tree(result.Commit.Tree)["big.bin"].Ref))
}

// noon is when the weak-timestamp tests' files are first written.
var noon = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

func TestAWeakMtimeRereadsAFileRewrittenWithinItsResolution(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		resolution               time.Duration
		written, scan, rewritten time.Duration
	}{
		{"minute", time.Minute, 10 * time.Second, 30 * time.Second, 40 * time.Second},
		{"second", time.Second, 100 * time.Millisecond, 500 * time.Millisecond, 800 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, fsys, now := newMemFSFixture(t, Traits{Resolution: tc.resolution})
			fsys.write("/level.dat", "version-1", noon.Add(tc.written))

			*now = noon.Add(tc.scan)
			f.run()

			// same size, and the same mtime once truncated to the resolution
			fsys.write("/level.dat", "version-2", noon.Add(tc.rewritten))

			*now = noon.Add(5 * time.Minute)
			second := f.run()
			require.EqualValues(t, 1, second.Read)
			require.Equal(t, "version-2", f.content(second.Commit.Tree, "level.dat"))

			*now = noon.Add(10 * time.Minute)
			third := f.run()
			require.EqualValues(t, 1, third.Reused)
			require.EqualValues(t, 0, third.Read)
		})
	}
}

func TestSkewWidensTheRacyWindow(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{Resolution: time.Second, Skew: 2 * time.Minute})
	fsys.behind = 90 * time.Second

	*now = noon
	fsys.write("/level.dat", "version-1", noon.Add(200*time.Millisecond))
	f.run()

	fsys.write("/level.dat", "version-2", noon.Add(700*time.Millisecond))

	*now = noon.Add(5 * time.Minute)
	second := f.run()
	require.EqualValues(t, 1, second.Read, "the change just after the scan is read though the FS stamps it 90s earlier")
	require.Equal(t, "version-2", f.content(second.Commit.Tree, "level.dat"))

	*now = noon.Add(10 * time.Minute)
	require.EqualValues(t, 1, f.run().Reused)
}

func TestMtimesTruncatedByTheDriverKeepAFlappingServerFromRereading(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{Resolution: time.Second})
	fsys.write("/level.dat", "level", noon.Add(300*time.Millisecond))
	fsys.write("/region/r.0.0.mca", "region", noon.Add(600*time.Millisecond))

	*now = noon.Add(time.Hour)
	f.run()

	for run := range 4 {
		fsys.coarse = run%2 == 0
		*now = noon.Add(time.Duration(run+2) * time.Hour)

		result := f.run()
		require.EqualValues(t, 2, result.Reused)
		require.EqualValues(t, 0, result.Read)
	}
}

func TestVerifyReadsEveryFileAndUploadsNoChunkAgain(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{Resolution: time.Second})
	fsys.write("/region/r.0.0.mca", string(f.random(300<<10)), noon)
	fsys.write("/region/r.0.1.mca", string(f.random(200<<10)), noon)

	*now = noon.Add(time.Hour)
	first := f.run()

	f.walker.Verify = true
	*now = noon.Add(2 * time.Hour)
	second := f.run()
	require.EqualValues(t, 2, second.Read)
	require.EqualValues(t, 0, second.Reused)
	require.EqualValues(t, 0, second.Uploaded)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))
}

func TestVerifyCatchesAWriteThatKeptMtimeAndSize(t *testing.T) {
	f, fsys, now := newMemFSFixture(t, Traits{Resolution: time.Second})
	fsys.write("/level.dat", "version-1", noon)

	*now = noon.Add(time.Hour)
	f.run()

	// touch -r: new content under the old mtime
	fsys.write("/level.dat", "version-2", noon)

	*now = noon.Add(2 * time.Hour)
	missed := f.run()
	require.Equal(t, "version-1", f.content(missed.Commit.Tree, "level.dat"))

	f.walker.Verify = true
	*now = noon.Add(3 * time.Hour)
	verified := f.run()
	require.Equal(t, "version-2", f.content(verified.Commit.Tree, "level.dat"))
}

// vetoingStatCache counts its calls and contradicts every file it is asked
// about.
type vetoingStatCache struct {
	calls atomic.Int64
}

func (c *vetoingStatCache) Lookup(string, os.FileInfo) (bool, bool) {
	c.calls.Add(1)
	return true, false
}

func (c *vetoingStatCache) Store(string, os.FileInfo, *proto.Ref) error {
	c.calls.Add(1)
	return nil
}

func TestTheStatCacheIsConsultedOnlyWithIdentity(t *testing.T) {
	for _, identity := range []bool{false, true} {
		t.Run(fmt.Sprint("identity ", identity), func(t *testing.T) {
			f, fsys, now := newMemFSFixture(t, Traits{Identity: identity})
			cache := &vetoingStatCache{}
			f.walker.Cache = cache
			fsys.write("/level.dat", "level", noon)

			*now = noon.Add(time.Hour)
			f.run()

			*now = noon.Add(2 * time.Hour)
			second := f.run()

			if identity {
				require.EqualValues(t, 1, second.Read)
				require.NotZero(t, cache.calls.Load())
			} else {
				require.EqualValues(t, 1, second.Reused)
				require.Zero(t, cache.calls.Load())
			}
		})
	}
}

func TestSampledRehashesNeedReread(t *testing.T) {
	for _, reread := range []bool{false, true} {
		t.Run(fmt.Sprint("reread ", reread), func(t *testing.T) {
			f, fsys, now := newMemFSFixture(t, Traits{Reread: reread})
			f.walker.ForceHashPercent = 100
			fsys.write("/level.dat", "level", noon)
			fsys.write("/region/r.0.0.mca", "region", noon)

			*now = noon.Add(time.Hour)
			f.run()
			opened := fsys.openCount()

			*now = noon.Add(2 * time.Hour)
			second := f.run()
			require.EqualValues(t, 2, second.Reused)

			if reread {
				require.Equal(t, opened+2, fsys.openCount())
			} else {
				require.Equal(t, opened, fsys.openCount())
			}
		})
	}
}
