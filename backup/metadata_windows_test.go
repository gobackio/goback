//go:build windows

package backup

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// countingMetadataFS counts the security descriptors LocalFS is asked for.
type countingMetadataFS struct {
	LocalFS
	reads atomic.Int64
}

func (c *countingMetadataFS) Security(ctx context.Context, path string) ([]byte, error) {
	c.reads.Add(1)
	return c.LocalFS.Security(ctx, path)
}

func setAttributes(t *testing.T, path string, attributes uint32) {
	t.Helper()

	require.NoError(t, windows.SetFileAttributes(windows.StringToUTF16Ptr(path), attributes))
}

func TestAWalkRecordsStreamsSecurityAndAttributesAndARestorePutsThemBack(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("content"))
	f.write("sub/b.txt", []byte("other"))

	zone := "[ZoneTransfer]\r\nZoneId=3\r\n"
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "a.txt:Zone.Identifier"), []byte(zone), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "a.txt:extra"), f.random(300_000), 0o644))
	setAttributes(t, filepath.Join(f.root, "sub", "b.txt"), windows.FILE_ATTRIBUTE_HIDDEN|windows.FILE_ATTRIBUTE_ARCHIVE)

	result := f.run()
	nodes := f.tree(result.Commit.Tree)

	a := nodes["a.txt"].Stat
	require.NotNil(t, a.Security)
	require.NotZero(t, a.CtimeNs)
	require.NotZero(t, a.BirthtimeNs)
	require.Len(t, a.Streams, 2)
	require.Equal(t, "Zone.Identifier", string(a.Streams[0].Name))
	require.EqualValues(t, len(zone), a.Streams[0].Size)
	require.True(t, nodes["sub"].Stat.Security != nil, "a directory records its descriptor too")

	dst := t.TempDir()
	restorer := &Restorer{Store: f.store}
	require.NoError(t, restorer.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	content, err := os.ReadFile(filepath.Join(dst, "a.txt:Zone.Identifier"))
	require.NoError(t, err)
	require.Equal(t, zone, string(content))

	extra, err := os.ReadFile(filepath.Join(dst, "a.txt:extra"))
	require.NoError(t, err)
	want, err := os.ReadFile(filepath.Join(f.root, "a.txt:extra"))
	require.NoError(t, err)
	require.Equal(t, want, extra)

	restored, err := lstat(filepath.Join(dst, "sub", "b.txt"), "b.txt")
	require.NoError(t, err)
	stat := restored.Sys().(*fileStatInformation)
	require.NotZero(t, stat.FileAttributes&windows.FILE_ATTRIBUTE_HIDDEN)
	require.Equal(t, nodes["sub"].Stat.BirthtimeNs, filetimeNsOf(t, filepath.Join(dst, "sub")))

	info, err := os.Stat(filepath.Join(dst, "a.txt"))
	require.NoError(t, err)
	require.Equal(t, a.MtimeNs, info.ModTime().UnixNano(), "writing the streams leaves the recorded mtime")

	if !writePrivileges()[seRestorePrivilege] {
		require.EqualValues(t, 3, restorer.Stats().SecurityLeftOut)
	}
}

func filetimeNsOf(t *testing.T, path string) int64 {
	t.Helper()

	stat, err := statByName(path)
	require.NoError(t, err)

	return filetimeNs(stat.CreationTime)
}

func TestAWalkRereadsMetadataOnlyWhereTheChangeTimeMoved(t *testing.T) {
	f := newWalkerFixture(t)
	fsys := &countingMetadataFS{}
	f.walker.FS = fsys

	f.write("a.txt", []byte("one"))
	f.write("b.txt", []byte("two"))

	first := f.run()
	require.EqualValues(t, 2, fsys.reads.Load())

	fsys.reads.Store(0)
	second := f.run()
	require.Zero(t, fsys.reads.Load())
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))

	path := filepath.Join(f.root, "b.txt")
	before, err := os.Stat(path)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)
	setAttributes(t, path, windows.FILE_ATTRIBUTE_HIDDEN)

	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime())

	third := f.run()
	require.EqualValues(t, 1, fsys.reads.Load())
	require.False(t, third.Commit.Tree.Equal(second.Commit.Tree))
	require.NotZero(t, f.tree(third.Commit.Tree)["b.txt"].Stat.Attributes&windows.FILE_ATTRIBUTE_HIDDEN)

	f.walker.Verify = true
	fsys.reads.Store(0)
	f.run()
	require.EqualValues(t, 2, fsys.reads.Load())
}

// plainFS is the local disk without its MetadataFS methods.
type plainFS struct{ FS }

func TestAWalkReadsTheMetadataABaseRecordedWithoutIt(t *testing.T) {
	f := newWalkerFixture(t)
	f.walker.FS = plainFS{LocalFS{}}

	f.write("a.txt", []byte("one"))
	f.write("sub/b.txt", []byte("two"))

	first := f.run()
	require.Nil(t, f.tree(first.Commit.Tree)["a.txt"].Stat.Security)

	fsys := &countingMetadataFS{}
	f.walker.FS = fsys

	second := f.run()
	require.EqualValues(t, 3, fsys.reads.Load())
	require.NotNil(t, f.tree(second.Commit.Tree)["a.txt"].Stat.Security)
	require.NotNil(t, f.tree(second.Commit.Tree)["sub"].Stat.Security)
}

func TestEntriesSharingADescriptorStoreItOnce(t *testing.T) {
	f := newWalkerFixture(t)
	for _, name := range []string{"a", "b", "c", "d"} {
		f.write(name, []byte(name))
	}

	result := f.run()

	refs := map[string]bool{}
	for _, node := range f.tree(result.Commit.Tree) {
		refs[string(node.Stat.Security.GetHash())] = true
	}
	require.Len(t, refs, 1)

	obj, err := f.store.Get(context.Background(), &proto.Ref{Hash: []byte(firstKey(refs))})
	require.NoError(t, err)
	require.NotEmpty(t, obj.GetFile().GetInline())
}

func firstKey(m map[string]bool) string {
	for k := range m {
		return k
	}

	return ""
}

func TestASealedWalkSealsStreamNamesAndKeepsItsTreeWhenNothingChanged(t *testing.T) {
	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	f := newWalkerFixture(t)
	f.walker.Key = key
	f.write("a.txt", []byte("content"))
	for _, name := range []string{"one", "two", "three"} {
		require.NoError(t, os.WriteFile(filepath.Join(f.root, "a.txt:"+name), []byte(name), 0o644))
	}

	first := f.run()
	second := f.run()
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))

	tree, err := LoadTree(context.Background(), f.store, first.Commit.Tree)
	require.NoError(t, err)
	for _, stream := range tree.Nodes[0].Stat.Streams {
		require.NotContains(t, []string{"one", "two", "three"}, string(stream.Name))
	}

	dst := t.TempDir()
	require.NoError(t, (&Restorer{Store: f.store, Key: key}).RestoreTree(context.Background(), first.Commit, dst, TreeOptions{}))

	for _, name := range []string{"one", "two", "three"} {
		content, err := os.ReadFile(filepath.Join(dst, "a.txt:"+name))
		require.NoError(t, err)
		require.Equal(t, name, string(content))
	}
}

// cloudFS is LocalFS with the files named in cloud shown as cloud-only
// placeholders. It records which of them were opened, which downloads them.
type cloudFS struct {
	LocalFS
	cloud  map[string]bool
	mtx    sync.Mutex
	opened []string
}

func (c *cloudFS) ReadDir(ctx context.Context, dir string) ([]os.DirEntry, error) {
	entries, err := c.LocalFS.ReadDir(ctx, dir)

	for i, entry := range entries {
		if c.cloud[entry.Name()] {
			entries[i] = cloudEntry{entry}
		}
	}

	return entries, err
}

func (c *cloudFS) Open(ctx context.Context, path string) (File, error) {
	if c.cloud[filepath.Base(path)] {
		c.mtx.Lock()
		c.opened = append(c.opened, filepath.Base(path))
		c.mtx.Unlock()
	}

	return c.LocalFS.Open(ctx, path)
}

type cloudEntry struct{ os.DirEntry }

func (e cloudEntry) Info() (os.FileInfo, error) {
	info, err := e.DirEntry.Info()
	if err != nil {
		return nil, err
	}

	stat := *info.Sys().(*fileStatInformation)
	stat.FileAttributes |= recallAttributes

	return &identified{name: info.Name(), stat: stat}, nil
}

func TestACloudOnlyFileIsRecordedWithoutContentAndLeftOutOfARestore(t *testing.T) {
	f := newWalkerFixture(t)
	fsys := &cloudFS{cloud: map[string]bool{"cloud.txt": true}}
	f.walker.FS = fsys

	f.write("local.txt", []byte("here"))
	f.write("cloud.txt", []byte("only in the cloud"))

	first := f.run()
	require.Empty(t, fsys.opened)
	require.EqualValues(t, 1, first.Placeholders)
	require.EqualValues(t, 1, first.Files)

	node := f.tree(first.Commit.Tree)["cloud.txt"]
	require.True(t, cloudOnly(node.Stat))
	require.EqualValues(t, len("only in the cloud"), node.Stat.Size)
	require.NotNil(t, node.Stat.Security)

	dst := t.TempDir()
	restorer := &Restorer{Store: f.store}
	require.NoError(t, restorer.RestoreTree(context.Background(), first.Commit, dst, TreeOptions{}))
	require.NoFileExists(t, filepath.Join(dst, "cloud.txt"))
	require.FileExists(t, filepath.Join(dst, "local.txt"))
	require.EqualValues(t, 1, restorer.Stats().CloudOnly)

	// once the sync client has downloaded it, the next run reads it
	delete(fsys.cloud, "cloud.txt")
	second := f.run()
	require.Zero(t, second.Placeholders)

	content, err := (&Restorer{Store: f.store}).ReadFile(context.Background(), f.tree(second.Commit.Tree)["cloud.txt"].Stat, f.tree(second.Commit.Tree)["cloud.txt"].Ref)
	require.NoError(t, err)
	require.Equal(t, "only in the cloud", string(content))
}

func TestDownloadPlaceholdersReadsACloudOnlyFile(t *testing.T) {
	f := newWalkerFixture(t)
	fsys := &cloudFS{cloud: map[string]bool{"cloud.txt": true}}
	f.walker.FS = fsys
	f.walker.DownloadPlaceholders = true

	f.write("cloud.txt", []byte("downloaded"))

	result := f.run()
	require.Zero(t, result.Placeholders)
	require.EqualValues(t, 1, result.Files)
	require.Equal(t, []string{"cloud.txt"}, fsys.opened)
}
