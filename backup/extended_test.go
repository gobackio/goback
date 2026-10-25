package backup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// extFS is a memFS whose entries carry extended attributes and flags.
type extFS struct {
	*memFS

	mtx sync.Mutex
	ext map[string]Extended
}

func (f *extFS) Extended(_ context.Context, p string) (Extended, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	return f.ext[p], nil
}

func (f *extFS) set(p string, ext Extended) {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.ext[p] = ext
}

func (f *walkerFixture) xattr(stat *proto.FileInfo, name string) string {
	f.t.Helper()

	for _, xattr := range stat.GetXattrs() {
		if string(xattr.GetName()) == name {
			content, err := NewBackupReader(f.store).ReadFile(context.Background(), xattr.GetRef())
			require.NoError(f.t, err)

			value, err := io.ReadAll(content)
			require.NoError(f.t, err)

			return string(value)
		}
	}

	f.t.Fatalf("no extended attribute %s", name)

	return ""
}

func names(streams []*proto.Stream) []string {
	var out []string
	for _, stream := range streams {
		out = append(out, string(stream.GetName()))
	}

	return out
}

func TestAWalkRecordsExtendedAttributesAndFlagsWithoutReadingAnUnchangedFileAgain(t *testing.T) {
	f, mem, now := newMemFSFixture(t, Traits{})
	fsys := &extFS{memFS: mem, ext: map[string]Extended{}}
	f.walker.FS = fsys

	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	label := []byte("system_u:object_r:httpd_sys_content_t:s0")

	mem.write("/www/index.html", "hi", at)
	mem.write("/www/style.css", "body{}", at)
	fsys.set("/www/index.html", Extended{Xattrs: map[string][]byte{"security.selinux": label, "user.origin": []byte("cms")}, LinuxFlags: 0x40})
	fsys.set("/www/style.css", Extended{Xattrs: map[string][]byte{"security.selinux": label}})
	fsys.set("/www", Extended{Xattrs: map[string][]byte{"system.posix_acl_default": []byte("acl")}, DarwinFlags: 0x8000})

	*now = at.Add(time.Minute)
	first := f.run()

	www := f.tree(first.Commit.Tree)["www"]
	require.Equal(t, "acl", f.xattr(www.Stat, "system.posix_acl_default"))
	require.EqualValues(t, 0x8000, www.Stat.DarwinFlags)

	files := f.tree(www.Ref)
	index := files["index.html"].Stat
	require.Equal(t, []string{"security.selinux", "user.origin"}, names(index.Xattrs))
	require.Equal(t, string(label), f.xattr(index, "security.selinux"))
	require.EqualValues(t, len(label), index.Xattrs[0].Size)
	require.EqualValues(t, 0x40, index.LinuxFlags)
	require.True(t, index.Xattrs[0].Ref.Equal(files["style.css"].Stat.Xattrs[0].Ref), "one label, one object")

	opens := mem.openCount()

	*now = at.Add(2 * time.Minute)
	second := f.run()
	require.EqualValues(t, 2, second.Reused)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))

	fsys.set("/www/index.html", Extended{Xattrs: map[string][]byte{"security.selinux": label}, LinuxFlags: 0x50})

	*now = at.Add(3 * time.Minute)
	third := f.run()
	require.EqualValues(t, 2, third.Reused)
	require.Equal(t, opens, mem.openCount())

	index = f.tree(f.tree(third.Commit.Tree)["www"].Ref)["index.html"].Stat
	require.Equal(t, []string{"security.selinux"}, names(index.Xattrs))
	require.EqualValues(t, 0x50, index.LinuxFlags)
}

// failingExtFS fails to read what an entry carries beyond its stat.
type failingExtFS struct {
	*memFS
}

func (failingExtFS) Extended(context.Context, string) (Extended, error) {
	return Extended{}, os.ErrPermission
}

func TestExtendedAttributesThatCannotBeReadKeepThePreviousOnes(t *testing.T) {
	f, mem, now := newMemFSFixture(t, Traits{})
	fsys := &extFS{memFS: mem, ext: map[string]Extended{}}
	f.walker.FS = fsys

	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	mem.write("/a", "a", at)
	fsys.set("/a", Extended{Xattrs: map[string][]byte{"user.k": []byte("v")}, LinuxFlags: 0x10})

	*now = at.Add(time.Minute)
	first := f.run()

	f.walker.FS = failingExtFS{mem}

	*now = at.Add(2 * time.Minute)
	second := f.run()
	require.EqualValues(t, 1, second.Unreadable)
	require.True(t, second.Commit.Tree.Equal(first.Commit.Tree))
}

func TestSealedExtendedAttributeNamesAndHardLinksOpenAgain(t *testing.T) {
	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	ref := proto.HashPayload(proto.ObjectType_FILE, nil)
	info := &proto.FileInfo{
		Name:     []byte("b"),
		Xattrs:   []*proto.Stream{{Name: []byte("user.a"), Ref: ref}, {Name: []byte("user.b"), Ref: ref}},
		HardLink: []byte("dir/a"),
	}

	sealed := sealInfo(key, []byte("parent"), info)
	require.NotEqual(t, info.HardLink, sealed.HardLink)
	require.NotContains(t, names(sealed.Xattrs), "user.a")
	require.NoError(t, proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{{Stat: sealed, Ref: ref}}}).Validate())

	opened, err := openInfo(key, []byte("parent"), sealed)
	require.NoError(t, err)
	require.Equal(t, "dir/a", string(opened.HardLink))
	require.Equal(t, []string{"user.a", "user.b"}, names(opened.Xattrs))
}

type metaEvent struct {
	op, path, name string
}

// extWriteFS is the local disk, recording the owners, modes, extended
// attributes and flags given instead of, or as well as, setting them.
type extWriteFS struct {
	LocalFS

	mtx    sync.Mutex
	events []metaEvent
	refuse string
}

func (e *extWriteFS) record(op, path, name string) {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	e.events = append(e.events, metaEvent{op, filepath.Base(path), name})
}

func (e *extWriteFS) Lchown(_ context.Context, path string, _, _ int) error {
	e.record("chown", path, "")
	return nil
}

func (e *extWriteFS) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	e.record("chmod", path, "")
	return e.LocalFS.Chmod(ctx, path, mode)
}

func (e *extWriteFS) SetXattr(_ context.Context, path, name string, _ []byte) error {
	if name == e.refuse {
		return os.ErrPermission
	}

	e.record("xattr", path, name)

	return nil
}

func (e *extWriteFS) SetFlags(_ context.Context, path string, linux, _ uint32) error {
	if linux == 0 {
		return errors.ErrUnsupported
	}

	e.record("flags", path, "")

	return nil
}

func TestExtendedAttributesAreSetAfterTheOwnerAndBeforeTheModeAndFlagsLast(t *testing.T) {
	store := newMemStore()
	data := randomData(100, 1)
	ref := putFile(t, store, nil, data)
	value := putFile(t, store, nil, []byte("cap"))

	fsys := &extWriteFS{refuse: "trusted.x"}
	r := &Restorer{Store: store, FS: fsys, Owner: Owner{Recorded: true}, accounts: testAccounts}
	path := filepath.Join(t.TempDir(), "ping")

	stat := statFor(data)
	stat.User, stat.Group = []byte("alice"), []byte("staff")
	stat.Xattrs = []*proto.Stream{{Name: []byte("security.capability"), Ref: value, Size: 3}, {Name: []byte("trusted.x"), Ref: value, Size: 3}}
	stat.LinuxFlags = 0x10

	_, err := r.RestoreFile(context.Background(), path, stat, ref)
	require.NoError(t, err)
	requireRestored(t, path, data, stat)

	partial := filepath.Base(PartialPath(path))
	require.Equal(t, []metaEvent{
		{"chown", partial, ""},
		{"xattr", partial, "security.capability"},
		{"chmod", partial, ""},
		{"flags", "ping", ""},
	}, fsys.events)
	require.EqualValues(t, 1, r.Stats().XattrsLeftOut)

	stat.LinuxFlags, stat.DarwinFlags = 0, 0x2
	_, err = r.RestoreFile(context.Background(), filepath.Join(filepath.Dir(path), "other"), stat, ref)
	require.NoError(t, err)
	require.EqualValues(t, 1, r.Stats().FlagsLeftOut, "another platform's flags")
}

func TestATreeThatTakesNoExtendedAttributesCountsThemLeftOut(t *testing.T) {
	b := newTreeBuilder(t)
	value := putFile(t, b.store, nil, []byte("v"))

	file := b.file("a", "content", 0o644)
	file.Stat.Xattrs = []*proto.Stream{{Name: []byte("user.a"), Ref: value, Size: 1}, {Name: []byte("user.b"), Ref: value, Size: 1}}
	file.Stat.LinuxFlags, file.Stat.DarwinFlags = 0x40, 0x1

	r := &Restorer{Store: b.store, FS: plainWriteFS{LocalFS{}}}
	require.NoError(t, r.RestoreTree(context.Background(), b.commit(file), t.TempDir(), TreeOptions{}))
	require.EqualValues(t, 2, r.Stats().XattrsLeftOut)
	require.EqualValues(t, 1, r.Stats().FlagsLeftOut)
}

// plainWriteFS is the local disk as a WriteFS and nothing more.
type plainWriteFS struct {
	WriteFS
}
