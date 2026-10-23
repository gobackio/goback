package backup

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type chowned struct {
	path     string
	uid, gid int
}

// chownFS is the local disk, recording the owners given instead of
// setting them.
type chownFS struct {
	LocalFS

	mtx   sync.Mutex
	calls []chowned
}

func (c *chownFS) Lchown(_ context.Context, path string, uid, gid int) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.calls = append(c.calls, chowned{path, uid, gid})

	return nil
}

var testAccounts = accounts{
	user: func(name string) (*user.User, error) {
		switch name {
		case "alice":
			return &user.User{Username: name, Uid: "1001", Gid: "100"}, nil
		case "bob":
			return &user.User{Username: name, Uid: "1002", Gid: "100"}, nil
		}

		return nil, user.UnknownUserError(name)
	},
	group: func(name string) (*user.Group, error) {
		if name == "staff" {
			return &user.Group{Name: name, Gid: "50"}, nil
		}

		return nil, user.UnknownGroupError(name)
	},
}

func TestParseOwner(t *testing.T) {
	for s, want := range map[string]Owner{
		"none":         {},
		"recorded":     {Recorded: true},
		"alice":        {User: "alice"},
		"alice:staff":  {User: "alice", Group: "staff"},
		"alice:":       {User: "alice"},
		"minecraft:mc": {User: "minecraft", Group: "mc"},
	} {
		got, err := ParseOwner(s)
		require.NoError(t, err, s)
		require.Equal(t, want, got, s)
	}

	for _, s := range []string{"", ":staff"} {
		_, err := ParseOwner(s)
		require.Error(t, err, s)
	}
}

func TestARecordedOwnerIsGivenByNameBeforeTheRename(t *testing.T) {
	store := newMemStore()
	data := randomData(2*maxBlobSize+3, 1)
	ref := putFile(t, store, nil, data)

	fsys := &chownFS{}
	r := &Restorer{Store: store, FS: fsys, Owner: Owner{Recorded: true}, accounts: testAccounts}
	dir := t.TempDir()

	known := filepath.Join(dir, "known.dat")
	stat := statFor(data)
	stat.User, stat.Group = []byte("alice"), []byte("staff")

	_, err := r.RestoreFile(context.Background(), known, stat, ref)
	require.NoError(t, err)
	requireRestored(t, known, data, stat)
	require.Equal(t, []chowned{{PartialPath(known), 1001, 50}}, fsys.calls)

	for name, owner := range map[string][2]string{"ghost.dat": {"ghost", "staff"}, "nogroup.dat": {"alice", "ghosts"}, "unnamed.dat": {"", ""}} {
		stat := statFor(data)
		stat.User, stat.Group = []byte(owner[0]), []byte(owner[1])

		_, err := r.RestoreFile(context.Background(), filepath.Join(dir, name), stat, ref)
		require.NoError(t, err)
		requireRestored(t, filepath.Join(dir, name), data, stat)
	}

	require.Len(t, fsys.calls, 1, "a name with no account here is never guessed")
	require.EqualValues(t, 1, r.Stats().Owned)
	require.EqualValues(t, 3, r.Stats().OwnersLeftOut)
}

func TestAnAccountOwnerIsLookedUpOnceUpFront(t *testing.T) {
	store := newMemStore()
	data := randomData(100, 1)
	ref := putFile(t, store, nil, data)
	dir := t.TempDir()

	fsys := &chownFS{}
	r := &Restorer{Store: store, FS: fsys, Owner: Owner{User: "bob"}, accounts: testAccounts}
	_, err := r.RestoreFile(context.Background(), filepath.Join(dir, "a"), statFor(data), ref)
	require.NoError(t, err)
	require.Equal(t, []chowned{{PartialPath(filepath.Join(dir, "a")), 1002, 100}}, fsys.calls, "the user's primary group")

	for _, owner := range []Owner{{User: "ghost"}, {User: "bob", Group: "ghosts"}} {
		r := &Restorer{Store: store, FS: &chownFS{}, Owner: owner, accounts: testAccounts}
		_, err := r.RestoreFile(context.Background(), filepath.Join(dir, "b"), statFor(data), ref)
		require.Error(t, err, owner)
		require.NoFileExists(t, filepath.Join(dir, "b"))
	}
}

func TestRestoreTreeGivesDirectoriesAndLinksTheirOwner(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("world/region/r.0.0.mca", f.random(2*maxBlobSize+1))
	f.write("server.properties", []byte("motd=hi"))

	linked := os.Symlink("server.properties", filepath.Join(f.root, "link")) == nil

	result, err := f.walker.Run(context.Background())
	require.NoError(t, err)

	dst := t.TempDir()
	fsys := &chownFS{}
	r := &Restorer{Store: filesStore{f.store}, FS: fsys, Owner: Owner{User: "alice", Group: "staff"}, accounts: testAccounts}
	require.NoError(t, r.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))

	var paths []string
	for _, c := range fsys.calls {
		require.Equal(t, 1001, c.uid)
		require.Equal(t, 50, c.gid)
		paths = append(paths, c.path)
	}

	want := []string{
		filepath.Join(dst, "world"),
		filepath.Join(dst, "world", "region"),
		PartialPath(filepath.Join(dst, "world", "region", "r.0.0.mca")),
		PartialPath(filepath.Join(dst, "server.properties")),
	}
	if linked {
		want = append(want, PartialPath(filepath.Join(dst, "link")))
	}

	require.ElementsMatch(t, want, paths)

	world := slices.Index(paths, filepath.Join(dst, "world"))
	region := slices.Index(paths, PartialPath(filepath.Join(dst, "world", "region", "r.0.0.mca")))
	require.Greater(t, world, region, "a directory is given its owner after what it holds")
}

func TestAnOwnerOnTheLocalDiskNeedsUnixAndRoot(t *testing.T) {
	store := newMemStore()
	data := randomData(100, 1)
	ref := putFile(t, store, nil, data)
	path := filepath.Join(t.TempDir(), "a")

	r := &Restorer{Store: store, Owner: Owner{Recorded: true}}
	_, err := r.RestoreFile(context.Background(), path, statFor(data), ref)

	switch {
	case runtime.GOOS == "windows":
		require.ErrorContains(t, err, "Unix")
	case os.Geteuid() != 0:
		require.ErrorContains(t, err, "root")
	default:
		require.NoError(t, err)
	}

	if err != nil {
		require.NoFileExists(t, path)
	}
}
