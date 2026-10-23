//go:build !windows

package backup

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestARecordedOwnerIsSetOnTheLocalDiskAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("setting another owner needs root on Unix")
	}

	store := newMemStore()
	data := randomData(100, 1)
	ref := putFile(t, store, nil, data)
	path := filepath.Join(t.TempDir(), "a")

	r := &Restorer{Store: store, Owner: Owner{Recorded: true}, accounts: accounts{
		user:  func(string) (*user.User, error) { return &user.User{Uid: "4242", Gid: "4343"}, nil },
		group: func(string) (*user.Group, error) { return &user.Group{Gid: "4444"}, nil },
	}}

	stat := statFor(data)
	stat.User, stat.Group = []byte("someone"), []byte("somegroup")

	_, err := r.RestoreFile(context.Background(), path, stat, ref)
	require.NoError(t, err)

	uid, gid := ownerIDs(t, path)
	require.Equal(t, 4242, uid)
	require.Equal(t, 4444, gid)
}

func ownerIDs(t *testing.T, path string) (int, int) {
	t.Helper()

	info, err := os.Lstat(path)
	require.NoError(t, err)

	stat := info.Sys().(*syscall.Stat_t)

	return int(stat.Uid), int(stat.Gid)
}
