package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const noDump = 0x40

func setNoDump(t *testing.T, path string) {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	flags, err := unix.IoctlGetUint32(int(file.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil {
		t.Skipf("the filesystem keeps no flags: %v", err)
	}

	require.NoError(t, unix.IoctlSetPointerInt(int(file.Fd()), unix.FS_IOC_SETFLAGS, int(flags|noDump)))
}

func flagsAt(t *testing.T, path string) uint32 {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	flags, err := unix.IoctlGetUint32(int(file.Fd()), unix.FS_IOC_GETFLAGS)
	require.NoError(t, err)

	return flags
}

func TestExtendedAttributesAndFlagsOnLinuxComeBackWithARestore(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("dir/a", []byte("content"))

	file, dir := filepath.Join(f.root, "dir", "a"), filepath.Join(f.root, "dir")
	if err := unix.Setxattr(file, "user.goback", []byte("value"), 0); errors.Is(err, unix.ENOTSUP) {
		t.Skip("the filesystem holds no user extended attributes")
	} else {
		require.NoError(t, err)
	}

	require.NoError(t, unix.Setxattr(dir, "user.empty", []byte{}, 0))
	setNoDump(t, file)

	result := f.run()

	node := f.tree(f.tree(result.Commit.Tree)["dir"].Ref)["a"]
	require.Equal(t, "value", f.xattr(node.Stat, "user.goback"))
	require.EqualValues(t, noDump, node.Stat.LinuxFlags)

	dst := t.TempDir()
	r := &Restorer{Store: f.store}
	require.NoError(t, r.RestoreTree(context.Background(), result.Commit, dst, TreeOptions{}))
	require.Zero(t, r.Stats().XattrsLeftOut)
	require.Zero(t, r.Stats().FlagsLeftOut)

	value := make([]byte, 16)
	n, err := unix.Getxattr(filepath.Join(dst, "dir", "a"), "user.goback", value)
	require.NoError(t, err)
	require.Equal(t, "value", string(value[:n]))

	n, err = unix.Getxattr(filepath.Join(dst, "dir"), "user.empty", value)
	require.NoError(t, err)
	require.Zero(t, n)

	require.NotZero(t, flagsAt(t, filepath.Join(dst, "dir", "a"))&noDump)
}
