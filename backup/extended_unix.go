//go:build linux || darwin

package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	_ ExtendedFS      = LocalFS{}
	_ ExtendedWriteFS = LocalFS{}
	_ ExtendedWriteFS = (*rootedFS)(nil)
)

// openFlags open an entry only to read or set what it records beyond its
// content, without waiting on a FIFO there.
const openFlags = os.O_RDONLY | syscall.O_NONBLOCK | syscall.O_NOCTTY

// Extended implements ExtendedFS.
func (LocalFS) Extended(_ context.Context, path string) (Extended, error) {
	file, err := os.OpenFile(path, openFlags|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return Extended{}, err
	}
	defer file.Close()

	return readExtended(file)
}

// extendedIn is Extended for the entry at rel in root.
func extendedIn(root *os.Root, rel string) (Extended, error) {
	if rel == "" {
		rel = "."
	}

	file, err := root.OpenFile(rel, openFlags, 0)
	if err != nil {
		return Extended{}, err
	}
	defer file.Close()

	return readExtended(file)
}

func readExtended(file *os.File) (Extended, error) {
	info, err := file.Stat()
	if err != nil {
		return Extended{}, err
	}

	if !info.Mode().IsRegular() && !info.IsDir() {
		return Extended{}, fmt.Errorf("%w: %s is no longer a file or directory", errVanished, file.Name())
	}

	fd := int(file.Fd())

	var ext Extended
	if ext.LinuxFlags, ext.DarwinFlags, err = flagsOf(fd, info); err != nil {
		return Extended{}, fmt.Errorf("reading the flags of %s: %w", file.Name(), err)
	}

	if ext.Xattrs, err = xattrsOf(fd); err != nil {
		return Extended{}, fmt.Errorf("reading the extended attributes of %s: %w", file.Name(), err)
	}

	return ext, nil
}

// xattrsOf reads the extended attributes of the entry open as fd, none
// where its filesystem holds none.
func xattrsOf(fd int) (map[string][]byte, error) {
	list, err := sized(func(buf []byte) (int, error) { return unix.Flistxattr(fd, buf) })
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var xattrs map[string][]byte

	for _, name := range strings.Split(string(list), "\x00") {
		if name == "" {
			continue
		}

		value, err := sized(func(buf []byte) (int, error) { return unix.Fgetxattr(fd, name, buf) })
		if errors.Is(err, xattrGone) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}

		if xattrs == nil {
			xattrs = map[string][]byte{}
		}

		xattrs[name] = value
	}

	return xattrs, nil
}

// sized calls read once to learn the size of what it reads and again to
// read it, until what it reads has not grown in between.
func sized(read func(buf []byte) (int, error)) ([]byte, error) {
	for {
		size, err := read(nil)
		if err != nil || size == 0 {
			return []byte{}, err
		}

		buf := make([]byte, size)

		n, err := read(buf)
		if errors.Is(err, unix.ERANGE) {
			continue
		}

		if err != nil {
			return nil, err
		}

		return buf[:n], nil
	}
}

// SetXattr implements ExtendedWriteFS.
func (LocalFS) SetXattr(_ context.Context, path, name string, value []byte) error {
	return withOpen(func() (*os.File, error) { return os.OpenFile(path, openFlags|syscall.O_NOFOLLOW, 0) }, func(fd int) error {
		return unix.Fsetxattr(fd, name, value, 0)
	})
}

// SetFlags implements ExtendedWriteFS.
func (LocalFS) SetFlags(_ context.Context, path string, linux, darwin uint32) error {
	return setFlagsWith(func() (*os.File, error) { return os.OpenFile(path, openFlags|syscall.O_NOFOLLOW, 0) }, linux, darwin)
}

// SetXattr implements ExtendedWriteFS.
func (l *rootedFS) SetXattr(ctx context.Context, path, name string, value []byte) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.SetXattr(ctx, path, name, value)
	}

	return named(withOpen(func() (*os.File, error) { return root.OpenFile(rel, openFlags, 0) }, func(fd int) error {
		return unix.Fsetxattr(fd, name, value, 0)
	}), path)
}

// SetFlags implements ExtendedWriteFS.
func (l *rootedFS) SetFlags(ctx context.Context, path string, linux, darwin uint32) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.SetFlags(ctx, path, linux, darwin)
	}

	return named(setFlagsWith(func() (*os.File, error) { return root.OpenFile(rel, openFlags, 0) }, linux, darwin), path)
}

func setFlagsWith(open func() (*os.File, error), linux, darwin uint32) error {
	flags, err := platformFlags(linux, darwin)
	if err != nil {
		return err
	}

	return withOpen(open, func(fd int) error { return setFlags(fd, flags) })
}

func withOpen(open func() (*os.File, error), do func(fd int) error) error {
	file, err := open()
	if err != nil {
		return err
	}
	defer file.Close()

	return do(int(file.Fd()))
}
