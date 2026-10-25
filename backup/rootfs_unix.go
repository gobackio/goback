//go:build !windows

package backup

import (
	"context"
	"os"
)

var _ OwnerWriteFS = (*rootedFS)(nil)

// Lchown implements OwnerWriteFS.
func (l *rootedFS) Lchown(ctx context.Context, path string, uid, gid int) error {
	root, rel := l.resolve(path)
	if root == nil {
		return LocalFS{}.Lchown(ctx, path, uid, gid)
	}

	return named(root.Lchown(rel, uid, gid), path)
}

func symlinkIn(root *os.Root, target, rel, path string) error {
	return named(root.Symlink(target, rel), path)
}
