//go:build !windows

package backup

import (
	"context"
	"os"
)

var _ OwnerWriteFS = LocalFS{}

// Lchown implements OwnerWriteFS.
func (LocalFS) Lchown(_ context.Context, path string, uid, gid int) error {
	return os.Lchown(path, uid, gid)
}
