//go:build !windows

package vss

import (
	"context"

	"github.com/gobackio/goback/backup"
)

// Snapshot is a set of shadow copies taken together.
type Snapshot struct{}

// Take returns ErrUnsupported off Windows.
func Take(context.Context, ...string) (*Snapshot, error) {
	return nil, ErrUnsupported
}

// FS is nil off Windows.
func (*Snapshot) FS() backup.FS {
	return nil
}

// Release does nothing off Windows.
func (*Snapshot) Release() error {
	return nil
}
