package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/gobackio/goback/proto"
)

// RestorePath restores to dest the version of the slash-separated file
// path that set held at notAfter, as the index records it, making dest's
// directory and holding the file while it is read. It returns the version
// it restored; a path the set never held at notAfter fails with
// ErrNotFound.
func (r *Restorer) RestorePath(ctx context.Context, idx Index, set, path string, notAfter time.Time, dest string) (*proto.TreeNode, Outcome, error) {
	found, err := FindFile(ctx, idx, r.Key, set, path, notAfter, 1)
	if err != nil {
		return nil, 0, err
	}

	if len(found) == 0 {
		return nil, 0, fmt.Errorf("%s in set %s at %s: %w", path, set, notAfter.Format(time.RFC3339), ErrNotFound)
	}

	node := found[0]

	ctx, end, err := HoldRestore(ctx, r.Store, set, node.Ref)
	if err != nil {
		return nil, 0, err
	}
	defer end()

	if !r.DryRun {
		dir, _ := r.fs().Split(dest)
		if err := r.fs().MkdirAll(ctx, dir); err != nil {
			return nil, 0, err
		}
	}

	outcome, err := r.RestoreFile(ctx, dest, node.Stat, node.Ref)

	return node, outcome, err
}
