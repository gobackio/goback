package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gobackio/goback/proto"
)

// PickCommit is the commit of set that ref names, or, when ref is nil,
// the set's newest commit at or before notAfter.
func PickCommit(ctx context.Context, idx Index, set string, ref *proto.Ref, notAfter time.Time) (*proto.Commit, error) {
	if ref == nil {
		commits, err := idx.CommitInfo(ctx, set, notAfter, 1)
		if err != nil {
			return nil, err
		}

		if len(commits) != 1 {
			return nil, fmt.Errorf("set %s has no commit at or before %s", set, notAfter.Format(time.RFC3339))
		}

		return commits[0], nil
	}

	obj, err := idx.Get(ctx, ref)
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("set %s has no commit %x", set, ref.Hash)
	}

	if err != nil {
		return nil, err
	}

	commit := obj.GetCommit()
	if commit == nil {
		return nil, fmt.Errorf("%x is not a commit", ref.Hash)
	}

	if commit.GetBackupSet() != set {
		return nil, fmt.Errorf("commit %x belongs to set %s, not %s", ref.Hash, commit.GetBackupSet(), set)
	}

	return commit, nil
}
