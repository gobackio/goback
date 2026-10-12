package backup_test

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

type pickIndex struct {
	backup.Index
	objects map[string]*proto.Object
	newest  *proto.Commit
	asked   time.Time
}

func (x *pickIndex) Get(_ context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, ok := x.objects[string(ref.Hash)]
	if !ok {
		return nil, backup.ErrNotFound
	}

	return obj, nil
}

func (x *pickIndex) CommitInfo(_ context.Context, _ string, notAfter time.Time, _ int) ([]*proto.Commit, error) {
	x.asked = notAfter
	if x.newest == nil {
		return nil, nil
	}

	return []*proto.Commit{x.newest}, nil
}

func TestPickByRefChecksTheSet(t *testing.T) {
	tree := &proto.Ref{Hash: make([]byte, proto.HashSize)}
	ours := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree, BackupSet: "world"})
	theirs := proto.NewObject(&proto.Commit{Timestamp: 2, Tree: tree, BackupSet: "other"})
	newest := &proto.Commit{Timestamp: 3, Tree: tree, BackupSet: "world"}
	index := &pickIndex{
		objects: map[string]*proto.Object{string(ours.Ref().Hash): ours, string(theirs.Ref().Hash): theirs},
		newest:  newest,
	}
	ctx := context.Background()

	picked, err := backup.PickCommit(ctx, index, "world", ours.Ref(), time.Time{})
	require.NoError(t, err)
	require.Same(t, ours.GetCommit(), picked, "a ref wins over the newest commit")

	_, err = backup.PickCommit(ctx, index, "world", theirs.Ref(), time.Time{})
	require.Error(t, err)

	_, err = backup.PickCommit(ctx, index, "world", proto.NewObject(&proto.Commit{Timestamp: 4, Tree: tree, BackupSet: "world"}).Ref(), time.Time{})
	require.Error(t, err)

	at := time.Unix(1700000000, 0)
	picked, err = backup.PickCommit(ctx, index, "world", nil, at)
	require.NoError(t, err)
	require.Same(t, newest, picked)
	require.Equal(t, at, index.asked)

	index.newest = nil
	_, err = backup.PickCommit(ctx, index, "world", nil, at)
	require.Error(t, err)
}
