package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retentionIndex is a memIndex that records retention calls and answers
// them with a configured error.
type retentionIndex struct {
	*memIndex

	calls []string
	fail  error
	agent string
}

func (r *retentionIndex) record(ctx context.Context, call string) error {
	p, err := auth.Require(ctx)
	if err != nil {
		return err
	}

	r.agent = p.AgentID
	r.calls = append(r.calls, call)

	return r.fail
}

func (r *retentionIndex) DeleteCommit(ctx context.Context, ref *proto.Ref) error {
	return r.record(ctx, "delete")
}

func (r *retentionIndex) UndeleteCommit(ctx context.Context, ref *proto.Ref) error {
	return r.record(ctx, "undelete")
}

func (r *retentionIndex) TrashedCommits(ctx context.Context, set string, before time.Time, limit int) ([]*proto.TrashedCommit, error) {
	err := r.record(ctx, fmt.Sprintf("trash:%s:%d:%d", set, before.UnixNano(), limit))
	if err != nil {
		return nil, err
	}

	return []*proto.TrashedCommit{{Ref: &proto.Ref{Hash: testHash("commit")}, DeletedAtNs: 3, ExpiresAtNs: 9}}, nil
}

func (r *retentionIndex) CountCommits(ctx context.Context, set string, period proto.Period, from, to time.Time, zone string, deleted bool) ([]*proto.CommitCount, error) {
	err := r.record(ctx, fmt.Sprintf("count:%s:%v:%d:%d:%s:%v", set, period, from.UnixNano(), to.UnixNano(), zone, deleted))
	if err != nil {
		return nil, err
	}

	return []*proto.CommitCount{{StartNs: 4, Count: 2}}, nil
}

func (r *retentionIndex) DeleteSet(ctx context.Context, set string, erase bool) error {
	return r.record(ctx, "delete-set:"+set)
}

func (r *retentionIndex) UndeleteSet(ctx context.Context, set string) error {
	return r.record(ctx, "undelete-set:"+set)
}

func (r *retentionIndex) Unpin(ctx context.Context, pin *proto.Ref) error {
	return r.record(ctx, "unpin")
}

func (r *retentionIndex) Pins(ctx context.Context) ([]*proto.PinInfo, error) {
	err := r.record(ctx, "pins")
	if err != nil {
		return nil, err
	}

	return []*proto.PinInfo{{Ref: &proto.Ref{Hash: testHash("pin")}, Target: &proto.Ref{Hash: testHash("commit")}, ReceivedAtNs: 7}}, nil
}

func TestRemoteRetentionCallsReachTheIndex(t *testing.T) {
	index := &retentionIndex{memIndex: newMemIndex()}
	dial := startServerWith(t, index, nil)
	ctx := context.Background()
	client := dial("node-1")

	ref := &proto.Ref{Hash: testHash("commit")}

	require.NoError(t, client.DeleteCommit(ctx, ref))
	require.NoError(t, client.UndeleteCommit(ctx, ref))
	trash, err := client.TrashedCommits(ctx, "world", time.Unix(0, 5), 2)
	require.NoError(t, err)
	require.Len(t, trash, 1)
	require.True(t, trash[0].Ref.Equal(ref))
	require.EqualValues(t, 9, trash[0].ExpiresAtNs)
	counts, err := client.CountCommits(ctx, "world", proto.Period_PERIOD_DAY, time.Unix(0, 5), time.Unix(0, 8), "Europe/Berlin", true)
	require.NoError(t, err)
	require.Len(t, counts, 1)
	require.EqualValues(t, 2, counts[0].Count)
	require.NoError(t, client.DeleteSet(ctx, "world", true))
	require.NoError(t, client.UndeleteSet(ctx, "world"))
	require.NoError(t, client.Unpin(ctx, ref))

	pins, err := client.Pins(ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.True(t, pins[0].Target.Equal(ref))

	require.Equal(t, []string{"delete", "undelete", "trash:world:5:2", "count:world:PERIOD_DAY:5:8:Europe/Berlin:true", "delete-set:world", "undelete-set:world", "unpin", "pins"}, index.calls)
	require.Equal(t, "node-1", index.agent, "the caller's principal reaches the index")

	index.fail = backup.ErrNewestCommit
	require.Equal(t, codes.FailedPrecondition, status.Code(client.DeleteCommit(ctx, ref)))

	index.fail = backup.ErrTombstoned
	require.Equal(t, codes.FailedPrecondition, status.Code(client.UndeleteCommit(ctx, ref)))

	index.fail = backup.ErrNotFound
	require.Equal(t, codes.NotFound, status.Code(client.Unpin(ctx, ref)))

	index.fail = backup.ErrInvalidCount
	_, err = client.CountCommits(ctx, "world", proto.Period_PERIOD_HOUR, time.Unix(0, 5), time.Unix(0, 8), "Mars/Olympus", false)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	index.fail = auth.ErrForbidden
	require.Equal(t, codes.PermissionDenied, status.Code(client.DeleteSet(ctx, "logs", false)))
}

func TestRemoteRetentionUnimplementedWithoutState(t *testing.T) {
	_, dial := startServer(t)
	ctx := context.Background()
	client := dial("node-1")

	require.Equal(t, codes.Unimplemented, status.Code(client.DeleteCommit(ctx, &proto.Ref{Hash: testHash("commit")})))

	_, err := client.Pins(ctx)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = client.GetPolicy(ctx, "world")
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = client.SetPolicy(ctx, "world", nil)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
