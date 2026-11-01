package sql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestNewSetsTakeTheIDsWithNewSetIDGives(t *testing.T) {
	f := newFixture(t)

	var asked []string
	f.x.newSetID = func(_ context.Context, name string) (int64, error) {
		asked = append(asked, name)

		return 7000 + int64(len(asked)), nil
	}

	_, err := f.x.BeginCommit(f.ctx, "world")
	require.NoError(t, err)
	require.NoError(t, f.commitIn(f.ctx, "logs"))
	base := retention.DatabaseBase
	require.NoError(t, f.commitIn(f.session("db/base", &base), "db/base"))
	require.NoError(t, f.commitIn(f.ctx, "world"))

	f.advance(time.Second)
	restored := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("r.txt", "r")).Ref(), BackupSet: "restored",
		AgentId: "node-1", SetId: 4200, ReceivedAtNs: f.clock.UnixNano()})
	require.NoError(t, f.store.Put(f.ctx, restored))
	_, err = f.x.IndexCommits(f.ctx, []*proto.Ref{restored.Ref()}, false)
	require.NoError(t, err)

	require.Equal(t, []string{"world", "logs", "db/base"}, asked, "each new set asks once, a set under its carried id never")

	for name, id := range map[string]int64{"world": 7001, "logs": 7002, "db/base": 7003, "restored": 4200} {
		found, err := findSet(f.ctx, f.x.client, name)
		require.NoError(t, err)
		require.Equal(t, id, found, name)
	}
}

func TestAnErrorFromNewSetIDRefusesTheWriteThatNeededTheSet(t *testing.T) {
	f := newFixture(t)

	elsewhere := fmt.Errorf("%w: the set is homed at another store", backup.ErrCommitDenied)
	f.x.newSetID = func(context.Context, string) (int64, error) { return 0, elsewhere }

	_, err := f.x.BeginCommit(f.ctx, "world")
	require.ErrorIs(t, err, elsewhere)
	require.ErrorIs(t, f.commitIn(f.ctx, "logs"), elsewhere)

	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Empty(t, sets)
}
