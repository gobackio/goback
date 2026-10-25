package goback

import (
	"context"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestTheBackupKeyReportsTheAgentsScheduleWhichTheServerRecordsAndDrops(t *testing.T) {
	ctx := context.Background()
	addr, repo := servedRepo(t, "admin")
	agent, _ := clientWith(t, addr, "backup")

	for _, set := range []string{"world", "logs"} {
		sctx, err := agent.BeginSession(ctx, &backup.Session{Set: set})
		require.NoError(t, err)

		a, aNode := fileNamed("a.txt", set)
		tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode}})
		require.NoError(t, agent.PutObjects(sctx, []*proto.Object{a, tree}))
		require.NoError(t, agent.PutCommits(sctx, []*proto.Object{snapshotOf(set, 1, tree)}))
	}

	world := schedule.Expectation{Set: "world", Kind: schedule.Every, LongestGap: time.Hour}
	logs := schedule.Expectation{Set: "logs", Kind: schedule.Cron, LongestGap: 72 * time.Hour, Jitter: time.Minute}

	expected := func(name string) *schedule.Reported {
		t.Helper()

		set, err := repo.Index.(*sql.Index).GetSet(ctx, name)
		require.NoError(t, err)

		return set.Schedule
	}

	require.NoError(t, agent.ReportSchedule(ctx, schedule.Report{Agent: "agent", Sets: []schedule.Expectation{world, logs}}))
	require.Equal(t, world, expected("world").Expectation)
	require.Equal(t, "agent", expected("world").Agent)
	require.Equal(t, logs, expected("logs").Expectation)

	require.NoError(t, agent.ReportSchedule(ctx, schedule.Report{Agent: "agent", Sets: []schedule.Expectation{world}}))
	require.NotNil(t, expected("world"))
	require.Nil(t, expected("logs"), "a set the agent's report no longer lists expects nothing")
}
