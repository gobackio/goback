package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/backup/schedule"

	"github.com/stretchr/testify/require"
)

var (
	daily = schedule.Expectation{Set: "world", Kind: schedule.Every, LongestGap: 24 * time.Hour, Jitter: 30 * time.Minute}
	wal   = schedule.Expectation{Set: "db/wal", Kind: schedule.Receiver, CommitEvery: time.Minute}
)

func (f *fixture) report(agent string, sets ...schedule.Expectation) {
	f.t.Helper()
	require.NoError(f.t, f.x.ReportSchedule(f.ctx, schedule.Report{Agent: agent, Sets: sets}))
}

func (f *fixture) reported(name string) *schedule.Reported {
	f.t.Helper()

	set, err := f.x.GetSet(f.ctx, name)
	require.NoError(f.t, err)

	return set.Schedule
}

func TestAReportRecordsWhatTheAgentExpectsOfEachSet(t *testing.T) {
	f := newFixture(t)
	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	at := f.clock

	f.report("node-1", daily, wal)

	require.Equal(t, &schedule.Reported{Expectation: daily, Agent: "node-1", Since: at, At: at}, f.reported("world"))

	f.commit("db/wal", f.tree(f.file("0001", "wal")), false)
	require.Equal(t, &schedule.Reported{Expectation: wal, Agent: "node-1", Since: at, At: at}, f.reported("db/wal"), "a set reported before its first commit")
}

func TestAReportAgainKeepsSinceWhenItsAgentFirstReportedTheSet(t *testing.T) {
	f := newFixture(t)
	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	first := f.clock

	f.report("node-1", daily)
	f.advance(time.Hour)

	hourly := daily
	hourly.LongestGap = time.Hour
	f.report("node-1", hourly)

	require.Equal(t, &schedule.Reported{Expectation: hourly, Agent: "node-1", Since: first, At: f.clock}, f.reported("world"))

	f.advance(time.Hour)
	f.report("node-2", hourly)
	require.Equal(t, &schedule.Reported{Expectation: hourly, Agent: "node-2", Since: f.clock, At: f.clock}, f.reported("world"), "another agent reports the set from now")
}

func TestAReportDropsTheSetsItsAgentNoLongerLists(t *testing.T) {
	f := newFixture(t)
	for _, set := range []string{"world", "logs", "other"} {
		f.commit(set, f.tree(f.file("a.txt", set)), false)
	}

	logs := daily
	logs.Set = "logs"
	other := daily
	other.Set = "other"

	f.report("node-1", daily, logs)
	f.report("node-2", other)
	f.report("node-1", daily)

	require.NotNil(t, f.reported("world"))
	require.Nil(t, f.reported("logs"))
	require.NotNil(t, f.reported("other"), "another agent's sets stay")

	f.report("node-1")
	require.Nil(t, f.reported("world"))
}

func TestASetNoAgentReportedForThirtyDaysExpectsNothing(t *testing.T) {
	f := newFixture(t)
	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.commit("other", f.tree(f.file("a.txt", "two")), false)

	other := daily
	other.Set = "other"

	f.report("node-2", other)
	f.advance(schedule.StaleAfter + time.Second)
	require.Nil(t, f.reported("other"))

	f.report("node-1", daily)
	n, err := f.x.client.Schedule.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "a report drops what went stale")
}

func TestASetIsLateByItsReportedScheduleAndNewestCommit(t *testing.T) {
	f := newFixture(t)
	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	newest := f.clock

	f.report("node-1", daily)

	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	require.Equal(t, newest, sets[0].NewestAt)
	require.False(t, sets[0].Late(f.clock))
	require.True(t, sets[0].Late(f.clock.Add(37*time.Hour)))

	require.NoError(t, f.x.DeleteSet(f.ctx, "world", false))
	set, err := f.x.GetSet(f.ctx, "world")
	require.NoError(t, err)
	require.False(t, set.Late(f.clock.Add(37*time.Hour)), "a closed set is never late")
}
