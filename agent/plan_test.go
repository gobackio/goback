package agent

import (
	"context"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func berlin(t *testing.T) *time.Location {
	t.Helper()

	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	return loc
}

func cronPlan(t *testing.T, expr string, loc *time.Location) *Plan {
	t.Helper()

	p, err := Cron(expr, loc)
	require.NoError(t, err)

	return p
}

func TestEveryRunsAnIntervalAfterTheLastStart(t *testing.T) {
	last := time.Date(2026, 10, 11, 9, 0, 7, 0, time.UTC)

	require.Equal(t, last.Add(6*time.Hour), Every(6*time.Hour).due(0, last, last.Add(time.Hour)))
}

func TestCronRunsAtTheNextSlotInItsZone(t *testing.T) {
	loc := berlin(t)
	p := cronPlan(t, "0 3 * * *", loc)

	last := time.Date(2026, 10, 11, 3, 0, 2, 0, loc)
	require.Equal(t, time.Date(2026, 10, 12, 3, 0, 0, 0, loc), p.due(0, last, last.Add(time.Hour)))
}

func TestOverdueRunsOnceNow(t *testing.T) {
	loc := berlin(t)
	daily := cronPlan(t, "0 3 * * *", loc)
	last := time.Date(2026, 10, 11, 3, 0, 0, 0, loc)

	for _, missed := range []int{1, 100} {
		now := last.AddDate(0, 0, missed).Add(5 * time.Hour)
		require.Equal(t, now, daily.due(0, last, now))

		now = last.Add(time.Duration(missed)*time.Minute + time.Second)
		require.Equal(t, now, Every(time.Minute).due(0, last, now))
	}
}

func TestANewSetIsDueNow(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, now, cronPlan(t, "0 3 * * *", time.UTC).due(time.Hour, time.Time{}, now))
	require.Equal(t, now, Every(time.Hour).due(0, time.Time{}, now))
}

func TestALastStartAheadOfTheClockCountsAsNow(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, now.Add(time.Hour), Every(time.Hour).due(0, now.Add(5*time.Hour), now))
}

func TestJitterIsStableAndInsideTheWindow(t *testing.T) {
	window := 30 * time.Minute
	offsets := map[time.Duration]bool{}

	for _, agent := range []string{"a", "b", "c", "d", "e", "f"} {
		offset := jitterOffset(agent, "world", window)
		require.Equal(t, offset, jitterOffset(agent, "world", window))
		require.GreaterOrEqual(t, offset, time.Duration(0))
		require.Less(t, offset, window)

		offsets[offset] = true
	}

	require.Greater(t, len(offsets), 1)
	require.Zero(t, jitterOffset("a", "world", 0))
}

func TestJitterMovesEverySlotByItsOffset(t *testing.T) {
	loc := berlin(t)
	p := cronPlan(t, "0 3 * * *", loc)

	offset := 7 * time.Minute
	last := time.Date(2026, 10, 11, 3, 7, 1, 0, loc)

	require.Equal(t, time.Date(2026, 10, 12, 3, 7, 0, 0, loc), p.due(offset, last, last.Add(time.Hour)))
}

func TestCronAcrossDST(t *testing.T) {
	loc := berlin(t)
	p := cronPlan(t, "30 2 * * *", loc)

	// spring forward: 2026-03-29 02:00 CET becomes 03:00 CEST
	before := time.Date(2026, 3, 28, 2, 30, 0, 0, loc)
	skipped := p.next(before)
	require.Equal(t, 29, skipped.Day())
	require.Equal(t, 3, skipped.Hour())
	require.Equal(t, time.Date(2026, 3, 30, 2, 30, 0, 0, loc), p.next(skipped))

	// fall back: 2026-10-25 03:00 CEST becomes 02:00 CET
	before = time.Date(2026, 10, 24, 2, 30, 0, 0, loc)
	first := p.next(before)
	_, offset := first.Zone()
	require.Equal(t, 2*60*60, offset)
	require.Equal(t, 2, first.Hour())
	require.Equal(t, time.Date(2026, 10, 26, 2, 30, 0, 0, loc), p.next(first))
}

func TestEveryAcrossDSTIsElapsedTime(t *testing.T) {
	loc := berlin(t)
	last := time.Date(2026, 10, 25, 2, 30, 0, 0, loc)

	require.Equal(t, time.Hour, Every(time.Hour).next(last).Sub(last))
}

func TestStaleAfterIsThreeIntervals(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, 3*time.Minute, Every(time.Minute).staleAfter(now))
	require.Equal(t, 72*time.Hour, cronPlan(t, "0 3 * * *", time.UTC).staleAfter(now))
}

type commitsIndex struct {
	backup.Index
	commits []*proto.Commit
}

func (c *commitsIndex) CommitInfo(context.Context, string, time.Time, int) ([]*proto.Commit, error) {
	return c.commits, nil
}

func TestLastRunIsTheScanStart(t *testing.T) {
	start := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)
	now := start.Add(time.Hour)

	last, commit, err := LastRun(context.Background(), &commitsIndex{}, "s", now)
	require.NoError(t, err)
	require.Nil(t, commit)
	require.True(t, last.IsZero())

	last, _, err = LastRun(context.Background(), &commitsIndex{commits: []*proto.Commit{{ScanStartNs: start.UnixNano(), Timestamp: now.Unix()}}}, "s", now)
	require.NoError(t, err)
	require.True(t, start.Equal(last))

	last, _, err = LastRun(context.Background(), &commitsIndex{commits: []*proto.Commit{{Timestamp: start.Unix()}}}, "s", now)
	require.NoError(t, err)
	require.True(t, start.Equal(last))
}
