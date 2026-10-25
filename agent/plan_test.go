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

func TestEveryRunsAtTheMultiplesOfItsIntervalSinceTheEpoch(t *testing.T) {
	last := time.Date(2026, 10, 11, 9, 0, 7, 0, time.UTC)

	require.Equal(t, time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC), Every(6*time.Hour).Due(0, last, last.Add(time.Hour)))
}

func TestJitterMovesTheSlotsOfAnInterval(t *testing.T) {
	for _, offset := range []time.Duration{0, 17 * time.Minute, 45 * time.Minute} {
		last := time.Date(2026, 10, 11, 12, 0, 3, 0, time.UTC).Add(offset)
		want := time.Date(2026, 10, 11, 18, 0, 0, 0, time.UTC).Add(offset)
		require.Equal(t, want, Every(6*time.Hour).Due(offset, last, last.Add(time.Hour)), offset)
	}
}

func TestANewOrOverdueTaskStartsItsOffsetAfterNow(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	offset := 40 * time.Minute

	require.Equal(t, now.Add(offset), Every(24*time.Hour).Due(offset, time.Time{}, now))
	require.Equal(t, now.Add(offset), Every(24*time.Hour).Due(offset, now.AddDate(0, 0, -3), now))
	require.Equal(t, now.Add(offset), cronPlan(t, "0 3 * * *", time.UTC).Due(offset, time.Time{}, now))

	require.Equal(t, time.Date(2026, 10, 11, 9, 10, 0, 0, time.UTC), Every(30*time.Minute).Due(offset, time.Time{}, now), "or at its next slot, sooner")
}

func TestCronRunsAtTheNextSlotInItsZone(t *testing.T) {
	loc := berlin(t)
	p := cronPlan(t, "0 3 * * *", loc)

	last := time.Date(2026, 10, 11, 3, 0, 2, 0, loc)
	require.Equal(t, time.Date(2026, 10, 12, 3, 0, 0, 0, loc), p.Due(0, last, last.Add(time.Hour)))
}

func TestOverdueRunsOnceNow(t *testing.T) {
	loc := berlin(t)
	daily := cronPlan(t, "0 3 * * *", loc)
	last := time.Date(2026, 10, 11, 3, 0, 0, 0, loc)

	for _, missed := range []int{1, 100} {
		now := last.AddDate(0, 0, missed).Add(5 * time.Hour)
		require.Equal(t, now, daily.Due(0, last, now))

		now = last.Add(time.Duration(missed)*time.Minute + time.Second)
		require.Equal(t, now, Every(time.Minute).Due(0, last, now))
	}
}

func TestANewSetWithoutJitterIsDueNow(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, now, cronPlan(t, "0 3 * * *", time.UTC).Due(0, time.Time{}, now))
	require.Equal(t, now, Every(time.Hour).Due(0, time.Time{}, now))
}

func TestALastStartAheadOfTheClockCountsAsNow(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, now.Add(time.Hour), Every(time.Hour).Due(0, now.Add(5*time.Hour), now))
}

func TestJitterIsStableAndInsideTheWindow(t *testing.T) {
	window := 30 * time.Minute
	offsets := map[time.Duration]bool{}

	for _, agent := range []string{"a", "b", "c", "d", "e", "f"} {
		offset := JitterOffset(agent, "world", window)
		require.Equal(t, offset, JitterOffset(agent, "world", window))
		require.GreaterOrEqual(t, offset, time.Duration(0))
		require.Less(t, offset, window)

		offsets[offset] = true
	}

	require.Greater(t, len(offsets), 1)
	require.Zero(t, JitterOffset("a", "world", 0))
}

func TestJitterMovesEverySlotByItsOffset(t *testing.T) {
	loc := berlin(t)
	p := cronPlan(t, "0 3 * * *", loc)

	offset := 7 * time.Minute
	last := time.Date(2026, 10, 11, 3, 7, 1, 0, loc)

	require.Equal(t, time.Date(2026, 10, 12, 3, 7, 0, 0, loc), p.Due(offset, last, last.Add(time.Hour)))
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

	next := Every(time.Hour).next(last)
	require.Equal(t, 30*time.Minute, next.Sub(last))
	require.Equal(t, time.Hour, Every(time.Hour).next(next).Sub(next))
}

func TestStaleAfterIsThreeIntervals(t *testing.T) {
	last := time.Date(2026, 10, 11, 3, 0, 0, 0, time.UTC)

	require.Equal(t, 3*time.Minute, Every(time.Minute).staleAfter(last))
	require.Equal(t, 72*time.Hour, cronPlan(t, "0 3 * * *", time.UTC).staleAfter(last))
}

func TestACronTaskIsStaleOnlyOnceThreeSlotsPassedSinceItsLastSuccess(t *testing.T) {
	friday := time.Date(2026, 10, 9, 2, 30, 0, 0, time.UTC)
	tuesday := time.Date(2026, 10, 13, 2, 30, 0, 0, time.UTC)

	for _, c := range []struct {
		expr      string
		last      time.Time
		fresh     time.Time
		staleFrom time.Time
	}{
		{"0 2 * * 1-5", friday, time.Date(2026, 10, 12, 2, 0, 30, 0, time.UTC), time.Date(2026, 10, 14, 2, 0, 0, 0, time.UTC)},
		{"0 2 * * 1,2", tuesday, time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC), time.Date(2026, 10, 26, 2, 0, 0, 0, time.UTC)},
	} {
		bound := cronPlan(t, c.expr, time.UTC).staleAfter(c.last)
		require.LessOrEqual(t, c.fresh.Sub(c.last), bound, c.expr)
		require.Equal(t, c.staleFrom.Sub(c.last), bound, c.expr)
	}
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
