package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func utcAt(year int, month time.Month, day, hour, min int) time.Time {
	return time.Date(year, month, day, hour, min, 0, 0, time.UTC)
}

// counted reads counts as period starts in UTC with their counts.
func counted(counts []*proto.CommitCount) map[time.Time]int64 {
	out := map[time.Time]int64{}
	for _, c := range counts {
		out[time.Unix(0, c.StartNs).UTC()] = c.Count
	}

	return out
}

// newest reads counts as period starts in UTC with the ref and receipt
// instant of their newest commit.
func newest(counts []*proto.CommitCount) map[time.Time]periodNewest {
	out := map[time.Time]periodNewest{}
	for _, c := range counts {
		out[time.Unix(0, c.StartNs).UTC()] = periodNewest{fmt.Sprintf("%x", c.Newest.GetHash()), time.Unix(0, c.NewestReceivedAtNs).UTC()}
	}

	return out
}

type periodNewest struct {
	ref      string
	received time.Time
}

func TestCountCommitsPerPeriodInAZone(t *testing.T) {
	f := newFixture(t)

	received := []time.Time{
		utcAt(2025, 12, 31, 23, 30), // 00:30 on New Year's Day in Berlin
		utcAt(2026, 3, 28, 23, 30),  // 00:30 CET on the day clocks spring forward
		utcAt(2026, 3, 29, 0, 30),   // 01:30 CET
		utcAt(2026, 3, 29, 1, 30),   // 03:30 CEST
		utcAt(2026, 3, 29, 22, 30),  // 00:30 CEST the next day
		utcAt(2026, 3, 31, 10, 0),
		utcAt(2026, 4, 15, 10, 0),
		utcAt(2026, 10, 25, 0, 30),  // 02:30 CEST on the day clocks fall back
		utcAt(2026, 10, 25, 1, 30),  // 02:30 CET
		utcAt(2026, 10, 25, 22, 30), // 23:30 CET, the 25th hour of the day
	}

	refs := make([]string, len(received))
	for i, at := range received {
		f.clock = at
		refs[i] = fmt.Sprintf("%x", f.commit("world", f.tree(f.file("a.txt", fmt.Sprint(i))), false).GetHash())
	}

	newestOf := func(i int) periodNewest { return periodNewest{refs[i], received[i]} }

	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	local := func(year int, month time.Month, day int) time.Time {
		return time.Date(year, month, day, 0, 0, 0, 0, berlin)
	}

	count := func(period proto.Period, from, to time.Time, zone string) map[time.Time]int64 {
		counts, err := f.x.CountCommits(f.ctx, "world", period, from, to, zone, false)
		require.NoError(t, err)

		for i := 1; i < len(counts); i++ {
			require.Less(t, counts[i-1].StartNs, counts[i].StartNs, "oldest first")
		}

		return counted(counts)
	}

	require.Equal(t, map[time.Time]int64{utcAt(2025, 12, 31, 23, 0): 10},
		count(proto.Period_PERIOD_YEAR, local(2025, 1, 1), local(2027, 1, 1), "Europe/Berlin"))
	require.Equal(t, map[time.Time]int64{utcAt(2025, 1, 1, 0, 0): 1, utcAt(2026, 1, 1, 0, 0): 9},
		count(proto.Period_PERIOD_YEAR, utcAt(2025, 1, 1, 0, 0), utcAt(2027, 1, 1, 0, 0), ""))
	require.Equal(t, map[time.Time]int64{utcAt(2025, 12, 31, 23, 0): 10},
		count(proto.Period_PERIOD_YEAR, local(2026, 1, 1), local(2027, 1, 1), "Europe/Berlin"), "a range within one period")

	require.Equal(t, map[time.Time]int64{
		utcAt(2025, 12, 31, 23, 0): 1,
		utcAt(2026, 2, 28, 23, 0):  5,
		utcAt(2026, 3, 31, 22, 0):  1,
		utcAt(2026, 9, 30, 22, 0):  3,
	}, count(proto.Period_PERIOD_MONTH, local(2026, 1, 1), local(2027, 1, 1), "Europe/Berlin"))

	months, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_MONTH, local(2026, 1, 1), local(2027, 1, 1), "Europe/Berlin", false)
	require.NoError(t, err)
	require.Equal(t, map[time.Time]periodNewest{
		utcAt(2025, 12, 31, 23, 0): newestOf(0),
		utcAt(2026, 2, 28, 23, 0):  newestOf(5),
		utcAt(2026, 3, 31, 22, 0):  newestOf(6),
		utcAt(2026, 9, 30, 22, 0):  newestOf(9),
	}, newest(months), "each period names the commit received last")

	march, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_MONTH, local(2026, 3, 1), utcAt(2026, 3, 30, 0, 0), "Europe/Berlin", false)
	require.NoError(t, err)
	require.Equal(t, map[time.Time]periodNewest{utcAt(2026, 2, 28, 23, 0): newestOf(4)}, newest(march),
		"the newest is the newest within the range")

	require.Equal(t, map[time.Time]int64{
		utcAt(2026, 3, 28, 23, 0): 3,
		utcAt(2026, 3, 29, 22, 0): 1,
		utcAt(2026, 3, 30, 22, 0): 1,
	}, count(proto.Period_PERIOD_DAY, local(2026, 3, 1), local(2026, 4, 1), "Europe/Berlin"))

	require.Equal(t, map[time.Time]int64{utcAt(2026, 10, 24, 22, 0): 3},
		count(proto.Period_PERIOD_DAY, local(2026, 10, 1), local(2026, 11, 1), "Europe/Berlin"),
		"the day clocks fall back runs 25 hours")

	require.Equal(t, map[time.Time]int64{
		utcAt(2026, 3, 28, 23, 0): 1,
		utcAt(2026, 3, 29, 0, 0):  1,
		utcAt(2026, 3, 29, 1, 0):  1,
	}, count(proto.Period_PERIOD_HOUR, local(2026, 3, 29), local(2026, 3, 30), "Europe/Berlin"))

	require.Equal(t, map[time.Time]int64{
		utcAt(2026, 10, 25, 0, 0):  1,
		utcAt(2026, 10, 25, 1, 0):  1,
		utcAt(2026, 10, 25, 22, 0): 1,
	}, count(proto.Period_PERIOD_HOUR, local(2026, 10, 25), local(2026, 10, 26), "Europe/Berlin"),
		"the repeated hour counts twice")

	require.Equal(t, map[time.Time]int64{utcAt(2026, 3, 28, 23, 0): 2, utcAt(2026, 3, 29, 22, 0): 1},
		count(proto.Period_PERIOD_DAY, utcAt(2026, 3, 29, 0, 0), utcAt(2026, 3, 31, 10, 0), "Europe/Berlin"),
		"from is inclusive, to exclusive, and a period starts where it does even before from")

	missing, err := f.x.CountCommits(f.ctx, "unknown", proto.Period_PERIOD_DAY, local(2026, 3, 1), local(2026, 4, 1), "", false)
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestCountCommitsLiveOrDeleted(t *testing.T) {
	f := newFixture(t)

	day := utcAt(2026, 9, 16, 0, 0)

	f.clock = day.Add(time.Hour)
	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.clock = day.Add(2 * time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), true)
	f.clock = day.Add(25 * time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "three")), false)

	count := func(deleted bool) map[time.Time]int64 {
		counts, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, day, day.Add(48*time.Hour), "", deleted)
		require.NoError(t, err)

		return counted(counts)
	}

	require.Equal(t, map[time.Time]int64{day: 1, day.Add(24 * time.Hour): 1}, count(false), "a partial commit is not counted")
	require.Empty(t, count(true))

	require.NoError(t, f.x.DeleteCommit(f.ctx, a))
	require.Equal(t, map[time.Time]int64{day.Add(24 * time.Hour): 1}, count(false))
	require.Equal(t, map[time.Time]int64{day: 1}, count(true))

	trashed, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, day, day.Add(48*time.Hour), "", true)
	require.NoError(t, err)
	require.Equal(t, map[time.Time]periodNewest{day: {fmt.Sprintf("%x", a.GetHash()), day.Add(time.Hour)}}, newest(trashed),
		"a deleted period names its newest deleted commit")

	_, err = f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Empty(t, count(true), "a tombstoned commit is not counted")
}

func TestCountCommitsRefusesWhatItCannotCount(t *testing.T) {
	f := newFixture(t)

	from := utcAt(2026, 1, 1, 0, 0)

	for name, call := range map[string]func() error{
		"unknown zone": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, from, from.AddDate(0, 1, 0), "Mars/Olympus", false)
			return err
		},
		"server zone": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, from, from.AddDate(0, 1, 0), "Local", false)
			return err
		},
		"no period": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_UNSPECIFIED, from, from.AddDate(0, 1, 0), "", false)
			return err
		},
		"empty range": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, from, from, "", false)
			return err
		},
		"open range": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_DAY, time.Time{}, from, "", false)
			return err
		},
		"too many periods": func() error {
			_, err := f.x.CountCommits(f.ctx, "world", proto.Period_PERIOD_HOUR, from, from.AddDate(0, 2, 0), "", false)
			return err
		},
	} {
		require.ErrorIs(t, call(), backup.ErrInvalidCount, name)
	}

	starts, err := periodStarts(proto.Period_PERIOD_HOUR, from, from.Add(backup.MaxCountPeriods*time.Hour), "")
	require.NoError(t, err)
	require.Len(t, starts, backup.MaxCountPeriods)
}

func TestMidnightSkippedByAClockChange(t *testing.T) {
	santiago, err := time.LoadLocation("America/Santiago")
	require.NoError(t, err)

	// Chile springs forward from 00:00 to 01:00.
	start := midnight(2026, time.September, 6, santiago)
	require.Equal(t, utcAt(2026, 9, 6, 4, 0), start.UTC())
	require.Equal(t, 6, start.Day())
	require.Equal(t, 1, start.Hour())
}

func TestHourOfFollowsHalfHourOffsets(t *testing.T) {
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	require.Equal(t, utcAt(2026, 3, 1, 9, 30), hourOf(utcAt(2026, 3, 1, 10, 15), kolkata).UTC())
}
