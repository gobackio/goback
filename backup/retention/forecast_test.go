package retention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCadenceIsTheMedianGapBetweenTheNewestCommits(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	require.Zero(t, Cadence(nil))
	require.Zero(t, Cadence([]Commit{{ReceivedAt: now}}))

	commits := []Commit{{ReceivedAt: now}, {ReceivedAt: now.Add(-time.Hour)}, {ReceivedAt: now.Add(-3 * time.Hour)},
		{ReceivedAt: now.Add(-4 * time.Hour)}, {ReceivedAt: now.Add(-5 * time.Hour)}, {ReceivedAt: now.Add(-30 * 24 * time.Hour)}}
	require.Equal(t, time.Hour, Cadence(commits))

	var hourly []Commit
	for i := range 100 {
		hourly = append(hourly, Commit{ReceivedAt: now.Add(-time.Duration(i) * time.Hour)})
	}
	for i := range 20 {
		hourly = append(hourly, Commit{ReceivedAt: now.Add(-time.Duration(i) * time.Minute)})
	}
	require.Equal(t, time.Minute, Cadence(hourly), "only the newest commits count")

	checkpoints := commits
	for i := range 6 {
		checkpoints = append(checkpoints, Commit{ReceivedAt: now.Add(time.Duration(i+1) * time.Second), Partial: true})
	}
	require.Equal(t, time.Hour, Cadence(checkpoints), "a partial commit counts for nothing")
}

func TestForecastRetiresWhatArrivalsSupersede(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 30, 0, 0, time.UTC)
	p := Policy{KeepLast: 1, Brackets: []Bracket{{Period: Hourly, Count: 48}, {Period: Daily, Count: 20}, {Period: Monthly}}}

	var commits []Commit
	for i := range 6 {
		commits = append(commits, Commit{ReceivedAt: now.Add(-time.Duration(i) * time.Hour)})
	}

	past := time.Date(2026, 8, 31, 23, 0, 0, 0, time.UTC)
	commits = append(commits, Commit{ReceivedAt: past})

	at := []time.Time{now.Add(7 * 24 * time.Hour), now.Add(31 * 24 * time.Hour), now.Add(92 * 24 * time.Hour)}

	got := Forecast(commits, p, time.Hour, at)
	for i := range 6 {
		require.Equal(t, 0, got[i], "commit %d is no day's last once the day goes on", i)
	}
	require.Equal(t, len(at), got[6], "a past month's last commit is kept for good")

	still := Forecast(commits, p, 0, at)
	require.Equal(t, len(at), still[0], "with nothing arriving the newest stays")
	require.Equal(t, 0, still[1])
}

func TestForecastCountsArrivalsAgainstKeepLast(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	p := Policy{KeepLast: 5}

	var commits []Commit
	for i := range 5 {
		commits = append(commits, Commit{ReceivedAt: now.Add(-time.Duration(i) * time.Minute)})
	}

	got := Forecast(commits, p, time.Minute, []time.Time{now.Add(3 * time.Minute), now.Add(time.Hour)})
	require.Equal(t, []int{1, 1, 0, 0, 0}, got)
}
