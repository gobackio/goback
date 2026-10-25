package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

func reported(kind string, gap, jitter time.Duration) Reported {
	return Reported{
		Expectation: Expectation{Set: "world", Kind: kind, LongestGap: gap, Jitter: jitter},
		Agent:       "host",
		Since:       now.Add(-90 * 24 * time.Hour),
		At:          now.Add(-time.Hour),
	}
}

func TestGraceIsHalfTheGapAndAtLeastAnHour(t *testing.T) {
	require.Equal(t, 12*time.Hour, Grace(24*time.Hour))
	require.Equal(t, time.Hour, Grace(time.Hour))
	require.Equal(t, time.Hour, Grace(10*time.Minute))
}

func TestADailySetIsLateAfterThirtySixHoursAndItsJitter(t *testing.T) {
	r := reported(Every, 24*time.Hour, 30*time.Minute)

	require.False(t, r.Late(now.Add(-36*time.Hour-30*time.Minute), now))
	require.True(t, r.Late(now.Add(-36*time.Hour-31*time.Minute), now))
}

func TestAnHourlySetIsLateAfterTwoHours(t *testing.T) {
	r := reported(Cron, time.Hour, 0)

	require.False(t, r.Late(now.Add(-2*time.Hour), now))
	require.True(t, r.Late(now.Add(-2*time.Hour-time.Second), now))
}

func TestASetWithoutASnapshotIsLateFromWhenItWasFirstReported(t *testing.T) {
	r := reported(Every, time.Hour, 0)

	r.Since = now.Add(-time.Hour)
	require.False(t, r.Late(time.Time{}, now))

	r.Since = now.Add(-3 * time.Hour)
	require.True(t, r.Late(time.Time{}, now))
}

func TestAReceiverIsLateOnlyThroughItsAgentsSilence(t *testing.T) {
	r := reported(Receiver, 0, 0)
	r.CommitEvery = time.Minute

	require.False(t, r.Late(now.Add(-30*24*time.Hour), now))

	r.At = now.Add(-72*time.Hour - time.Second)
	require.True(t, r.Late(now, now))
}

func TestAnAgentTwoDaysPastItsDailyReportMakesItsSetsLate(t *testing.T) {
	r := reported(Every, 7*24*time.Hour, 0)

	r.At = now.Add(-72 * time.Hour)
	require.False(t, r.Silent(now))
	require.False(t, r.Late(now, now))

	r.At = now.Add(-72*time.Hour - time.Second)
	require.True(t, r.Silent(now))
	require.True(t, r.Late(now, now))
}

func TestAnExpectationNotReportedForThirtyDaysExpectsNothing(t *testing.T) {
	r := reported(Every, time.Hour, 0)
	r.At = now.Add(-30*24*time.Hour - time.Second)

	require.True(t, r.Stale(now))
	require.False(t, r.Late(now.Add(-31*24*time.Hour), now))
}

func TestValidateRefusesWhatNoPlanSays(t *testing.T) {
	require.NoError(t, Expectation{Set: "world", Kind: Every, LongestGap: time.Hour}.Validate())
	require.NoError(t, Expectation{Set: "db/wal", Kind: Receiver, CommitEvery: time.Minute}.Validate())

	for _, e := range []Expectation{
		{Kind: Every, LongestGap: time.Hour},
		{Set: "world", Kind: "hourly", LongestGap: time.Hour},
		{Set: "world", Kind: Cron},
		{Set: "world", Kind: Every, LongestGap: time.Hour, Jitter: -time.Second},
		{Set: "db/wal", Kind: Receiver},
	} {
		require.ErrorIs(t, e.Validate(), ErrInvalid, "%+v", e)
	}
}
