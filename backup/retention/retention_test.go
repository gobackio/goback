package retention

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func daily(from time.Time, days int) []Commit {
	commits := make([]Commit, days)
	for i := range commits {
		commits[i] = Commit{ReceivedAt: from.AddDate(0, 0, -i)}
	}

	return commits
}

func kept(decisions []Decision) int {
	n := 0
	for _, d := range decisions {
		if d.Keep {
			n++
		}
	}

	return n
}

func TestEvaluateThinsDailiesToWeekliesToMonthlies(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	commits := daily(now, 120)

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Daily, Count: 7},
		{Period: Weekly, Count: 4},
		{Period: Monthly},
	}}, now)

	require.Equal(t, []string{"latest", "last", "daily"}, got[0].Reasons)

	for i := 1; i < 7; i++ {
		require.True(t, got[i].Keep, "day %d", i)
		require.Contains(t, got[i].Reasons, "daily")
	}

	require.False(t, got[8].Keep, "past the week, a daily that is not its week's newest is thinned")

	weeklies, monthlies := 0, 0
	for _, d := range got {
		for _, r := range d.Reasons {
			switch r {
			case "weekly":
				weeklies++
			case "monthly":
				monthlies++
			}
		}
	}

	require.InDelta(t, 4, weeklies, 1)
	require.InDelta(t, 3, monthlies, 1)
	require.Less(t, kept(got), 20, "120 daily commits thinned to a fortnight of detail and a tail")
}

func TestAYearlyTailKeepsEachCalendarYearsLastCommitPastTheMonthlies(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	var commits []Commit
	for i := range 50 {
		commits = append(commits, Commit{ReceivedAt: now.AddDate(0, -i, 0)})
	}

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Monthly, Count: 12},
		{Period: Yearly},
	}}, now)

	var yearly []time.Time
	for i, d := range got {
		if i < 12 {
			require.True(t, d.Keep, "month %d", i)
		}

		if slices.Contains(d.Reasons, "yearly") {
			yearly = append(yearly, commits[i].ReceivedAt)
		}
	}

	require.Equal(t, []time.Time{
		time.Date(2024, 12, 16, 12, 0, 0, 0, time.UTC),
		time.Date(2023, 12, 16, 12, 0, 0, 0, time.UTC),
		time.Date(2022, 12, 16, 12, 0, 0, 0, time.UTC),
	}, yearly, "past the twelve months, the last commit of each year; 2025's is a monthly already")
	require.Equal(t, 15, kept(got))

	got = Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Monthly, Count: 12},
		{Period: Yearly, Count: 3},
	}}, now)
	require.Equal(t, 14, kept(got), "three years counting the one the monthlies ended in: 2025, 2024 and 2023")

	p, err := Parse([]byte(`{"keep_last":1,"brackets":[{"period":"monthly","count":12},{"period":"yearly"}]}`))
	require.NoError(t, err)
	require.Equal(t, Yearly, p.Brackets[1].Period)
}

// Brackets go by a commit's age, not by how many periods happen to hold
// commits, so a set nobody has backed up in months is thinned to what
// its age deserves rather than keeping the last few it managed.
func TestASetThatStoppedIsThinnedByItsAgeAndNotByWhatItHas(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	// an agent that stopped six months ago
	commits := daily(now.AddDate(0, -6, 0), 3)

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Daily, Count: 7},
		{Period: Monthly},
	}}, now)

	require.Equal(t, 1, kept(got), "three dailies of one old month are one monthly")
	require.Equal(t, []string{"latest", "last", "monthly"}, got[0].Reasons)
}

func TestEvaluateKeepsTheNewestAndPinned(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	commits := daily(now, 5)
	commits[3].Pinned = true

	got := Evaluate(commits, Policy{KeepLast: 1}, now)

	require.Equal(t, []string{"latest", "last"}, got[0].Reasons)
	require.False(t, got[1].Keep)
	require.Equal(t, []string{"pinned"}, got[3].Reasons)
	require.False(t, got[4].Keep)
}

func TestEvaluatePartialCheckpoints(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := []Commit{
		{ReceivedAt: now, Partial: true},
		{ReceivedAt: now.Add(-time.Hour)},
	}

	got := Evaluate(commits, Policy{KeepLast: 1}, now)
	require.Equal(t, []string{"latest"}, got[0].Reasons, "a newest checkpoint is live but counts for nothing")
	require.Equal(t, []string{"last"}, got[1].Reasons, "the real commit is still the last one")

	commits = []Commit{
		{ReceivedAt: now},
		{ReceivedAt: now.Add(-time.Hour), Partial: true},
		{ReceivedAt: now.Add(-2 * time.Hour)},
	}

	got = Evaluate(commits, Policy{KeepLast: 2}, now)
	require.True(t, got[0].Keep)
	require.False(t, got[1].Keep, "a superseded checkpoint is retired")
	require.True(t, got[2].Keep, "and does not use up a keep_last slot")
}

func TestAPeriodKeepsItsLastCompleteCommitOverANewerIncompleteOne(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	yesterday := now.AddDate(0, 0, -1)

	commits := []Commit{
		{ReceivedAt: now},
		{ReceivedAt: yesterday.Add(8 * time.Hour), Incomplete: true},
		{ReceivedAt: yesterday},
		{ReceivedAt: yesterday.Add(-time.Hour)},
	}

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{{Period: Daily, Count: 7}}}, now)
	require.False(t, got[1].Keep, "the incomplete commit is retired")
	require.Equal(t, []string{"daily"}, got[2].Reasons, "the day keeps its last complete commit")
	require.False(t, got[3].Keep)
}

func TestAPeriodWithOnlyIncompleteCommitsKeepsTheLastOfThem(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	yesterday := now.AddDate(0, 0, -1)

	commits := []Commit{
		{ReceivedAt: now},
		{ReceivedAt: yesterday, Incomplete: true},
		{ReceivedAt: yesterday.Add(-time.Hour), Incomplete: true},
	}

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{{Period: Daily, Count: 7}}}, now)
	require.Equal(t, []string{"daily"}, got[1].Reasons)
	require.False(t, got[2].Keep)
}

func TestKeepLastCountsCompleteCommitsBeforeIncompleteOnes(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := []Commit{
		{ReceivedAt: now, Incomplete: true},
		{ReceivedAt: now.Add(-time.Hour), Incomplete: true},
		{ReceivedAt: now.Add(-2 * time.Hour)},
		{ReceivedAt: now.Add(-3 * time.Hour), Incomplete: true},
	}

	got := Evaluate(commits, Policy{KeepLast: 1}, now)
	require.Equal(t, []string{"latest"}, got[0].Reasons, "the newest is kept but takes no slot")
	require.False(t, got[1].Keep)
	require.Equal(t, []string{"last"}, got[2].Reasons)
	require.False(t, got[3].Keep)

	got = Evaluate(commits, Policy{KeepLast: 3}, now)
	require.Equal(t, []bool{true, true, true, false}, []bool{got[0].Keep, got[1].Keep, got[2].Keep, got[3].Keep},
		"incomplete commits fill the slots complete ones leave, newest first")
}

func TestEvaluateKeepWithin(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := []Commit{
		{ReceivedAt: now},
		{ReceivedAt: now.Add(-time.Minute)},
		{ReceivedAt: now.Add(-2 * time.Minute)},
		{ReceivedAt: now.Add(-48 * time.Hour)},
	}

	got := Evaluate(commits, Policy{KeepLast: 1, KeepWithin: 24 * time.Hour}, now)
	require.Equal(t, 3, kept(got), "a flood of commits inside the window cannot push each other out")
	require.False(t, got[3].Keep)

	commits = append(commits, Commit{ReceivedAt: now.AddDate(-50, 0, 0)})
	require.NoError(t, KeepAll.Validate())
	require.Equal(t, 5, kept(Evaluate(commits, KeepAll, now)), "keep-all retires nothing")
}

func TestEvaluateInputOrderIsPreserved(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := []Commit{
		{ReceivedAt: now.Add(-2 * time.Hour)},
		{ReceivedAt: now},
		{ReceivedAt: now.Add(-time.Hour)},
	}

	got := Evaluate(commits, Policy{KeepLast: 1}, now)
	require.False(t, got[0].Keep)
	require.True(t, got[1].Keep)
	require.False(t, got[2].Keep)
}

func TestPolicyValidate(t *testing.T) {
	require.ErrorIs(t, Policy{}.Validate(), ErrInvalidPolicy, "keep-all by zeros is rejected")
	require.ErrorIs(t, Policy{KeepLast: 1, Brackets: []Bracket{{Period: Daily, Count: -1}}}.Validate(), ErrInvalidPolicy)
	require.NoError(t, Policy{KeepLast: 1}.Validate())
}

func TestPolicyJSON(t *testing.T) {
	p := Policy{KeepLast: 2, KeepWithin: 36 * time.Hour, Brackets: []Bracket{{Period: Daily, Count: 7}}}

	data, err := json.Marshal(p)
	require.NoError(t, err)
	require.JSONEq(t, `{"keep_last":2,"keep_within":"36h0m0s","brackets":[{"period":"daily","count":7}]}`, string(data))

	back, err := Parse(data)
	require.NoError(t, err)
	require.Equal(t, p, back)

	_, err = Parse([]byte(`{"keep_last":0}`))
	require.ErrorIs(t, err, ErrInvalidPolicy)

	_, err = Parse([]byte(`{"keep_last":1,"keep_within":"soon"}`))
	require.ErrorIs(t, err, ErrInvalidPolicy)
}

// hourlyCommits is one commit an hour, newest first.
func hourlyCommits(from time.Time, hours int) []Commit {
	commits := make([]Commit, hours)
	for i := range commits {
		commits[i] = Commit{ReceivedAt: from.Add(-time.Duration(i) * time.Hour)}
	}

	return commits
}

// The brackets the user asked for: hourly for a fortnight, then daily
// for two months, then weekly for a quarter, then monthly forever.
func theirPolicy() Policy {
	return Policy{
		KeepLast: 1,
		Brackets: []Bracket{
			{Period: Hourly, Count: 14 * 24},
			{Period: Daily, Count: 60},
			{Period: Weekly, Count: 12},
			{Period: Monthly},
		},
	}
}

func TestEachBracketKeepsTheCommitsFallingInItAtItsOwnGranularity(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	// two years of hourly commits, which every bracket has to thin
	commits := hourlyCommits(now, 2*365*24)
	got := Evaluate(commits, theirPolicy(), now)

	days := func(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

	// why the commits of an age range were kept, and how many for each
	// reason, which is what says a bracket covers that range alone
	over := func(from, to time.Duration) map[string]int {
		found := map[string]int{}

		for i, d := range got {
			age := time.Duration(i) * time.Hour
			if age < from || age >= to {
				continue
			}

			for _, reason := range d.Reasons {
				found[reason]++
			}
		}

		return found
	}

	fortnight, twoMonths := over(0, days(14)), over(days(14), days(74))
	quarter, tail := over(days(74), days(158)), over(days(158), days(730))

	require.Equal(t, 14*24, fortnight["hourly"], "every hour of the fortnight")
	require.Zero(t, fortnight["daily"]+fortnight["weekly"]+fortnight["monthly"],
		"no coarser bracket reaches into the fortnight")

	require.InDelta(t, 60, twoMonths["daily"], 1, "one a day for the sixty days after it")
	require.Zero(t, twoMonths["hourly"], "and the hourly bracket ended at the fortnight")

	require.InDelta(t, 12, quarter["weekly"], 1, "one a week for the twelve weeks after that")
	require.Zero(t, quarter["daily"])

	require.InDelta(t, 19, tail["monthly"], 1, "and one a month for the rest of the two years")
	require.Zero(t, tail["weekly"])
}

// Inside a bracket only the newest commit of each of its periods is
// kept, which is what thins an hourly set down to one a day.
func TestABracketKeepsOnePerPeriodAndTheNewestOfThem(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := hourlyCommits(now, 40*24)
	got := Evaluate(commits, theirPolicy(), now)

	kept, hours := 0, map[int]int{}
	for i, d := range got {
		if time.Duration(i)*time.Hour < 14*24*time.Hour || !d.Keep {
			continue
		}

		kept++
		hours[commits[i].ReceivedAt.UTC().Hour()]++
	}

	require.Equal(t, 26, kept, "one a day over the 26 days between a fortnight old and forty days old")
	require.Equal(t, 26, hours[23], "the last hour of each day, even of the one the fortnight ended partway through")
}

// A month's last commit is kept once the month reaches the monthly
// bracket, though the weekly one it aged through ends that week in the
// month after.
func TestAMonthKeepsItsLastCommitThroughTheWeeksBeforeIt(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	policy := Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Weekly, Count: 8},
		{Period: Monthly},
	}}

	// one commit a day; 31 May 2026 is a Sunday, but 30 June a Tuesday
	commits := daily(now, 200)
	got := Evaluate(commits, policy, now)

	for i, c := range commits {
		day := c.ReceivedAt
		if day.AddDate(0, 0, 1).Month() == day.Month() {
			continue
		}

		require.True(t, got[i].Keep, "the last day of %s", day.Month())
	}
}

func TestTheTailKeepsEverythingOlderThanTheBracketsBeforeIt(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	// ten years of monthly commits, all of them in the tail
	commits := make([]Commit, 120)
	for i := range commits {
		commits[i] = Commit{ReceivedAt: now.AddDate(0, -i, 0)}
	}

	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Daily, Count: 30},
		{Period: Monthly},
	}}, now)

	require.Equal(t, 120, kept(got), "a tail that keeps monthly keeps every month there has ever been")
}

// Without a tail the brackets stop, and what falls off the end of the
// last one is retired rather than kept by default.
func TestCommitsOlderThanTheLastBracketAreNotKept(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	commits := daily(now, 60)
	got := Evaluate(commits, Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Daily, Count: 30},
	}}, now)

	require.True(t, got[29].Keep)
	require.False(t, got[31].Keep, "a month and a day old, with nothing to keep it")
}

func TestABracketPolicyRoundTrips(t *testing.T) {
	encoded, err := json.Marshal(theirPolicy())
	require.NoError(t, err)

	require.Contains(t, string(encoded), `"period":"hourly","count":336`)
	require.Contains(t, string(encoded), `{"period":"monthly"}`, "the tail carries no count")

	read, err := Parse(encoded)
	require.NoError(t, err)
	require.Equal(t, theirPolicy(), read)
}

func TestAPolicyThatKeepsForeverBeforeItsLastBracketIsRefused(t *testing.T) {
	err := Policy{KeepLast: 1, Brackets: []Bracket{
		{Period: Monthly},
		{Period: Daily, Count: 1},
	}}.Validate()

	require.ErrorIs(t, err, ErrInvalidPolicy)
	require.ErrorContains(t, err, "never begin")
}

func TestABracketKeepingByNothingRecognisedIsRefused(t *testing.T) {
	err := Policy{KeepLast: 1, Brackets: []Bracket{{Period: "fortnightly", Count: 1}}}.Validate()

	require.ErrorIs(t, err, ErrInvalidPolicy)
	require.ErrorContains(t, err, "fortnightly")
}

// A stored policy from when periods were counted has no brackets, and
// reading it as one would retire everything its counts were holding.
func TestAPolicyWrittenWhenPeriodsWereCountedIsRefused(t *testing.T) {
	_, err := Parse([]byte(`{"keep_last":1,"keep_daily":14,"keep_weekly":8}`))

	require.ErrorIs(t, err, ErrInvalidPolicy)
	require.ErrorContains(t, err, "keep_daily")
}

func TestAPolicyWrittenWithDurationsIsReadAsCounts(t *testing.T) {
	read, err := Parse([]byte(`{"keep_last":1,"brackets":[{"period":"hourly","for":"336h0m0s"},{"period":"daily","for":"1440h0m0s"},{"period":"weekly","for":"2016h0m0s"},{"period":"monthly","for":"1000h"}]}`))
	require.NoError(t, err)
	require.Equal(t, []Bracket{{Hourly, 336}, {Daily, 60}, {Weekly, 12}, {Monthly, 2}}, read.Brackets, "rounded up")

	written, err := json.Marshal(read)
	require.NoError(t, err)
	require.NotContains(t, string(written), `"for"`)
}

func TestTheWindowsOfAPolicyHoldExactlyTheCommitsItsBracketsKeep(t *testing.T) {
	now := time.Date(2026, 10, 5, 14, 30, 0, 0, time.UTC)
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var commits []Commit
	for at := now.Add(-time.Minute); !at.Before(since); at = at.Add(-time.Hour) {
		commits = append(commits, Commit{ReceivedAt: at})
	}

	windows := Default.Windows(now, since)
	decisions := Evaluate(commits, Default, now)

	// commits run newest first, so the first one seen in a window is its last
	holder := func(at time.Time) int {
		for i, w := range windows {
			if !at.Before(w.From) && at.Before(w.To) {
				return i
			}
		}

		return -1
	}

	seen := map[int]bool{}
	for i, c := range commits {
		w := holder(c.ReceivedAt)
		require.NotEqual(t, -1, w, "%s falls in no window", c.ReceivedAt)

		last := !seen[w]
		seen[w] = true

		byWindow := false
		for _, reason := range decisions[i].Reasons {
			byWindow = byWindow || Period(reason) == windows[w].Period
		}

		if byWindow {
			require.True(t, last, "%s is kept but is not the last of %+v", c.ReceivedAt, windows[w])
		}

		ends := back(windows[w].Period, startOf(windows[w].Period, windows[w].From), -1).Equal(windows[w].To)
		if last && ends {
			require.True(t, decisions[i].Keep, "the last of %+v is not kept", windows[w])
		}
	}

	require.Len(t, seen, len(windows), "every window holds a commit")
	require.Equal(t, Window{Period: Hourly, From: now.Truncate(time.Hour), To: now}, windows[0])
	require.Equal(t, Window{Period: Monthly, From: since, To: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)}, windows[len(windows)-1])
}

func TestAWindowWithinKeepWithinKeepsEveryCommit(t *testing.T) {
	now := time.Date(2026, 10, 5, 14, 30, 0, 0, time.UTC)
	p := Policy{KeepLast: 1, KeepWithin: 48 * time.Hour, Brackets: []Bracket{{Period: Daily, Count: 7}}}

	windows := p.Windows(now, time.Time{})
	require.Len(t, windows, 7)

	for _, w := range windows {
		require.Equal(t, !w.From.Before(now.Add(-48*time.Hour)), w.Every, "%+v", w)
	}

	require.True(t, windows[0].Every)
	require.False(t, windows[6].Every)
}

func TestABracketReadsBackAsItIsWritten(t *testing.T) {
	for _, b := range []Bracket{{Period: Daily, Count: 60}, {Period: Monthly}} {
		read, err := ParseBracket(b.String())
		require.NoError(t, err)
		require.Equal(t, b, read)
	}

	for _, bad := range []string{"daily=0", "daily=", "daily=many"} {
		_, err := ParseBracket(bad)
		require.ErrorIs(t, err, ErrInvalidPolicy, bad)
	}

	require.Equal(t, "keep-last=1 keep=hourly=336 keep=daily=60 keep=weekly=12 keep=monthly", Default.String())
	require.Equal(t, "keep-last=1 keep-within=forever", KeepAll.String())
}

func TestAPolicyReadsBackAsItIsWritten(t *testing.T) {
	for _, p := range []Policy{Default, KeepAll, DatabaseBase, DatabaseLog, {KeepLast: 3, KeepWithin: 90 * time.Minute}} {
		read, err := ParsePolicy(p.String())
		require.NoError(t, err)
		require.Equal(t, p, read, p.String())
	}

	read, err := ParsePolicy("keep=daily=7")
	require.NoError(t, err)
	require.Equal(t, Policy{KeepLast: 1, Brackets: []Bracket{{Period: Daily, Count: 7}}}, read)

	for _, bad := range []string{"", "keep nothing", "keep-last=0", "keep-last=x", "keep=fortnightly=2", "keep=monthly keep=daily=3", "keep-within=soon", "hold=3"} {
		_, err := ParsePolicy(bad)
		require.ErrorIs(t, err, ErrInvalidPolicy, bad)
	}
}
