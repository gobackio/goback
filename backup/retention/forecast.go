package retention

import (
	"slices"
	"time"
)

// cadenceCommits is how many of a set's newest commits Cadence measures.
const cadenceCommits = 21

// maxArrivals bounds the commits Forecast assumes arrive by one instant.
const maxArrivals = 1000

// Cadence is the median interval between the newest commits that are not
// partial, zero for fewer than two.
func Cadence(commits []Commit) time.Duration {
	times := make([]time.Time, 0, len(commits))
	for _, c := range commits {
		if !c.Partial {
			times = append(times, c.ReceivedAt)
		}
	}

	slices.SortFunc(times, func(a, b time.Time) int { return b.Compare(a) })
	times = times[:min(len(times), cadenceCommits)]

	if len(times) < 2 {
		return 0
	}

	gaps := make([]time.Duration, len(times)-1)
	for i := range gaps {
		gaps[i] = times[i].Sub(times[i+1])
	}

	slices.Sort(gaps)

	return gaps[len(gaps)/2]
}

// Forecast returns, for each commit, the index of the first of the
// ascending instants at which p no longer keeps it, or len(at) when it
// keeps it at every one, as Evaluate decides once a complete commit has
// arrived every cadence after the newest. A cadence of zero assumes none
// arrive.
func Forecast(commits []Commit, p Policy, cadence time.Duration, at []time.Time) []int {
	out := make([]int, len(commits))
	for i := range out {
		out[i] = len(at)
	}

	var newest time.Time
	for _, c := range commits {
		if c.ReceivedAt.After(newest) {
			newest = c.ReceivedAt
		}
	}

	for n, when := range at {
		input := append(slices.Clip(commits), arrivals(p, newest, cadence, when)...)

		for i, d := range Evaluate(input, p, when)[:len(commits)] {
			if !d.Keep && out[i] == len(at) {
				out[i] = n
			}
		}
	}

	return out
}

// arrivals are the commits arriving every cadence after newest up to
// until that can change what p decides for the commits before them: the
// newest KeepLast, and the last of each of p's periods newest falls in.
func arrivals(p Policy, newest time.Time, cadence time.Duration, until time.Time) []Commit {
	if cadence <= 0 || !until.After(newest) {
		return nil
	}

	arrived := int64(until.Sub(newest) / cadence)
	if arrived < 1 {
		return nil
	}

	picked := make(map[int64]bool)
	for k := max(1, arrived-int64(min(max(p.KeepLast, 1), maxArrivals))+1); k <= arrived; k++ {
		picked[k] = true
	}

	for _, b := range p.Brackets {
		end := back(b.Period, startOf(b.Period, newest), -1)
		if k := min(arrived, int64(end.Sub(newest)-1)/int64(cadence)); k >= 1 {
			picked[k] = true
		}
	}

	out := make([]Commit, 0, len(picked))
	for k := range picked {
		out = append(out, Commit{ReceivedAt: newest.Add(time.Duration(k) * cadence)})
	}

	return out
}
