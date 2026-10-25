package retention

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAPolicyCoversOnlyWhatKeepsAtLeastAsMuch(t *testing.T) {
	week := 7 * 24 * time.Hour
	base := Policy{KeepLast: 2, KeepWithin: week, Brackets: []Bracket{
		{Period: Hourly, Count: 48},
		{Period: Daily, Count: 30},
		{Period: Monthly, Count: 12},
	}}

	with := func(change func(*Policy)) Policy {
		p := base
		p.Brackets = append([]Bracket(nil), base.Brackets...)
		change(&p)

		return p
	}

	for _, c := range []struct {
		name   string
		p, q   Policy
		covers bool
	}{
		{"itself", base, base, true},
		{"the default itself", Default, Default, true},
		{"more of the newest", with(func(p *Policy) { p.KeepLast = 5 }), base, true},
		{"fewer of the newest", with(func(p *Policy) { p.KeepLast = 1 }), base, false},
		{"a keep_last below one is one", Policy{KeepLast: 1}, Policy{KeepLast: 0}, true},
		{"one is a keep_last below one", Policy{KeepLast: 0}, Policy{KeepLast: 1}, true},
		{"a longer window", with(func(p *Policy) { p.KeepWithin = 2 * week }), base, true},
		{"a shorter window", with(func(p *Policy) { p.KeepWithin = week / 2 }), base, false},
		{"no window", with(func(p *Policy) { p.KeepWithin = 0 }), base, false},
		{"a longer bracket", with(func(p *Policy) { p.Brackets[1].Count = 31 }), base, true},
		{"a shorter bracket", with(func(p *Policy) { p.Brackets[1].Count = 29 }), base, false},
		{"a shorter first bracket", with(func(p *Policy) { p.Brackets[0].Count = 47 }), base, false},
		{"the last bracket forever", with(func(p *Policy) { p.Brackets[2].Count = 0 }), base, true},
		{"the forever bracket ending", base, with(func(p *Policy) { p.Brackets[2].Count = 0 }), false},
		{"a bracket after the others", with(func(p *Policy) { p.Brackets = append(p.Brackets, Bracket{Period: Yearly}) }), base, true},
		{"a bracket before the others", with(func(p *Policy) { p.Brackets = append([]Bracket{{Period: Hourly, Count: 6}}, p.Brackets...) }), base, true},
		{"a bracket between two", with(func(p *Policy) {
			p.Brackets = append(p.Brackets[:2:2], Bracket{Period: Weekly, Count: 8}, p.Brackets[2])
		}), base, true},
		{"a bracket dropped", with(func(p *Policy) { p.Brackets = p.Brackets[:2] }), base, false},
		{"the first bracket dropped", with(func(p *Policy) { p.Brackets = p.Brackets[1:] }), base, false},
		{"a bracket swapped for another", with(func(p *Policy) { p.Brackets[1] = Bracket{Period: Weekly, Count: 30} }), base, false},
		{"a bracket swapped for a finer one as long", with(func(p *Policy) { p.Brackets[1] = Bracket{Period: Hourly, Count: 30 * 24} }), base, false},
		{"brackets swapped in order", with(func(p *Policy) { p.Brackets[1], p.Brackets[2] = p.Brackets[2], p.Brackets[1] }), base, false},
		{"no brackets", with(func(p *Policy) { p.Brackets = nil }), base, false},
		{"brackets where there were none", base, Policy{KeepLast: 2, KeepWithin: week}, true},
		{"everything", KeepAll, base, true},
		{"everything over the default", KeepAll, Default, true},
		{"everything over a tail", KeepAll, Policy{KeepLast: 100, Brackets: []Bracket{{Period: Yearly}}}, true},
		{"everything over everything", KeepAll, KeepAll, true},
		{"anything less than everything", Default, KeepAll, false},
		{"a long window over brackets", Policy{KeepLast: 2, KeepWithin: 100 * 365 * 24 * time.Hour}, base, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.covers, c.p.Covers(c.q))
		})
	}
}

// changed is p after one of the changes Covers takes as keeping more, or
// loosely, after one that may keep less.
func changed(rng *rand.Rand, p Policy, loose bool) Policy {
	p.Brackets = append([]Bracket(nil), p.Brackets...)

	if loose && rng.IntN(2) == 0 {
		switch rng.IntN(4) {
		case 0:
			p.KeepLast = max(1, p.KeepLast-1)
		case 1:
			p.KeepWithin /= 2
		case 2:
			if n := len(p.Brackets); n > 0 {
				i := rng.IntN(n)
				p.Brackets[i] = Bracket{Period: allPeriods[rng.IntN(len(allPeriods))], Count: max(1, p.Brackets[i].Count+rng.IntN(5)-2)}
				if i == n-1 && rng.IntN(2) == 0 {
					p.Brackets[i].Count = 0
				}
			}
		case 3:
			if n := len(p.Brackets); n > 0 {
				i := rng.IntN(n)
				p.Brackets = append(p.Brackets[:i], p.Brackets[i+1:]...)
			}
		}

		return p
	}

	switch rng.IntN(5) {
	case 0:
		p.KeepLast += 1 + rng.IntN(3)
	case 1:
		p.KeepWithin += time.Duration(1+rng.IntN(72)) * time.Hour
	case 2:
		if len(p.Brackets) > 0 {
			i := rng.IntN(len(p.Brackets))
			if p.Brackets[i].Count > 0 {
				p.Brackets[i].Count += 1 + rng.IntN(5)
			}
		}
	case 3:
		at := rng.IntN(len(p.Brackets) + 1)
		if at == len(p.Brackets) && len(p.Brackets) > 0 && p.Brackets[at-1].Count == 0 {
			at--
		}

		inserted := Bracket{Period: allPeriods[rng.IntN(len(allPeriods))], Count: 1 + rng.IntN(10)}
		p.Brackets = append(p.Brackets[:at], append([]Bracket{inserted}, p.Brackets[at:]...)...)
	case 4:
		if n := len(p.Brackets); n > 0 {
			p.Brackets[n-1].Count = 0
		}
	}

	return p
}

var allPeriods = []Period{Hourly, Daily, Weekly, Monthly, Yearly}

func randomPolicy(rng *rand.Rand) Policy {
	p := Policy{KeepLast: 1 + rng.IntN(3)}
	if rng.IntN(3) == 0 {
		p.KeepWithin = time.Duration(rng.IntN(96)) * time.Hour
	}

	for n := rng.IntN(4); n > 0; n-- {
		p.Brackets = append(p.Brackets, Bracket{Period: allPeriods[rng.IntN(len(allPeriods))], Count: 1 + rng.IntN(12)})
	}

	if len(p.Brackets) > 0 && rng.IntN(3) == 0 {
		p.Brackets[len(p.Brackets)-1].Count = 0
	}

	return p
}

func TestAPolicyThatCoversAnotherKeepsEveryCommitTheOtherKeeps(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for round := range 400 {
		q := randomPolicy(rng)
		require.NoError(t, q.Validate())

		loose := round%2 == 1
		p := q
		for n := 1 + rng.IntN(3); n > 0; n-- {
			p = changed(rng, p, loose)
		}

		require.NoError(t, p.Validate())
		if !loose {
			require.True(t, p.Covers(q), "round %d: %v over %v", round, p, q)
		}

		if !p.Covers(q) {
			continue
		}

		var commits []Commit
		for at := start; at.Before(start.AddDate(2, 0, 0)); at = at.Add(time.Duration(1+rng.IntN(36*60)) * time.Minute) {
			commits = append(commits, Commit{ReceivedAt: at, Incomplete: rng.IntN(10) == 0, Partial: rng.IntN(50) == 0})
		}

		for range 5 {
			now := commits[len(commits)-1].ReceivedAt.Add(time.Duration(rng.Int64N(int64(400 * 24 * time.Hour))))
			longer, shorter := Evaluate(commits, p, now), Evaluate(commits, q, now)

			for i := range commits {
				if shorter[i].Keep {
					require.True(t, longer[i].Keep, "round %d: %v drops the commit of %v that %v keeps at %v", round, p, commits[i].ReceivedAt, q, now)
				}
			}
		}
	}
}
