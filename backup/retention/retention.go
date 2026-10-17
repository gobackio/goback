// Package retention decides which commits of a set a policy keeps.
package retention

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Period is how finely a bracket keeps: one commit per hour, day, week
// or month of the stretch it covers.
type Period string

// The periods a bracket may keep by.
const (
	Hourly  Period = "hourly"
	Daily   Period = "daily"
	Weekly  Period = "weekly"
	Monthly Period = "monthly"
)

// A Bracket keeps the last commit of each of Count UTC calendar periods.
// Brackets run consecutively back from now, each starting with the
// period the one before it ended in. A Count of zero is the tail: every
// period older than the brackets before it, kept forever.
type Bracket struct {
	Period Period
	Count  int
}

// Policy is what a set keeps. Brackets divide its past into consecutive
// stretches, each kept at its own granularity; KeepLast and KeepWithin
// hold on to the newest commits regardless of which bracket they fall
// in. The rules are ORed, and KeepLast is never below 1.
type Policy struct {
	Brackets   []Bracket
	KeepLast   int
	KeepWithin time.Duration
}

// Default is the policy a store applies until it sets its own.
var Default = Policy{KeepLast: 1, Brackets: []Bracket{
	{Period: Hourly, Count: 14 * 24},
	{Period: Daily, Count: 60},
	{Period: Weekly, Count: 12},
	{Period: Monthly},
}}

// DatabaseBase is the initial policy of a database's base backup or dump
// set: daily for 60 days, then weekly for 4 weeks.
var DatabaseBase = Policy{KeepLast: 1, Brackets: []Bracket{
	{Period: Daily, Count: 60},
	{Period: Weekly, Count: 4},
}}

// DatabaseLog is the initial policy of a WAL or binlog set. Each of its
// commits holds the log back to the newest base backup before the
// recovery window, so the newest commits suffice.
var DatabaseLog = Policy{KeepLast: 60}

// KeepAll is a policy that retires nothing.
var KeepAll = Policy{KeepLast: 1, KeepWithin: time.Duration(math.MaxInt64)}

// ErrInvalidPolicy wraps every validation failure.
var ErrInvalidPolicy = errors.New("invalid retention policy")

// Validate rejects a policy with a negative counter, a keep_last below 1
// or nothing to keep at all.
func (p Policy) Validate() error {
	if p.KeepLast < 1 {
		return fmt.Errorf("%w: keep_last must be at least 1", ErrInvalidPolicy)
	}

	if p.KeepWithin < 0 {
		return fmt.Errorf("%w: keep_within is negative", ErrInvalidPolicy)
	}

	for i, b := range p.Brackets {
		if _, ok := periods[b.Period]; !ok {
			return fmt.Errorf("%w: bracket %d keeps by %q, not one of hourly, daily, weekly or monthly",
				ErrInvalidPolicy, i, b.Period)
		}

		if b.Count < 0 {
			return fmt.Errorf("%w: bracket %d keeps a negative count", ErrInvalidPolicy, i)
		}

		if b.Count == 0 && i != len(p.Brackets)-1 {
			return fmt.Errorf("%w: bracket %d keeps %s forever, so the brackets after it never begin",
				ErrInvalidPolicy, i, b.Period)
		}
	}

	return nil
}

// periods is how a commit's place in each period is named, so that two
// commits of one period compare equal.
var periods = map[Period]func(time.Time) string{
	Hourly: func(t time.Time) string { return t.UTC().Format("2006-01-02T15") },
	Daily:  func(t time.Time) string { return t.UTC().Format("2006-01-02") },
	Weekly: func(t time.Time) string {
		y, w := t.UTC().ISOWeek()

		return fmt.Sprintf("%04d-W%02d", y, w)
	},
	Monthly: func(t time.Time) string { return t.UTC().Format("2006-01") },
}

// startOf is when the period holding t began.
func startOf(period Period, t time.Time) time.Time {
	t = t.UTC()

	switch period {
	case Hourly:
		return t.Truncate(time.Hour)
	case Daily:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case Weekly:
		day := startOf(Daily, t)

		return day.AddDate(0, 0, -(int(day.Weekday())+6)%7)
	default:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
}

// back is the start of the period n before the one starting at start.
func back(period Period, start time.Time, n int) time.Time {
	switch period {
	case Hourly:
		return start.Add(-time.Duration(n) * time.Hour)
	case Daily:
		return start.AddDate(0, 0, -n)
	case Weekly:
		return start.AddDate(0, 0, -7*n)
	default:
		return start.AddDate(0, -n, 0)
	}
}

// bounds is where each bracket begins, walking back from now: bracket i
// holds the commits at or after bounds[i] and before bounds[i-1]. The
// tail begins at the zero time.
func (p Policy) bounds(now time.Time) []time.Time {
	out := make([]time.Time, len(p.Brackets))
	until := now

	for i, b := range p.Brackets {
		if b.Count == 0 {
			break
		}

		// the period holding the newest moment this bracket covers
		newest := startOf(b.Period, until.Add(-time.Nanosecond))
		if i == 0 {
			newest = startOf(b.Period, now)
		}

		until = back(b.Period, newest, b.Count-1)
		out[i] = until
	}

	return out
}

// A Window is one calendar period of a bracket, from From up to To
// (exclusive), whose last commit the policy keeps. Every marks a window
// within KeepWithin, which keeps all of its commits.
type Window struct {
	Period   Period
	From, To time.Time
	Every    bool
}

// Windows lists the windows p keeps a commit of as of now, newest first:
// each period of each bracket, clipped to the bracket, and the tail's
// periods back to since. A window also keeps the last commit of any
// coarser period of a later bracket that ends in it.
func (p Policy) Windows(now, since time.Time) []Window {
	var out []Window

	bounds := p.bounds(now)
	until := now

	for i, b := range p.Brackets {
		from := bounds[i]
		if b.Count == 0 {
			from = since
		}

		if !until.After(from) {
			break
		}

		for at := startOf(b.Period, until.Add(-time.Nanosecond)); ; at = back(b.Period, at, 1) {
			w := Window{Period: b.Period, From: at, To: back(b.Period, at, -1)}
			if w.From.Before(from) {
				w.From = from
			}

			if w.To.After(until) {
				w.To = until
			}

			w.Every = p.KeepWithin > 0 && !w.From.Before(now.Add(-p.KeepWithin))
			out = append(out, w)

			if !at.After(from) {
				break
			}
		}

		until = from
	}

	return out
}

// bracketAt is the bracket a commit received at t falls in, and whether
// the brackets reach that far back at all.
func bracketAt(bounds []time.Time, t time.Time) (int, bool) {
	for i, from := range bounds {
		if !t.Before(from) {
			return i, true
		}
	}

	return 0, false
}

type bracketJSON struct {
	Period Period `json:"period"`
	// absent for the tail, which lasts forever
	Count int `json:"count,omitempty"`
	// For is how long a bracket lasted in policies written before brackets
	// were counted; reading turns it into a count. Never written.
	For string `json:"for,omitempty"`
}

// spans is about how long one period lasts, to count a For in.
var spans = map[Period]time.Duration{
	Hourly:  time.Hour,
	Daily:   24 * time.Hour,
	Weekly:  7 * 24 * time.Hour,
	Monthly: 30 * 24 * time.Hour,
}

type policyJSON struct {
	Brackets   []bracketJSON `json:"brackets,omitempty"`
	KeepLast   int           `json:"keep_last"`
	KeepWithin string        `json:"keep_within,omitempty"`
}

// MarshalJSON writes keep_within as a duration string.
func (p Policy) MarshalJSON() ([]byte, error) {
	out := policyJSON{KeepLast: p.KeepLast}
	if p.KeepWithin > 0 {
		out.KeepWithin = p.KeepWithin.String()
	}

	for _, b := range p.Brackets {
		out.Brackets = append(out.Brackets, bracketJSON{Period: b.Period, Count: b.Count})
	}

	return json.Marshal(out)
}

// UnmarshalJSON reads keep_within as a duration string. A policy
// written when periods were counted rather than bracketed is refused
// rather than read as the brackets it has none of, which would retire
// everything the counts were holding.
func (p *Policy) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	for _, counted := range []string{"keep_hourly", "keep_daily", "keep_weekly", "keep_monthly"} {
		if _, ok := fields[counted]; ok {
			return fmt.Errorf("%w: %s counts periods, which policies no longer do; set brackets instead",
				ErrInvalidPolicy, counted)
		}
	}

	var in policyJSON
	err := json.Unmarshal(data, &in)
	if err != nil {
		return err
	}

	*p = Policy{KeepLast: in.KeepLast}

	if in.KeepWithin != "" {
		p.KeepWithin, err = time.ParseDuration(in.KeepWithin)
		if err != nil {
			return fmt.Errorf("%w: keep_within: %v", ErrInvalidPolicy, err)
		}
	}

	for i, read := range in.Brackets {
		bracket := Bracket{Period: read.Period, Count: read.Count}

		if read.For != "" && read.Count == 0 && spans[read.Period] > 0 {
			lasted, err := time.ParseDuration(read.For)
			if err != nil {
				return fmt.Errorf("%w: bracket %d: %v", ErrInvalidPolicy, i, err)
			}

			span := spans[read.Period]
			bracket.Count = int((lasted + span - 1) / span)
		}

		p.Brackets = append(p.Brackets, bracket)
	}

	return nil
}

// Parse decodes and validates a JSON policy.
func Parse(data []byte) (Policy, error) {
	var p Policy
	err := json.Unmarshal(data, &p)
	if err != nil {
		return p, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}

	return p, p.Validate()
}

// Commit is what evaluation needs to know about one commit of a set.
type Commit struct {
	ReceivedAt time.Time
	Partial    bool
	Pinned     bool
}

// Decision is the outcome for one commit: kept for the listed reasons, or
// retired.
type Decision struct {
	Keep    bool
	Reasons []string
}

// RetainedBy renders the reasons as the string stored on the commit row.
func (d Decision) RetainedBy() string {
	return strings.Join(d.Reasons, ",")
}

// Evaluate applies the policy to a set's commits and returns one decision
// per commit, in input order. The newest commit is always kept. A partial
// commit is kept only while it is the newest; it counts for no rule. A
// pinned commit is kept and counts like any other. Commits within
// KeepWithin of now are kept regardless of counts.
func Evaluate(commits []Commit, p Policy, now time.Time) []Decision {
	order := make([]int, len(commits))
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool {
		return commits[order[a]].ReceivedAt.After(commits[order[b]].ReceivedAt)
	})

	decisions := make([]Decision, len(commits))
	keep := func(i int, reason string) {
		decisions[i].Keep = true
		decisions[i].Reasons = append(decisions[i].Reasons, reason)
	}

	last := p.KeepLast
	if last < 1 {
		last = 1
	}

	bounds := p.bounds(now)

	// lastOf says whether a commit is the last complete one of its hour,
	// day, week or month; walking newest first, that is the first seen
	seen := map[Period]map[string]bool{}
	lastOf := make([]map[Period]bool, len(commits))
	for _, i := range order {
		if commits[i].Partial {
			continue
		}

		lastOf[i] = map[Period]bool{}
		for period, name := range periods {
			if seen[period] == nil {
				seen[period] = map[string]bool{}
			}

			if at := name(commits[i].ReceivedAt); !seen[period][at] {
				seen[period][at] = true
				lastOf[i][period] = true
			}
		}
	}

	for n, i := range order {
		c := commits[i]

		if n == 0 {
			keep(i, "latest")
		}

		if c.Pinned {
			keep(i, "pinned")
		}

		if c.Partial {
			continue
		}

		if last > 0 {
			keep(i, "last")
			last--
		}

		if p.KeepWithin > 0 && !c.ReceivedAt.Before(now.Add(-p.KeepWithin)) {
			keep(i, "within")
		}

		// a bracket also holds what a later, coarser one will keep, so a
		// month's last commit survives the weeks it ages through
		if b, ok := bracketAt(bounds, c.ReceivedAt); ok {
			for _, later := range p.Brackets[b:] {
				if lastOf[i][later.Period] {
					keep(i, string(later.Period))
					break
				}
			}
		}
	}

	return decisions
}
