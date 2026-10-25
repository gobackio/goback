package agent

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	cron "github.com/netresearch/go-cron"
)

// Plan is when a task runs: at the multiples of an interval since the Unix
// epoch, or at the times of a cron expression read in a time zone.
type Plan struct {
	every time.Duration
	cron  cron.Schedule
	loc   *time.Location
	spec  string
}

// Every runs a task at every multiple of d since the Unix epoch, moved by
// the task's jitter offset.
func Every(d time.Duration) *Plan {
	return &Plan{every: d, spec: "every " + d.String()}
}

// cronShorthands are the descriptors Cron takes besides five fields.
var cronShorthands = map[string]bool{"@hourly": true, "@daily": true, "@weekly": true, "@monthly": true}

// Cron runs a task at the times a five-field cron expression, or @hourly,
// @daily, @weekly or @monthly, names in loc. A time a DST change skips
// runs once in the hour after the change; a repeated one runs once.
func Cron(expr string, loc *time.Location) (*Plan, error) {
	expr = strings.TrimSpace(expr)

	switch {
	case strings.HasPrefix(expr, "@") && !cronShorthands[expr]:
		return nil, fmt.Errorf("cron %q: the shorthands are @hourly, @daily, @weekly and @monthly; use every for an interval", expr)
	case strings.Contains(expr, "TZ="):
		return nil, fmt.Errorf("cron %q: schedules are read in the system's time zone, which a cron expression can't change", expr)
	case !strings.HasPrefix(expr, "@") && len(strings.Fields(expr)) != 5:
		return nil, fmt.Errorf("cron %q: takes five fields, minute hour day-of-month month day-of-week", expr)
	}

	s, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("cron %q: %w", expr, err)
	}

	p := &Plan{cron: s, loc: loc, spec: "cron " + expr}
	if p.next(time.Now()).IsZero() {
		return nil, fmt.Errorf("cron %q never runs", expr)
	}

	return p, nil
}

// String is the plan as the config says it.
func (p *Plan) String() string {
	return p.spec
}

// next is the first slot after t. A slot in the hour a fall-back repeats
// counts once, at its first occurrence.
func (p *Plan) next(t time.Time) time.Time {
	if p.cron == nil {
		every := int64(p.every)
		slot := t.UnixNano() / every
		if t.UnixNano() < 0 && t.UnixNano()%every != 0 {
			slot--
		}

		return time.Unix(0, (slot+1)*every).In(t.Location())
	}

	n := p.cron.Next(t.In(p.loc))
	if repeated(n) {
		n = p.cron.Next(n)
	}

	return n
}

// repeated reports whether the wall-clock time of t already occurred
// earlier that day, before a fall-back moved the clock back.
func repeated(t time.Time) bool {
	if t.IsZero() {
		return false
	}

	_, offset := t.Zone()
	_, before := t.Add(-2 * time.Hour).Zone()

	shift := time.Duration(before-offset) * time.Second
	if shift <= 0 {
		return false
	}

	twin := t.Add(-shift)

	return twin.Day() == t.Day() && twin.Hour() == t.Hour() && twin.Minute() == t.Minute() && twin.Second() == t.Second()
}

// Due is when a task with this plan next runs, its slots moved by offset,
// given when its last run started and the time now. A task that never ran,
// or missed a slot, however many, runs offset after now, or at its next
// slot when that comes first, so tasks whose offsets a jitter window spreads
// start apart; a last start ahead of now counts as now.
func (p *Plan) Due(offset time.Duration, last, now time.Time) time.Time {
	if !last.IsZero() {
		if last.After(now) {
			last = now
		}

		if next := p.next(last.Add(-offset)).Add(offset); !next.Before(now) {
			return next
		}
	}

	spread := now.Add(offset)
	if next := p.next(now.Add(-offset)).Add(offset); next.Before(spread) {
		return next
	}

	return spread
}

// LongestGap is the most time the plan leaves between two slots: its
// interval, or for cron the longest gap between two of its slots from the
// first after from through the year after from.
func (p *Plan) LongestGap(from time.Time) time.Duration {
	if p.cron == nil {
		return p.every
	}

	end := from.AddDate(1, 0, 0)

	var longest time.Duration

	for slot := p.next(from); !slot.IsZero() && !slot.After(end); {
		next := p.next(slot)
		if next.IsZero() {
			break
		}

		longest = max(longest, next.Sub(slot))
		slot = next
	}

	return longest
}

// staleAfter is how long after since, the task's last success, a task with
// this plan is stale without another: three intervals, or for cron until
// the third slot after since.
func (p *Plan) staleAfter(since time.Time) time.Duration {
	if p.cron == nil {
		return 3 * p.every
	}

	slot := since
	for range 3 {
		slot = p.next(slot)
	}

	return slot.Sub(since)
}

// JitterOffset is the offset in [0, window) every slot of the named task
// of agent moves by. It is the same on every start.
func JitterOffset(agent, task string, window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(agent))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(task))

	return time.Duration(h.Sum64() % uint64(window))
}

// LastRun is when the set's newest complete commit started, by its scan
// start, with the commit; zero and nil when the set has none.
func LastRun(ctx context.Context, index backup.Index, set string, now time.Time) (time.Time, *proto.Commit, error) {
	// a commit stamped ahead of this clock still counts, as now
	commits, err := index.CommitInfo(ctx, set, now.AddDate(100, 0, 0), 1)
	if err != nil || len(commits) == 0 {
		return time.Time{}, nil, err
	}

	c := commits[0]
	if c.GetScanStartNs() != 0 {
		return time.Unix(0, c.GetScanStartNs()), c, nil
	}

	return time.Unix(c.GetTimestamp(), 0), c, nil
}
