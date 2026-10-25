// Package schedule is what an agent reports of how often each set it
// writes gets a snapshot, and when a store counts a set late by that.
package schedule

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The kinds of plan a set's snapshots follow: an interval, a cron
// expression, or a receiver of a database's log, which commits only while
// the database writes.
const (
	Every    = "every"
	Cron     = "cron"
	Receiver = "receiver"
)

// ErrInvalid is an Expectation no plan makes.
var ErrInvalid = errors.New("invalid schedule")

// Expectation is how often one set gets a snapshot by its agent's plan.
type Expectation struct {
	Set  string
	Kind string
	// LongestGap is the most time the plan allows between two snapshots;
	// zero for a Receiver.
	LongestGap time.Duration
	// Jitter is the window the plan's slots move by.
	Jitter time.Duration
	// CommitEvery is how often a Receiver commits while its database
	// writes.
	CommitEvery time.Duration
}

// Validate reports an Expectation without a set, of an unknown kind, with a
// negative duration, or without the gap or commit interval its kind takes,
// as ErrInvalid.
func (e Expectation) Validate() error {
	switch {
	case e.Set == "":
		return fmt.Errorf("%w: no set", ErrInvalid)
	case e.Kind != Every && e.Kind != Cron && e.Kind != Receiver:
		return fmt.Errorf("%w: set %q is of kind %q", ErrInvalid, e.Set, e.Kind)
	case e.LongestGap < 0 || e.Jitter < 0 || e.CommitEvery < 0:
		return fmt.Errorf("%w: set %q has a negative duration", ErrInvalid, e.Set)
	case e.Kind == Receiver && e.CommitEvery == 0:
		return fmt.Errorf("%w: receiver %q commits never", ErrInvalid, e.Set)
	case e.Kind != Receiver && e.LongestGap == 0:
		return fmt.Errorf("%w: set %q has no gap", ErrInvalid, e.Set)
	}

	return nil
}

// Report is what an agent plans: an Expectation per set it writes. It
// replaces what the agent reported before, so a set it no longer lists
// expects nothing.
type Report struct {
	Agent string
	Sets  []Expectation
}

// Reporter takes an agent's Report; one that cannot is
// backup.ErrNotImplemented.
type Reporter interface {
	ReportSchedule(ctx context.Context, r Report) error
}

// An agent reports every ReportEvery. It is silent once its report is
// SilentAfter overdue, and what it reported expires StaleAfter after its
// last report.
const (
	ReportEvery = 24 * time.Hour
	SilentAfter = 48 * time.Hour
	StaleAfter  = 30 * 24 * time.Hour
)

// Reported is the Expectation a store keeps for a set: the last one an
// agent reported, the agent, since when that agent reports the set and when
// it last did.
type Reported struct {
	Expectation
	Agent string
	Since time.Time
	At    time.Time
}

// Grace is how long past its longest gap a set may go without a snapshot:
// half the gap, and at least an hour.
func Grace(gap time.Duration) time.Duration {
	return max(time.Hour, gap/2)
}

// Silent reports whether r's agent is more than SilentAfter past its due
// report at now.
func (r Reported) Silent(now time.Time) bool {
	return now.Sub(r.At) > ReportEvery+SilentAfter
}

// Stale reports whether r expired at now.
func (r Reported) Stale(now time.Time) bool {
	return now.Sub(r.At) > StaleAfter
}

// Late reports whether the set is late at now, given when the store
// received its newest snapshot, zero for none: once its agent is Silent,
// whatever that snapshot, and otherwise, but for a Receiver, once its
// LongestGap, Jitter and Grace passed since that snapshot, or since r's
// agent first reported the set if that is later. A Stale r is never late.
func (r Reported) Late(newest, now time.Time) bool {
	switch {
	case r.Stale(now):
		return false
	case r.Silent(now):
		return true
	case r.Kind == Receiver || r.LongestGap <= 0:
		return false
	}

	from := newest
	if r.Since.After(from) {
		from = r.Since
	}

	return now.Sub(from) > r.LongestGap+r.Jitter+Grace(r.LongestGap)
}
