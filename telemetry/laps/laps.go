// Package laps times the consecutive steps of one operation for its log
// line.
package laps

import "time"

// Laps holds how long each step of an operation took. A nil *Laps times
// nothing, so a step can be lapped whether or not anyone asked for it.
type Laps struct {
	last  time.Time
	attrs []any
}

// Start begins timing the first step now.
func Start() *Laps {
	return &Laps{last: time.Now()}
}

// Lap ends the current step, naming it step, and begins the next.
func (l *Laps) Lap(step string) {
	if l == nil {
		return
	}

	now := time.Now()
	l.attrs = append(l.attrs, step, now.Sub(l.last).Round(time.Millisecond))
	l.last = now
}

// Attrs returns each step and its duration as alternating key-value
// arguments for slog.
func (l *Laps) Attrs() []any {
	if l == nil {
		return nil
	}

	return l.attrs
}
