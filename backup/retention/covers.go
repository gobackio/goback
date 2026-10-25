package retention

import "errors"

// ErrShortens is returned for a change to a policy that keeps less than
// the one in force.
var ErrShortens = errors.New("the policy keeps less than the one in force")

// Covers reports whether p keeps every commit q keeps, for any commits and
// at any time: when p keeps everything, as KeepAll does, or when p keeps
// at least as many of the newest commits and for at least as long as q,
// and holds q's brackets in q's order, each by the same period and at
// least as many periods, a tail only by a tail. Brackets of its own may
// come before, between and after them. Any other p, even one that would
// keep as much, does not cover q.
func (p Policy) Covers(q Policy) bool {
	if p.KeepWithin >= KeepAll.KeepWithin {
		return true
	}

	if max(p.KeepLast, 1) < max(q.KeepLast, 1) || max(p.KeepWithin, 0) < max(q.KeepWithin, 0) {
		return false
	}

	i := 0

	for _, held := range q.Brackets {
		for i < len(p.Brackets) && !p.Brackets[i].holds(held) {
			i++
		}

		if i == len(p.Brackets) {
			return false
		}

		i++
	}

	return true
}

// holds reports whether b keeps by other's period for at least as many
// periods; a tail holds any count.
func (b Bracket) holds(other Bracket) bool {
	return b.Period == other.Period && (b.Count == 0 || (other.Count != 0 && b.Count >= other.Count))
}
