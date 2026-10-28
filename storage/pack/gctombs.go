package pack

import (
	"bytes"
	"slices"
)

// stamp is a Version in sixteen bytes.
type stamp struct {
	ns  int64
	off uint32
}

func stampOf(v Version) stamp {
	return stamp{v.Time.UnixNano(), v.Offset}
}

func (s stamp) before(o stamp) bool {
	if s.ns != o.ns {
		return s.ns < o.ns
	}

	return s.off < o.off
}

// The flags of a tombstone target.
const (
	// targetHeld is set once the mark reached the target.
	targetHeld uint8 = 1 << iota
	// targetCondemning is set when a tombstone the previous generation's
	// horizon covers names the target.
	targetCondemning
	// targetCopied is set when the snapshot holds a copy of the target.
	targetCopied
	// targetBlob is set when the target is a blob.
	targetBlob
)

// tombTargets holds what a run knows of each tombstone target of its
// snapshot, by the target's rank among them.
type tombTargets struct {
	keys []refKey
	from []uint32
	// newest is the version of the target's newest tombstone, condemning
	// that of the newest the previous generation's horizon covers, and
	// oldest that of its oldest copy.
	newest, condemning, oldest []stamp
	flags                      []uint8
}

// newTombTargets holds the targets of keys, which it sorts.
func newTombTargets(keys []refKey) *tombTargets {
	slices.SortFunc(keys, func(a, b refKey) int { return bytes.Compare(a[:], b[:]) })
	keys = slices.Clip(slices.Compact(keys))

	t := &tombTargets{keys: keys, newest: make([]stamp, len(keys)), condemning: make([]stamp, len(keys)),
		oldest: make([]stamp, len(keys)), flags: make([]uint8, len(keys))}
	t.from = prefixIndex(len(keys), t.sum)

	return t
}

func (t *tombTargets) sum(i int) []byte {
	return t.keys[i][:]
}

// rank is where key sits among the targets, false when it is none.
func (t *tombTargets) rank(key refKey) (int, bool) {
	return search(t.from, t.sum, key)
}

// is reports whether key is a target with the flag set.
func (t *tombTargets) is(key refKey, flag uint8) bool {
	i, ok := t.rank(key)

	return ok && t.flags[i]&flag != 0
}

func (t *tombTargets) tombstoned(at int, v stamp, condemning bool) {
	if t.newest[at] == (stamp{}) || t.newest[at].before(v) {
		t.newest[at] = v
	}

	if condemning && (t.flags[at]&targetCondemning == 0 || t.condemning[at].before(v)) {
		t.condemning[at], t.flags[at] = v, t.flags[at]|targetCondemning
	}
}

func (t *tombTargets) copied(at int, v stamp) {
	if t.flags[at]&targetCopied == 0 || v.before(t.oldest[at]) {
		t.oldest[at], t.flags[at] = v, t.flags[at]|targetCopied
	}
}

// condemningOnly keeps only the targets a covered tombstone names, with
// their condemning versions, for the sweep.
func (t *tombTargets) condemningOnly() *tombTargets {
	n := 0
	for _, f := range t.flags {
		if f&targetCondemning != 0 {
			n++
		}
	}

	kept := &tombTargets{keys: make([]refKey, 0, n), condemning: make([]stamp, 0, n), flags: make([]uint8, 0, n)}
	for i, f := range t.flags {
		if f&targetCondemning != 0 {
			kept.keys = append(kept.keys, t.keys[i])
			kept.condemning = append(kept.condemning, t.condemning[i])
			kept.flags = append(kept.flags, targetCondemning)
		}
	}
	kept.from = prefixIndex(n, kept.sum)

	return kept
}

// tombsByOwn finds the snapshot's tombstones by their own refs.
type tombsByOwn struct {
	tombs []gcTombstone
	at    []uint32
	from  []uint32
}

func newTombsByOwn(tombs []gcTombstone) *tombsByOwn {
	b := &tombsByOwn{tombs: tombs, at: make([]uint32, len(tombs))}
	for i := range b.at {
		b.at[i] = uint32(i)
	}

	slices.SortFunc(b.at, func(x, y uint32) int { return bytes.Compare(b.own(x), b.own(y)) })
	b.from = prefixIndex(len(b.at), b.sum)

	return b
}

func (b *tombsByOwn) own(i uint32) []byte {
	t := b.tombs[i]

	return t.ga.idx[t.pos].Sum[:]
}

func (b *tombsByOwn) sum(i int) []byte {
	return b.own(b.at[i])
}

// target is the rank of the target of the tombstone whose ref is key,
// false when no tombstone of the snapshot has that ref.
func (b *tombsByOwn) target(key refKey) (uint32, bool) {
	i, ok := search(b.from, b.sum, key)
	if !ok {
		return 0, false
	}

	return b.tombs[b.at[i]].target, true
}
