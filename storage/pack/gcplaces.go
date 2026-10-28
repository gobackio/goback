package pack

import (
	"bytes"
	"cmp"
	"slices"
	"sort"

	"github.com/gobackio/goback/proto"
)

// places finds the records of a snapshot the mark may read by their refs,
// one copy of each, the first in archive order, in eight bytes a record.
type places struct {
	order []*gcArchive
	slots []slot
	// from is where the slots of each first two bytes of a ref begin
	from []uint32
}

// slot is a record of the snapshot: its archive's place in the order and
// its position in the archive's index.
type slot struct{ archive, pos uint32 }

func newPlaces(order []*gcArchive) *places {
	p := &places{order: order}

	n := 0
	for _, ga := range order {
		for pos := range ga.idx {
			if readable(ga.idx[pos].Type) {
				n++
			}
		}
	}

	p.slots = make([]slot, 0, n)
	for i, ga := range order {
		for pos := range ga.idx {
			if readable(ga.idx[pos].Type) {
				p.slots = append(p.slots, slot{uint32(i), uint32(pos)})
			}
		}
	}

	slices.SortFunc(p.slots, func(a, b slot) int {
		if c := bytes.Compare(p.sum(a), p.sum(b)); c != 0 {
			return c
		}

		return cmp.Or(cmp.Compare(a.archive, b.archive), cmp.Compare(a.pos, b.pos))
	})
	p.slots = slices.CompactFunc(p.slots, func(a, b slot) bool { return bytes.Equal(p.sum(a), p.sum(b)) })

	p.from = prefixIndex(len(p.slots), p.sumAt)

	return p
}

// prefixIndex is where each first two bytes begin among n sorted refs,
// the ith of which sum gives.
func prefixIndex(n int, sum func(i int) []byte) []uint32 {
	from := make([]uint32, 1<<16+1)
	at := 0
	for prefix := range 1 << 16 {
		for at < n && prefixOf16(sum(at)) < prefix {
			at++
		}

		from[prefix] = uint32(at)
	}
	from[1<<16] = uint32(n)

	return from
}

// search is where key sits among the sorted refs from indexes, false
// when it is none of them.
func search(from []uint32, sum func(i int) []byte, key refKey) (int, bool) {
	prefix := prefixOf16(key[:])
	lo, hi := int(from[prefix]), int(from[prefix+1])

	i := lo + sort.Search(hi-lo, func(i int) bool { return bytes.Compare(sum(lo+i), key[:]) >= 0 })

	return i, i < hi && bytes.Equal(sum(i), key[:])
}

// readable is whether the mark reads a record of the type: a blob is
// marked by what names it and a tombstone is never walked.
func readable(t uint32) bool {
	return proto.ObjectType(t) != proto.ObjectType_BLOB && proto.ObjectType(t) != proto.ObjectType_TOMBSTONE
}

func prefixOf16(sum []byte) int {
	return int(sum[0])<<8 | int(sum[1])
}

func (p *places) sum(s slot) []byte {
	return p.order[s.archive].idx[s.pos].Sum[:]
}

func (p *places) sumAt(i int) []byte {
	return p.sum(p.slots[i])
}

// count is how many refs the places hold.
func (p *places) count() int {
	return len(p.slots)
}

// rank is where the ref sits among the places, false when the snapshot
// holds no record of it the mark may read.
func (p *places) rank(key refKey) (int, bool) {
	return search(p.from, p.sumAt, key)
}

// at places the ref in the snapshot.
func (p *places) at(key refKey) (placed, bool) {
	i, ok := p.rank(key)
	if !ok {
		return placed{}, false
	}

	s := p.slots[i]
	ga := p.order[s.archive]

	return placed{key: key, a: ga.a, rec: &ga.idx[s.pos]}, true
}
