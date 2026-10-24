package pack

import (
	"slices"
	"time"

	"github.com/gobackio/goback/proto"
)

// DeathHorizons are how far past a collection the death classes its
// rewrites file objects by end, soonest first. An object is expected to
// die when the last root that reaches it does; one expected to outlive
// the last horizon, or never to die, is filed with the others that do.
var DeathHorizons = []time.Duration{7 * 24 * time.Hour, 31 * 24 * time.Hour, 92 * 24 * time.Hour, 366 * 24 * time.Hour}

// deathClass is how soon an object is expected to die: never, the zero
// class, or class n for within DeathHorizons[len(DeathHorizons)-n], so a
// smaller class dies later.
type deathClass uint8

const never deathClass = 0

// deathOf is the class of an object expected to die at death, seen from
// now; a zero death is never.
func deathOf(death, now time.Time) deathClass {
	if death.IsZero() {
		return never
	}

	for i, horizon := range DeathHorizons {
		if !death.After(now.Add(horizon)) {
			return deathClass(len(DeathHorizons) - i)
		}
	}

	return never
}

// later reports whether d dies after o.
func (d deathClass) later(o deathClass) bool {
	return d < o
}

// outputClass is what a rewrite writes into one output: the blobs, or the
// other objects, of one owner expected to die about the same time. A zero
// Set with a Group is what several sets of that group share; a zero
// Attribution is what the mark attributed to nobody.
type outputClass struct {
	Owner Attribution `json:"owner"`
	Data  bool        `json:"data,omitempty"`
	Death deathClass  `json:"death,omitempty"`
}

// isData reports whether a record of type t goes into a data archive,
// which holds blobs alone, rather than a metadata one.
func isData(t uint32) bool {
	return proto.ObjectType(t) == proto.ObjectType_BLOB
}

// classes numbers the output classes of a sweep.
type classes struct {
	ids map[outputClass]int32
	all []outputClass
}

func newClasses(all []outputClass) *classes {
	c := &classes{ids: make(map[outputClass]int32, len(all)), all: all}
	for id, class := range all {
		c.ids[class] = int32(id)
	}

	return c
}

func (c *classes) id(class outputClass) int32 {
	id, ok := c.ids[class]
	if !ok {
		id = int32(len(c.all))
		c.ids[class] = id
		c.all = append(c.all, class)
	}

	return id
}

// fold maps every class sizes holds to the one its objects are written
// into. A class holding less than small joins the next one of its owner
// and kind to die later; the last to die, if that leaves it small, joins
// the one it would have followed.
func (c *classes) fold(sizes map[int32]uint64, small uint64) map[int32]int32 {
	into := make(map[int32]int32, len(sizes))

	type ownerKind struct {
		owner Attribution
		data  bool
	}

	byOwner := make(map[ownerKind][]int32)
	for id := range sizes {
		owner := ownerKind{c.all[id].Owner, c.all[id].Data}
		byOwner[owner] = append(byOwner[owner], id)
	}

	for _, ids := range byOwner {
		slices.SortFunc(ids, func(a, b int32) int {
			if c.all[b].Death.later(c.all[a].Death) {
				return -1
			}

			return 1
		})

		var carried []int32
		var held uint64
		last := int32(-1)

		for _, id := range ids {
			carried = append(carried, id)
			held += sizes[id]

			if held < small {
				continue
			}

			for _, from := range carried {
				into[from] = id
			}

			carried, held, last = nil, 0, id
		}

		target := last
		if target < 0 {
			target = ids[len(ids)-1]
		}

		for _, from := range carried {
			into[from] = target
		}
	}

	return into
}

// outputs counts the classes fold leaves of sizes, each written into an
// output of its own.
func (c *classes) outputs(sizes map[int32]uint64, small uint64) int {
	written := make(map[int32]bool)
	for _, id := range c.fold(sizes, small) {
		written[id] = true
	}

	return len(written)
}

// classed is the output class of every record of one archive a sweep
// rewrites, and what of each class survives in it.
type classed struct {
	Class []int32          `json:"class"`
	Bytes map[int32]uint64 `json:"bytes"`
}

// chunkClasses folds the classes of what a chunk of a rewrite holds, so
// each output it writes reaches small unless its owner holds less in the
// chunk, and returns the class of each record.
func chunkClasses(c *classes, small uint64, chunk []*archive, of func(a *archive) *classed) func(candidate *archive, pos int) int32 {
	sizes := make(map[int32]uint64)
	for _, a := range chunk {
		if cl := of(a); cl != nil {
			for id, n := range cl.Bytes {
				sizes[id] += n
			}
		}
	}

	into := c.fold(sizes, small)

	return func(candidate *archive, pos int) int32 {
		cl := of(candidate)
		if cl == nil || pos < 0 || pos >= len(cl.Class) {
			return -1
		}

		return into[cl.Class[pos]]
	}
}

// ownerOf is whom a rewrite files an object under: the set of the first
// group that reached it when that set alone did, the group when several
// of its sets did, nobody when none did.
func ownerOf(owners []Attribution) Attribution {
	var owner Attribution
	found := false

	for _, o := range owners {
		switch {
		case o.Set == 0:
		case !found:
			owner, found = o, true
		case o.Group != owner.Group:
			return owner
		case o.Set != owner.Set:
			owner.Set = 0
		}
	}

	return owner
}

// classify gives every record of the archives a sweep rewrites the output
// class it is written with: by the owner the mark attributed it to, its
// kind and how soon the mark expects it to die.
func (r *gcRun) classify(live *liveRuns) error {
	var selected []*gcArchive
	for _, ga := range r.order {
		if r.selected(ga) {
			ga.owners = make([]Attribution, ga.count)
			selected = append(selected, ga)
		}
	}

	if len(selected) == 0 {
		return nil
	}

	err := r.scanOf(selected, live, func(ga *gcArchive, pos int, _ *IndexRecord, found reach) {
		ga.owners[pos] = ownerOf(found.owners)
	}, nil)
	if err != nil {
		return err
	}

	r.classes = newClasses(nil)

	for _, ga := range selected {
		ga.classed = &classed{Class: make([]int32, ga.count), Bytes: make(map[int32]uint64)}

		ga.each(func(pos int, rec *IndexRecord) {
			// what the mark left unreachable goes once the next one does too
			death := deathClass(len(DeathHorizons))
			if ga.cur.Test(uint(pos)) {
				death = ga.next.death(pos)
			}

			id := r.classes.id(outputClass{ga.owners[pos], isData(rec.Type), death})
			ga.classed.Class[pos] = id

			if !r.droppable(ga, ga.next, pos, rec) {
				ga.classed.Bytes[id] += uint64(rec.Length)
			}
		})

		ga.owners = nil
	}

	return nil
}

// classedOf is the classes the sweep gives the records of a.
func (r *gcRun) classedOf(a *archive) *classed {
	if ga := r.archives[a.name]; ga != nil {
		return ga.classed
	}

	return nil
}
