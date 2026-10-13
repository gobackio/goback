package pack

import (
	"slices"
	"time"
)

// span is the length of the calendar period an age class covers.
type span int

const (
	spanDay span = iota
	spanWeek
	spanMonth
	spanYear
)

// ageClass is the calendar period an object's stored time falls in.
type ageClass struct {
	Span  span  `json:"span"`
	Start int64 `json:"start"`
}

// classOf is the period of t seen from now: its day within the last week,
// its week within the last five, its month within the last year and its
// year before that.
func classOf(t, now time.Time) ageClass {
	t, now = t.UTC(), now.UTC()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	if !day.Before(today.AddDate(0, 0, -6)) {
		return ageClass{spanDay, day.Unix()}
	}

	if week := monday(day); !week.Before(monday(today).AddDate(0, 0, -7*4)) {
		return ageClass{spanWeek, week.Unix()}
	}

	month := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	if !month.Before(time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -11, 0)) {
		return ageClass{spanMonth, month.Unix()}
	}

	return ageClass{spanYear, time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC).Unix()}
}

func monday(day time.Time) time.Time {
	return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
}

// newer orders classes newest first.
func (c ageClass) newer(o ageClass) bool {
	if c.Start != o.Start {
		return c.Start > o.Start
	}

	return c.Span < o.Span
}

// outputClass is what a rewrite writes into one output: the objects of
// one owner stored in one period. A zero Set with a Group is what several
// sets of that group share; a zero Attribution is what the mark
// attributed to nobody.
type outputClass struct {
	Owner Attribution `json:"owner"`
	Age   ageClass    `json:"age"`
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
// into. A class holding less than small joins its owner's next older one;
// the oldest, if that leaves it small, joins the newer one it would have
// followed.
func (c *classes) fold(sizes map[int32]uint64, small uint64) map[int32]int32 {
	into := make(map[int32]int32, len(sizes))

	byOwner := make(map[Attribution][]int32)
	for id := range sizes {
		owner := c.all[id].Owner
		byOwner[owner] = append(byOwner[owner], id)
	}

	for _, ids := range byOwner {
		slices.SortFunc(ids, func(a, b int32) int {
			if c.all[a].Age.newer(c.all[b].Age) {
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
// class it is written with.
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

	err := r.scanOf(selected, live, func(ga *gcArchive, pos int, _ *IndexRecord, owners []Attribution, _ uint64) {
		ga.owners[pos] = ownerOf(owners)
	}, nil)
	if err != nil {
		return err
	}

	r.classes = newClasses(nil)

	for _, ga := range selected {
		ga.classed = &classed{Class: make([]int32, ga.count), Bytes: make(map[int32]uint64)}

		ga.each(func(pos int, rec *IndexRecord) {
			id := r.classes.id(outputClass{ga.owners[pos], classOf(ga.a.version(*rec).Time, r.opts.Now)})
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
