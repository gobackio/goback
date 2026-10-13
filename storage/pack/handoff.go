package pack

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/pkg/errors"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
)

// PlanExt names the sweeps collections published instead of running:
// <generation>.plan lists, per archive, the records the rewrite drops.
// While one is left, collections wait for RewritePlan to run it.
const PlanExt = ".plan"

type sweepPlan struct {
	Generation uint64        `json:"generation"`
	Now        time.Time     `json:"now"`
	MinAge     time.Duration `json:"minAge"`
	// Drop holds, per archive, the index positions the rewrite drops.
	Drop map[string]*bitset.BitSet `json:"drop"`
	// Classes are the output classes of the rewrite, and Classed per
	// archive the class of each index position.
	Classes []outputClass       `json:"classes,omitempty"`
	Classed map[string]*classed `json:"classed,omitempty"`
}

func planName(generation uint64) string {
	return fmt.Sprintf("%020d%s", generation, PlanExt)
}

// PendingPlans lists the generations whose published plan RewritePlan has
// not run yet, oldest first.
func (ps *PackStorage) PendingPlans() ([]uint64, error) {
	names, err := ps.storage.List(PlanExt)
	if err != nil {
		return nil, err
	}

	var generations []uint64
	for _, name := range names {
		generation, err := strconv.ParseUint(strings.TrimSuffix(name, PlanExt), 10, 64)
		if err != nil {
			continue
		}

		generations = append(generations, generation)
	}

	slices.Sort(generations)

	return generations, nil
}

// publish writes the sweep the run would do as a plan for RewritePlan.
func (r *gcRun) publish() (int, error) {
	plan := sweepPlan{Generation: r.gen, Now: r.opts.Now, MinAge: r.opts.MinAge, Drop: make(map[string]*bitset.BitSet),
		Classed: make(map[string]*classed)}
	if r.classes != nil {
		plan.Classes = r.classes.all
	}

	for _, ga := range r.order {
		if !r.selected(ga) {
			continue
		}

		drop := bitset.New(uint(ga.count))
		ga.each(func(pos int, rec *IndexRecord) {
			if r.droppable(ga, ga.next, pos, rec) {
				drop.Set(uint(pos))
			}
		})

		plan.Drop[ga.a.name] = drop
		plan.Classed[ga.a.name] = ga.classed
	}

	if len(plan.Drop) == 0 {
		return 0, nil
	}

	data, err := json.Marshal(plan)
	if err != nil {
		return 0, err
	}

	if err := r.ps.storage.CreateNew(planName(r.gen), data); err != nil {
		return 0, errors.Wrap(err, "publishing the sweep")
	}

	return len(plan.Drop), nil
}

// RewriteReport summarizes the plans one RewritePlan ran.
type RewriteReport struct {
	Plans            int
	Swept            int
	ReclaimedObjects uint64
	ReclaimedBytes   uint64
	CopiedBytes      uint64
}

// RewritePlan runs the sweeps collections published, oldest first, and
// deletes each plan once its archives are rewritten. A run that stops
// part way leaves the plan for the next one, which skips the archives
// already retired.
func (ps *PackStorage) RewritePlan(ctx context.Context) (*RewriteReport, error) {
	ps.compactorMtx.Lock()
	defer ps.compactorMtx.Unlock()

	generations, err := ps.PendingPlans()
	if err != nil {
		return nil, err
	}

	report := &RewriteReport{}

	for _, generation := range generations {
		plan, err := ps.readPlan(generation)
		if err != nil {
			return report, err
		}

		if err := ps.rewrite(ctx, plan, report); err != nil {
			return report, err
		}

		if err := ps.storage.Delete(planName(generation)); err != nil && !notExist(err) {
			return report, err
		}

		report.Plans++
	}

	return report, nil
}

func (ps *PackStorage) readPlan(generation uint64) (*sweepPlan, error) {
	file, err := ps.storage.Open(planName(generation))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var plan sweepPlan
	if err := json.NewDecoder(file).Decode(&plan); err != nil {
		return nil, fmt.Errorf("reading the plan of generation %d: %w", generation, err)
	}

	return &plan, nil
}

func (ps *PackStorage) rewrite(ctx context.Context, plan *sweepPlan, report *RewriteReport) error {
	marks := &planMarks{ps: ps, indexes: make(map[string]IndexFile), results: make(map[string]*gcFile)}
	group := &compactionGroup{marked: marks.marked, classes: newClasses(plan.Classes),
		classed: func(a *archive) *classed { return plan.Classed[a.name] }}

	ps.mtx.RLock()
	for _, a := range ps.archives {
		if plan.Drop[a.name] != nil {
			group.candidates = append(group.candidates, a)
			group.total += a.size
		}
	}
	ps.mtx.RUnlock()

	if len(group.candidates) == 0 {
		return nil
	}

	group.keep = func(candidate *archive, hdr *proto.ObjectHeader) bool {
		pos, err := marks.position(candidate.name, hdr.Ref.Hash)
		if err != nil {
			ps.logger.Warn("looking an object up in its index failed, keeping it", "archive", candidate.name, "ref", fmt.Sprintf("%x", hdr.Ref.Hash), "err", err)

			return true
		}

		if pos < 0 || !plan.Drop[candidate.name].Test(uint(pos)) {
			return true
		}

		return hdr.Timestamp != nil && plan.Now.Sub(hdr.Timestamp.AsTime()) < plan.MinAge
	}

	ps.logger.Info("gc rewriting a published plan", "generation", plan.Generation, "archives", len(group.candidates))

	group.progress = progress.Start(ctx, progress.OpRewrite, progress.PhaseRewrite, int64(len(group.candidates)), int64(group.total))
	if err := ps.compactGroup(ctx, group); err != nil {
		return err
	}
	group.progress.Finish()

	report.Swept += len(group.candidates)
	report.ReclaimedObjects += group.droppedObjects
	report.ReclaimedBytes += group.droppedBytes
	report.CopiedBytes += group.copiedBytes

	return nil
}

// planMarks answers a rewrite's questions about marks from the indexes
// and mark results in the storage.
type planMarks struct {
	ps *PackStorage

	mu      sync.Mutex
	indexes map[string]IndexFile
	results map[string]*gcFile
}

func (m *planMarks) position(name string, hash []byte) (int, error) {
	m.mu.Lock()
	idx, ok := m.indexes[name]
	m.mu.Unlock()

	if !ok {
		a, err := m.ps.archiveByName(name)
		if err != nil {
			return -1, err
		}

		if idx, err = a.getIndex(); err != nil {
			return -1, err
		}

		m.mu.Lock()
		m.indexes[name] = idx
		m.mu.Unlock()
	}

	return idx.position(hash), nil
}

// marked reports whether the copy at loc was reachable as its archive
// was last marked. A copy no mark covered counts as reachable.
func (m *planMarks) marked(loc *IndexLocation) bool {
	m.mu.Lock()
	result, ok := m.results[loc.Archive]
	m.mu.Unlock()

	if !ok {
		var err error
		if result, err = readGCFile(m.ps.storage, loc.Archive); err != nil {
			m.ps.logger.Warn("reading a mark result failed, treating the copy as reachable", "archive", loc.Archive, "err", err)

			return true
		}

		m.mu.Lock()
		m.results[loc.Archive] = result
		m.mu.Unlock()
	}

	if result == nil {
		return true
	}

	pos, err := m.position(loc.Archive, loc.Record.Sum[:])
	if err != nil {
		m.ps.logger.Warn("looking an object up in its index failed, treating it as reachable", "archive", loc.Archive, "err", err)

		return true
	}

	return pos < 0 || result.Current.Test(uint(pos))
}
