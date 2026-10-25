package pack

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/bits-and-blooms/bitset"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// CollectOptions tunes one garbage collection run. Zero values take the
// defaults documented on each field.
type CollectOptions struct {
	// Readers is the number of parallel object reads during the mark (32).
	Readers int
	// DeadRatio selects an archive for sweeping once this share of its bytes
	// is dead (0.3).
	DeadRatio float64
	// ErasureBound selects an archive that has held dead objects this long
	// regardless of ratio (14 days).
	ErasureBound time.Duration
	// MinAge keeps every object younger than this, whatever the mark says (24h).
	MinAge time.Duration
	// NoSweep stops after the mark.
	NoSweep bool
	// Behind says the store stands in front of another, so a reachable
	// object it lacks is passed over without a warning.
	Behind bool
	// Handoff publishes the sweep as a plan for RewritePlan instead of
	// running it.
	Handoff bool
	// Quarantine is how long the files of an archive a rewrite retired
	// are kept before a collection deletes them (DefaultQuarantine).
	Quarantine time.Duration
	// TempDir holds the live-set runs and the checkpoints a crashed run
	// resumes from (os.TempDir()).
	TempDir string
	// Now is the snapshot time (time.Now()).
	Now time.Time
	// Owner names what a root belongs to, so the mark can attribute the
	// objects it reaches; nil, or a zero Attribution, attributes nothing.
	Owner func(root []byte) Attribution
	// Shallow names the roots that keep their trees and files but not the
	// parts of those files, which go unless another root reaches them; nil
	// keeps every root whole.
	Shallow func(root []byte) bool
	// Death is when a root is expected to stop being one, zero for never,
	// so a sweep writes what dies about the same time together; nil
	// expects no root to die.
	Death func(root []byte) time.Time
}

// Attribution is what a root belongs to: the group its objects are
// counted in and the set they are recorded against. An object is counted
// once per group, so two groups that both hold it each carry it in full,
// and within a group it goes to the first set the mark reached it from.
type Attribution struct {
	Group int64
	Set   int64
}

func (a Attribution) before(b Attribution) bool {
	if a.Group != b.Group {
		return a.Group < b.Group
	}

	return a.Set < b.Set
}

const (
	defaultGCReaders      = 32
	defaultGCDeadRatio    = 0.3
	defaultGCErasureBound = 14 * 24 * time.Hour
	defaultGCMinAge       = 24 * time.Hour
	liveRunLimit          = 1 << 20
)

// rootBatch is the number of roots marked between two checkpoints.
var rootBatch = 1024

// gcAfterBatch, when set, runs after every checkpoint; tests use it to
// interrupt a mark.
var gcAfterBatch func(batch int) error

func (o CollectOptions) withDefaults() CollectOptions {
	if o.Readers <= 0 {
		o.Readers = defaultGCReaders
	}
	if o.DeadRatio <= 0 {
		o.DeadRatio = defaultGCDeadRatio
	}
	if o.ErasureBound <= 0 {
		o.ErasureBound = defaultGCErasureBound
	}
	if o.MinAge <= 0 {
		o.MinAge = defaultGCMinAge
	}
	if o.Quarantine <= 0 {
		o.Quarantine = DefaultQuarantine
	}
	if o.TempDir == "" {
		o.TempDir = os.TempDir()
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}

	return o
}

// CollectReport summarizes one garbage collection run.
type CollectReport struct {
	Generation  uint64
	Archives    int
	Objects     uint64
	Roots       int
	Marked      uint64
	DeadObjects uint64
	DeadBytes   uint64
	// Resumed counts the root batches taken from a previous, interrupted run.
	Resumed int
	// ErasedArchives counts the archives flagged for holding the objects of
	// an erased commit.
	ErasedArchives int
	// SetBytes is what each set's live objects take up in the archives,
	// keyed by the set ids Owner returned. Within a group an object counts
	// once, for the first set the mark reached it from; across groups it
	// counts in each.
	SetBytes map[int64]uint64
	// SetDeduplicated is the uncompressed size of the distinct file content
	// each set's live objects carry, attributed as SetBytes is.
	SetDeduplicated map[int64]uint64
	// SetAlone is what each set's live objects would take up were it the
	// only set of its group, and SetExclusive what of that no other set of
	// the group reaches: what deleting the set would free.
	SetAlone, SetExclusive map[int64]uint64
	// SetDeduplicatedAlone is the uncompressed size of the distinct file
	// content each set's live objects carry were it the only set of its group.
	SetDeduplicatedAlone map[int64]uint64
	// Unattributed is what the live objects no set reaches take up, such
	// as those only a commit without an owner reaches; zero without Owner.
	Unattributed uint64
	// Condemned counts the unreachable objects the run stored tombstones for.
	Condemned int
	// Waiting is the generation whose published plan has not been
	// rewritten yet; a run that waits does nothing else.
	Waiting uint64
	// Busy is set when another process maintains the store; a run that
	// finds it busy does nothing else.
	Busy bool
	// SweepSkipped names the reason when the run marked but did not sweep.
	SweepSkipped string
	// Published counts the archives of the plan the run published.
	Published        int
	Swept            int
	ReclaimedObjects uint64
	ReclaimedBytes   uint64
	// CopiedBytes is what the sweep wrote again to reclaim that.
	CopiedBytes uint64
	// Purged counts the quarantined files the run deleted.
	Purged int
	// ArchiveBytes is what the archives the run marked take up in storage,
	// dead objects included, and RetiredBytes what the archives rewrites
	// retired still take up until they are purged.
	ArchiveBytes, RetiredBytes uint64
	// OldestDead is when the archive that has held dead objects longest
	// without being rewritten first held them; zero when none holds any.
	OldestDead time.Time
	Duration   time.Duration
}

// Summary renders the report as a few lines for a log or an operator.
func (r *CollectReport) Summary() string {
	if r.Waiting > 0 {
		return fmt.Sprintf("GC waits for the rewrite of generation %d", r.Waiting)
	}

	if r.Busy {
		return "GC left to another process maintaining the store"
	}

	summary := fmt.Sprintf("GC generation %d: %d roots, %d of %d objects in %d archives marked, %d objects (%s) dead, %s",
		r.Generation, r.Roots, r.Marked, r.Objects, r.Archives, r.DeadObjects, humanize.Bytes(r.DeadBytes), r.Duration.Round(time.Millisecond))

	if r.SweepSkipped != "" {
		return summary + "\nGC sweep skipped: " + r.SweepSkipped
	}

	return summary + fmt.Sprintf("\nGC published %d archives, swept %d (%d flagged for erasure), reclaimed %d objects (%s), copied %s",
		r.Published, r.Swept, r.ErasedArchives, r.ReclaimedObjects, humanize.Bytes(r.ReclaimedBytes), humanize.Bytes(r.CopiedBytes))
}

// Collector is implemented by stores that can garbage collect themselves.
type Collector interface {
	Collect(ctx context.Context, opts CollectOptions) (*CollectReport, error)
}

var _ Collector = (*PackStorage)(nil)

type gcArchive struct {
	erase bool
	a     *archive
	// idx is the archive's index, count how many records it holds, and
	// droppable how many bytes of it this generation may drop.
	idx       IndexFile
	count     int
	droppable uint64
	bytes     uint64
	prev      *gcFile
	cur       *bitset.BitSet
	next      *gcFile
	// owners is whom the mark attributed each record to, death how soon
	// it expects each to die, and classed what a sweep writes each with
	owners  []Attribution
	death   deaths
	classed *classed
}

type gcTombstone struct {
	ga      *gcArchive
	pos     int
	target  refKey
	version Version
}

// each calls fn with every record of the archive's index in stored order.
func (ga *gcArchive) each(fn func(pos int, rec *IndexRecord)) {
	for pos := range ga.idx {
		fn(pos, &ga.idx[pos])
	}
}

// gcReads keeps what a run read of committed archives, which never change,
// so the check before a sweep reads only the archives that appeared since.
type gcReads struct {
	indexes map[string]IndexFile
	// tombs is what each archive's tombstones name, by index position
	tombs map[string]map[int]tombHeader
}

func newGCReads() *gcReads {
	return &gcReads{indexes: make(map[string]IndexFile), tombs: make(map[string]map[int]tombHeader)}
}

// tombHeader is what one tombstone's header says; named is false for a
// tombstone that names no target.
type tombHeader struct {
	target refKey
	named  bool
	erase  bool
}

func tombHeaderOf(hdr *proto.ObjectHeader) tombHeader {
	if hdr.TombstoneFor == nil {
		return tombHeader{}
	}

	return tombHeader{target: keyOf(hdr.TombstoneFor.Hash), named: true, erase: hdr.Erase}
}

type gcRun struct {
	ps       *PackStorage
	opts     CollectOptions
	gen      uint64
	prev     *gcState
	snapshot time.Time
	reads    *gcReads

	archives map[string]*gcArchive
	order    []*gcArchive
	// located places every record of the snapshot the mark may read, one
	// copy each
	located map[refKey]placed
	// pending are the finalized archives of live sessions that have not
	// committed, read for the un-tombstones a committing session wrote;
	// pendingAt places their objects, so the mark walks from a commit a
	// session un-tombstoned before committing it
	pending   []*archive
	pendingAt map[refKey]placed
	erased    []refKey
	runDir    string
	resumed   int
	// check is whether the run is the confirmation of another, whose
	// mark it reports as PhaseConfirm
	check bool
	// lost counts the reachable objects the mark found missing, which
	// leave what only they reach unmarked
	lost atomic.Int64

	roots      []gcRoot
	tombstones []gcTombstone
	// targets maps every tombstone target to whether the snapshot still holds it.
	targets map[refKey]bool
	// newestTomb is the version of the newest tombstone of each target in
	// the snapshot, and condemning that of the newest one the previous
	// generation's horizon covers.
	newestTomb  map[refKey]Version
	condemning  map[refKey]Version
	condemnedAt map[int64]bool
	// condemned maps what this generation condemns to its newest copy.
	condemned map[refKey]Version
	// tombTimes are the versions the snapshot's tombstones carry.
	tombTimes map[int64]bool
	// untombed are the objects an un-tombstone takes back, roots of the
	// mark; oldestCopy is the version of the oldest copy of each tombstone
	// target, and spent the tombstones with nothing left to condemn.
	untombed   map[refKey]bool
	oldestCopy map[refKey]Version
	spent      map[recordAt]bool
	// blobs are the tombstone targets and pending objects that are blobs,
	// which the mark adds from a root without reading them
	blobs map[refKey]bool
	// horizonEnded is whether every session of the previous horizon has
	// ended.
	horizonEnded bool
	// setBytes and setDeduplicated are what the mark attributed to each set.
	setBytes        map[int64]uint64
	setDeduplicated map[int64]uint64
	// setAlone and setExclusive are what each set reaches, and what only
	// it reaches within its group.
	setAlone, setExclusive map[int64]uint64
	setDeduplicatedAlone   map[int64]uint64
	unattributed           uint64
	// classes are the output classes of the sweep
	classes *classes
}

// newGCRun prepares the run of the generation after prev, keeping what it
// reads of committed archives in reads.
func newGCRun(ps *PackStorage, opts CollectOptions, prev *gcState, reads *gcReads) *gcRun {
	run := &gcRun{ps: ps, opts: opts, prev: prev, gen: 1, snapshot: opts.Now.UTC(), reads: reads,
		archives: make(map[string]*gcArchive), targets: make(map[refKey]bool),
		newestTomb: make(map[refKey]Version), condemning: make(map[refKey]Version),
		condemned: make(map[refKey]Version), condemnedAt: make(map[int64]bool), tombTimes: make(map[int64]bool),
		untombed: make(map[refKey]bool), oldestCopy: make(map[refKey]Version), spent: make(map[recordAt]bool),
		blobs:    make(map[refKey]bool),
		setBytes: make(map[int64]uint64), setDeduplicated: make(map[int64]uint64),
		setAlone: make(map[int64]uint64), setExclusive: make(map[int64]uint64),
		setDeduplicatedAlone: make(map[int64]uint64)}
	if prev != nil {
		run.gen = prev.Generation + 1

		for _, t := range prev.Condemned {
			run.condemnedAt[t.UnixNano()] = true
		}
	}

	return run
}

// gcRoot is a root with what it belongs to, zero when nothing names it,
// and how soon it is expected to die.
type gcRoot struct {
	key     refKey
	owner   Attribution
	shallow bool
	death   deathClass
}

// Collect marks every object reachable from a live commit or pin, records
// the result beside each archive and rewrites archives whose dead share
// justifies it. An unreachable object gets a tombstone, and the next
// generation drops it if it is still unreachable and no session that
// relied on it took the tombstone back as it committed. While another
// process maintains the store it does nothing and sets Busy. The context
// must not carry a session.
func (ps *PackStorage) Collect(ctx context.Context, opts CollectOptions) (*CollectReport, error) {
	if _, ok := backup.SessionFromContext(ctx); ok {
		return nil, errors.New("garbage collection runs outside a session")
	}

	opts = opts.withDefaults()
	started := time.Now()

	ctx, span := tracer.Start(ctx, "PackStorage.Collect")
	defer span.End()

	ps.maintenanceMtx.Lock()
	defer ps.maintenanceMtx.Unlock()

	lease, ctx, err := ps.leaseMaintenance(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "leasing the maintenance")
	}

	if lease == nil {
		return &CollectReport{Busy: true, Duration: time.Since(started)}, nil
	}
	defer lease.release()

	waiting, err := ps.waitingPlan()
	if err != nil {
		return nil, err
	}

	if waiting > 0 {
		return &CollectReport{Waiting: waiting, Duration: time.Since(started)}, nil
	}

	prev, err := loadGCState(ps.storage)
	if err != nil {
		return nil, errors.Wrap(err, "loading gc state")
	}

	run := newGCRun(ps, opts, prev, newGCReads())
	if prev == nil {
		floor, err := ps.newestGeneration()
		if err != nil {
			return nil, errors.Wrap(err, "finding the newest generation")
		}

		run.gen = floor + 1
	}

	report := &CollectReport{Generation: run.gen}

	sessions, err := run.liveSessions()
	if err != nil {
		return nil, errors.Wrap(err, "listing sessions")
	}

	if err := ps.pruneSeals(sessions); err != nil {
		return nil, errors.Wrap(err, "pruning seals")
	}

	if prev != nil {
		run.horizonEnded = true
		for _, id := range prev.Horizon {
			run.horizonEnded = run.horizonEnded && !sessions[id]
		}
	}

	if err := run.takeSnapshot(ctx); err != nil {
		return nil, err
	}

	report.Archives = len(run.order)
	for _, ga := range run.order {
		report.Objects += uint64(ga.count)
	}

	phase := progress.Start(ctx, progress.OpCollect, progress.PhaseRoots, 0, 0)
	if err := run.collectRoots(ctx); err != nil {
		return nil, err
	}
	report.Roots = len(run.roots)
	phase.Finish()

	ps.logger.Info("gc took a snapshot", "generation", run.gen, "archives", report.Archives, "objects", report.Objects,
		"roots", report.Roots, "untombed", len(run.untombed), "took", time.Since(started).Round(time.Millisecond))

	markStart := time.Now()
	live, err := run.mark(ctx)
	if err != nil {
		return nil, err
	}

	// the checkpoints describe runs close deletes, so they go with them
	defer func() {
		live.close()
		_ = os.RemoveAll(run.runDir)
	}()

	report.Resumed = run.resumed
	markTook := time.Since(markStart)
	gcMarkDuration.Record(ctx, markTook.Seconds())

	mergeStart := time.Now()
	phase = progress.Start(ctx, progress.OpCollect, progress.PhaseMerge, 0, 0)
	report.SetBytes = run.setBytes
	report.SetDeduplicated = run.setDeduplicated
	report.SetAlone, report.SetExclusive = run.setAlone, run.setExclusive
	report.SetDeduplicatedAlone = run.setDeduplicatedAlone
	report.Marked, err = run.merge(live)
	if err != nil {
		return nil, err
	}
	report.Unattributed = run.unattributed

	if err := run.flagErased(ctx); err != nil {
		return nil, err
	}

	// nothing after the erasure walk places an object, and the check before
	// a sweep places them anew
	run.located, run.pendingAt = nil, nil

	if err := run.writeResults(); err != nil {
		return nil, err
	}

	if err := run.classify(live); err != nil {
		return nil, err
	}

	for _, ga := range run.order {
		if ga.next.Erase {
			report.ErasedArchives++
		}
	}
	mergeTook := time.Since(mergeStart)
	phase.Finish()
	gcMergeDuration.Record(ctx, mergeTook.Seconds())

	live.close()
	_ = os.RemoveAll(run.runDir)

	for _, ga := range run.order {
		report.DeadObjects += ga.next.DeadObjects
		report.DeadBytes += ga.next.DeadBytes
		report.ArchiveBytes += ga.a.size
	}
	gcDeadBytes.Record(ctx, int64(report.DeadBytes))

	if report.Unattributed*100 > report.ArchiveBytes {
		ps.logger.Warn("gc reached more than 1% of the archives from no set", "generation", run.gen,
			"unattributed", humanize.Bytes(report.Unattributed), "archives", humanize.Bytes(report.ArchiveBytes))
	}

	// what only a missing object reaches went unmarked, so nothing the
	// mark left unreachable is condemned
	if run.lost.Load() == 0 {
		if err := run.condemn(ctx); err != nil {
			return nil, err
		}
		report.Condemned = len(run.condemned)
	}

	condemned, horizon, err := run.horizon()
	if err != nil {
		return nil, err
	}

	// a session that commits from here on copies what these condemn under
	// what it relied on, or wrote its un-tombstones before this
	if err := ps.writeSeal(seal{Generation: run.gen, Condemned: condemned}); err != nil {
		return nil, errors.Wrap(err, "sealing")
	}

	state := &gcState{Generation: run.gen, Snapshot: run.snapshot, Condemned: condemned, Horizon: horizon}
	if err := storeGCState(ps.storage, state); err != nil {
		return nil, errors.Wrap(err, "storing gc state")
	}

	ps.logger.Info("gc marked", "generation", run.gen, "marked", report.Marked, "objects", report.Objects, "archives", report.Archives, "dead", report.DeadObjects, "deadBytes", humanize.Bytes(report.DeadBytes),
		"mark", markTook.Round(time.Millisecond), "merge", mergeTook.Round(time.Millisecond))

	report.SweepSkipped = run.sweepBlocker()
	if report.SweepSkipped == "" && run.anySelected() {
		checkStart := time.Now()
		if err := run.confirmDrops(ctx); err != nil {
			return nil, err
		}

		ps.logger.Info("gc confirmed the drops", "generation", run.gen, "took", time.Since(checkStart).Round(time.Millisecond))
		report.SweepSkipped = run.sweepBlocker()
	}

	run.releaseMark()

	if report.SweepSkipped == "" && opts.Handoff {
		phase := progress.Start(ctx, progress.OpCollect, progress.PhasePublish, 0, 0)
		if report.Published, err = run.publish(); err != nil {
			return nil, err
		}
		phase.Finish()
	} else if report.SweepSkipped == "" {
		sweepStart := time.Now()
		if err := run.sweep(ctx, report); err != nil {
			return nil, err
		}
		gcSweepDuration.Record(ctx, time.Since(sweepStart).Seconds())
		gcReclaimedBytes.Add(ctx, int64(report.ReclaimedBytes))

		state.Swept = true
		if err := storeGCState(ps.storage, state); err != nil {
			return nil, errors.Wrap(err, "storing gc state")
		}
	}

	for _, ga := range run.order {
		if ga.next.DeadObjects == 0 || (state.Swept && run.selected(ga)) {
			continue
		}

		if report.OldestDead.IsZero() || ga.next.DeadSince.Before(report.OldestDead) {
			report.OldestDead = ga.next.DeadSince
		}
	}

	if report.SweepSkipped != "" {
		ps.logger.Info("gc did not sweep", "generation", run.gen, "reason", report.SweepSkipped)
	}

	purgeStart := time.Now()
	phase = progress.Start(ctx, progress.OpCollect, progress.PhasePurge, 0, 0)
	purgeNow, err := storageNow(ps.storage, opts.Now)
	if err != nil {
		return nil, errors.Wrap(err, "reading when the gc state was stored")
	}

	report.Purged, err = ps.PurgeQuarantine(opts.Quarantine, purgeNow)
	if err != nil {
		return nil, errors.Wrap(err, "purging the quarantine")
	}

	report.RetiredBytes, err = ps.retiredBytes()
	if err != nil {
		return nil, errors.Wrap(err, "measuring the quarantine")
	}

	phase.Finish()

	ps.logger.Info("gc purged the quarantine", "generation", run.gen, "purged", report.Purged,
		"retired", humanize.Bytes(report.RetiredBytes), "took", time.Since(purgeStart).Round(time.Millisecond))

	var horizons [][]string
	if prev != nil {
		horizons = append(horizons, prev.Horizon)
	}

	pruned, err := ps.pruneSessionMarkers(opts.Now, sessions, append(horizons, state.Horizon)...)
	if err != nil {
		return nil, errors.Wrap(err, "pruning session markers")
	}

	if pruned > 0 {
		ps.logger.Info("gc deleted the markers of sessions that left no archive", "generation", run.gen, "sessions", pruned)
	}

	report.Duration = time.Since(started)

	return report, nil
}

// newestGeneration is the newest generation a seal or a mark result names,
// zero for none, which a collection without a stored state continues
// after: sessions read only the seals of generations newer than they began.
func (ps *PackStorage) newestGeneration() (uint64, error) {
	newest, err := ps.lastSeal()
	if err != nil {
		return 0, err
	}

	if err := ps.refreshArchives(); err != nil {
		return 0, err
	}

	ps.mtx.RLock()
	defer ps.mtx.RUnlock()

	for _, a := range ps.archives {
		if g := a.gcResult(); g != nil {
			newest = max(newest, g.Generation)
		}
	}

	return newest, nil
}

// scanArchive walks an archive's index record by record, counting the
// records and their bytes when asked.
func scanArchive(a *archive, fn func(pos int, rec *IndexRecord) error, count *int, size *uint64) error {
	scanner, err := a.scanIndex()
	if err != nil {
		return err
	}
	defer scanner.close()

	for pos := 0; ; pos++ {
		record, err := scanner.next()
		if err != nil {
			return err
		}

		if record == nil {
			return nil
		}

		if count != nil {
			*count++
		}

		if size != nil {
			*size += uint64(record.Length)
		}

		if err := fn(pos, record); err != nil {
			return err
		}
	}
}

// takeSnapshot fixes the set of committed archives this generation covers
// and loads their indexes and previous mark results.
func (r *gcRun) takeSnapshot(ctx context.Context) error {
	// listed first: a session that commits after this is either live here
	// or has its archives committed in the snapshot
	live, err := r.liveSessions()
	if err != nil {
		return errors.Wrap(err, "listing sessions")
	}

	if err := r.ps.refreshArchives(); err != nil {
		return errors.Wrap(err, "catching up with the storage's archives")
	}

	r.ps.mtx.RLock()
	archives := make([]*archive, 0, len(r.ps.archives))
	for _, a := range r.ps.archives {
		a.mtx.RLock()
		committed := a.readOnly && a.state == ArchiveCommitted
		pending := a.readOnly && a.state == ArchivePending && live[a.session]
		a.mtx.RUnlock()

		if committed {
			archives = append(archives, a)
		}

		if pending {
			r.pending = append(r.pending, a)
		}
	}
	r.ps.mtx.RUnlock()

	sort.Slice(archives, func(i, j int) bool { return archives[i].name < archives[j].name })

	indexes := make([]IndexFile, len(archives))

	var phase *progress.Phase
	if !r.check {
		phase = progress.Start(ctx, progress.OpCollect, progress.PhaseSnapshot, int64(len(archives)), 0)
	}

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(r.opts.Readers)

	for i, a := range archives {
		if idx, ok := r.reads.indexes[a.name]; ok {
			indexes[i] = idx
			phase.Add(1, 0)
			continue
		}

		grp.Go(func() error {
			defer phase.Add(1, 0)

			if err := gctx.Err(); err != nil {
				return err
			}

			idx, err := a.getIndex()
			if err != nil {
				return errors.Wrapf(err, "reading index of %s", a.name)
			}

			indexes[i] = idx

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}
	phase.Finish()

	r.located = make(map[refKey]placed)

	for i, a := range archives {
		ga := &gcArchive{a: a, prev: a.gcResult(), idx: indexes[i], count: len(indexes[i])}
		r.reads.indexes[a.name] = ga.idx

		for pos := range ga.idx {
			rec := &ga.idx[pos]
			ga.bytes += uint64(rec.Length)

			if t := proto.ObjectType(rec.Type); t == proto.ObjectType_BLOB || t == proto.ObjectType_TOMBSTONE {
				continue
			}

			if key := keyOf(rec.Sum[:]); r.located[key].rec == nil {
				r.located[key] = placed{key: key, a: a, rec: rec}
			}
		}

		ga.cur = bitset.New(uint(ga.count))

		r.archives[a.name] = ga
		r.order = append(r.order, ga)
	}

	return nil
}

// collectRoots finds the commits and pins of the snapshot that carry no
// tombstone. Tombstones are remembered so the merge can decide their fate.
// root pairs a root ref with the set Owner names it for.
func (r *gcRun) root(hash []byte) gcRoot {
	root := gcRoot{key: keyOf(hash)}
	if r.opts.Owner != nil {
		root.owner = r.opts.Owner(hash)
	}

	if r.opts.Shallow != nil {
		root.shallow = r.opts.Shallow(hash)
	}

	if r.opts.Death != nil {
		root.death = deathOf(r.opts.Death(hash), r.opts.Now)
	}

	return root
}

// batches groups the roots into the units the mark checkpoints. A batch
// holds the roots of one set only, so every run it spills belongs to that
// set, and either shallow roots or whole ones.
func (r *gcRun) batches() [][]gcRoot {
	var batches [][]gcRoot

	for start := 0; start < len(r.roots); {
		end := start
		for end < len(r.roots) && end-start < rootBatch && r.roots[end].owner == r.roots[start].owner && r.roots[end].shallow == r.roots[start].shallow {
			end++
		}

		batches = append(batches, r.roots[start:end])
		start = end
	}

	return batches
}

func (r *gcRun) collectRoots(ctx context.Context) error {
	if err := r.readTombstones(ctx); err != nil {
		return err
	}

	tombstoned := make(map[refKey]bool)
	// tombOf maps a tombstone's own ref to its target
	tombOf := make(map[refKey]refKey)
	commits := make(map[refKey]bool)

	for _, ga := range r.order {
		tombs := r.reads.tombs[ga.a.name]

		ga.each(func(pos int, rec *IndexRecord) {
			if proto.ObjectType(rec.Type) == proto.ObjectType_COMMIT {
				commits[keyOf(rec.Sum[:])] = true
			}

			hdr := tombs[pos]
			if proto.ObjectType(rec.Type) != proto.ObjectType_TOMBSTONE || !hdr.named {
				return
			}

			target := hdr.target
			tombstoned[target] = true
			tombOf[keyOf(rec.Sum[:])] = target
			r.targets[target] = false

			v := ga.a.version(*rec)
			r.tombstones = append(r.tombstones, gcTombstone{ga: ga, pos: pos, target: target, version: v})

			newestAt(r.newestTomb, target, v)
			r.tombTimes[v.Time.UnixNano()] = true
			if r.condemnedAt[v.Time.UnixNano()] {
				newestAt(r.condemning, target, v)
			}

			if hdr.erase {
				r.erased = append(r.erased, target)
			}
		})
	}

	keep := func(untombed refKey) {
		if !r.untombed[untombed] {
			r.untombed[untombed] = true
			r.roots = append(r.roots, r.root(untombed[:]))
		}
	}

	// a session commits only after copying what it relies on, so its
	// un-tombstones stay its own until then; they keep what they take
	// back from the moment they are written
	r.pendingAt = make(map[refKey]placed)
	var pendingTombs []placed

	for _, a := range r.pending {
		err := scanArchive(a, func(_ int, rec *IndexRecord) error {
			held := *rec
			if proto.ObjectType(rec.Type) == proto.ObjectType_TOMBSTONE {
				pendingTombs = append(pendingTombs, placed{a: a, rec: &held})
			} else {
				r.pendingAt[keyOf(rec.Sum[:])] = placed{key: keyOf(rec.Sum[:]), a: a, rec: &held}
				r.noteBlob(rec)
			}

			return nil
		}, nil, nil)
		if err != nil && !notExist(err) {
			return errors.Wrapf(err, "reading index of %s", a.name)
		}
	}

	// a pending archive whose session ended without committing is gone,
	// and with it what its un-tombstones took back
	headers, err := r.readHeaders(ctx, pendingTombs, true)
	if err != nil {
		return err
	}

	var pendingUntombs []refKey
	for _, hdr := range headers {
		if hdr != nil && hdr.TombstoneFor != nil {
			pendingUntombs = append(pendingUntombs, keyOf(hdr.TombstoneFor.Hash))
		}
	}

	// an un-tombstone names only the tombstone it takes back, so one of
	// an object no tombstone names, as of a session's own commit, is
	// told by the session's objects
	byTomb := make(map[refKey]refKey, len(r.pendingAt))
	for key := range r.pendingAt {
		byTomb[keyOf(proto.TombstoneRef(&proto.Ref{Hash: key[:]}).Hash)] = key
	}

	for _, tomb := range pendingUntombs {
		if untombed, ok := tombOf[tomb]; ok {
			keep(untombed)
		} else if untombed, ok := byTomb[tomb]; ok {
			keep(untombed)
		}
	}

	leases, err := r.ps.RestoreLeases(ctx)
	if err != nil {
		return errors.Wrap(err, "listing restore leases")
	}

	for _, ref := range leases {
		r.roots = append(r.roots, r.root(ref.Hash))
	}

	for _, ga := range r.order {
		ga.each(func(_ int, rec *IndexRecord) {
			t := proto.ObjectType(rec.Type)
			if _, isTarget := r.targets[keyOf(rec.Sum[:])]; isTarget && t != proto.ObjectType_TOMBSTONE {
				oldestAt(r.oldestCopy, keyOf(rec.Sum[:]), ga.a.version(*rec))
				r.noteBlob(rec)
			}

			if t != proto.ObjectType_COMMIT && t != proto.ObjectType_PIN && t != proto.ObjectType_POLICY {
				return
			}

			if key := keyOf(rec.Sum[:]); !tombstoned[key] {
				r.roots = append(r.roots, r.root(rec.Sum[:]))
			}
		})
	}

	// a committed session's un-tombstone of its own commit is older than
	// the tombstone that retires the commit, so only a revival, newer than
	// it, takes a commit back; an object without a copy left is no root
	lost := make(map[refKey]bool)

	for _, t := range r.tombstones {
		untombed, ok := tombOf[t.target]
		if !ok {
			continue
		}

		if _, held := r.oldestCopy[untombed]; !held {
			if !lost[untombed] && r.newestTomb[untombed].Before(r.newestTomb[t.target]) {
				lost[untombed] = true
				r.ps.logger.Error("revived object has no copy left", "ref", fmt.Sprintf("%x", untombed))
			}

			continue
		}

		if commits[untombed] && !r.newestTomb[untombed].Before(r.newestTomb[t.target]) {
			continue
		}

		keep(untombed)
	}

	r.spendTombstones(tombOf)

	return ctx.Err()
}

func (r *gcRun) noteBlob(rec *IndexRecord) {
	if proto.ObjectType(rec.Type) == proto.ObjectType_BLOB {
		r.blobs[keyOf(rec.Sum[:])] = true
	}
}

// readTombstones reads the headers of the snapshot's tombstones that no
// earlier read of this run kept.
func (r *gcRun) readTombstones(ctx context.Context) error {
	var tombs []placed
	var positions []int

	for _, ga := range r.order {
		if _, ok := r.reads.tombs[ga.a.name]; ok {
			continue
		}

		ga.each(func(pos int, rec *IndexRecord) {
			if proto.ObjectType(rec.Type) == proto.ObjectType_TOMBSTONE {
				tombs = append(tombs, placed{a: ga.a, rec: rec})
				positions = append(positions, pos)
			}
		})
	}

	headers, err := r.readHeaders(ctx, tombs, false)
	if err != nil {
		return err
	}

	for _, ga := range r.order {
		if _, ok := r.reads.tombs[ga.a.name]; !ok {
			r.reads.tombs[ga.a.name] = make(map[int]tombHeader)
		}
	}

	for i, tomb := range tombs {
		r.reads.tombs[tomb.a.name][positions[i]] = tombHeaderOf(headers[i])
	}

	return nil
}

// readHeaders decodes the object headers of the records, reading each run
// of them that sits close together in an archive at once, up to Readers
// runs at a time. With missingOK, the headers of a run whose archive is gone
// are left nil.
func (r *gcRun) readHeaders(ctx context.Context, records []placed, missingOK bool) ([]*proto.ObjectHeader, error) {
	at := make([]int, len(records))
	for i := range at {
		at[i] = i
	}

	sort.Slice(at, func(i, j int) bool { return placedBefore(records[at[i]], records[at[j]]) })

	sorted := make([]placed, len(records))
	for i, from := range at {
		sorted[i] = records[from]
	}

	headers := make([]*proto.ObjectHeader, len(records))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(r.opts.Readers)

	eachSpan(sorted, func(start, end int, span int64) {
		run, into := sorted[start:end], at[start:end]

		grp.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			a, from := run[0].a, int64(run[0].rec.Offset)

			buf, _, err := a.readSpan(from, span)
			if missingOK && notExist(err) {
				return nil
			}

			if err != nil {
				return errors.Wrapf(err, "reading %d bytes at %d of %s", span, from, a.name)
			}

			for i, p := range run {
				record := buf[int64(p.rec.Offset)-from:][:p.rec.Length]
				hdrSize, consumed := proto.DecodeVarint(record)
				if consumed <= 0 || uint64(len(record)) < uint64(consumed)+hdrSize {
					return errors.Errorf("the header of %x in %s is truncated", p.rec.Sum, a.name)
				}

				hdr, err := proto.NewObjectHeaderFromBytes(record[consumed : uint64(consumed)+hdrSize])
				if err != nil {
					return errors.Wrapf(err, "reading the header of %x in %s", p.rec.Sum, a.name)
				}

				headers[into[i]] = hdr
			}

			return nil
		})
	})

	return headers, grp.Wait()
}

// recordAt names a record of the snapshot.
type recordAt struct {
	archive string
	pos     int
}

// spendTombstones finds the tombstones the previous horizon covers that
// condemn nothing any more: an un-tombstone took them back, or no copy
// they hide is left. Un-tombstones are left to outlive their targets.
func (r *gcRun) spendTombstones(tombOf map[refKey]refKey) {
	for _, t := range r.tombstones {
		if _, untomb := tombOf[t.target]; untomb {
			continue
		}

		if !r.condemnedAt[t.version.Time.UnixNano()] {
			continue
		}

		oldest, hides := r.oldestCopy[t.target]
		if r.untombed[t.target] || !hides || t.version.Before(oldest) {
			r.spent[recordAt{t.ga.a.name, t.pos}] = true
		}
	}
}

func oldestAt(versions map[refKey]Version, key refKey, v Version) {
	if old, ok := versions[key]; !ok || v.Before(old) {
		versions[key] = v
	}
}

// visitedSet remembers, for each object of a group, the bits of the sets
// that reached it, so a set walks into what another set already walked
// and its run holds everything it reaches. A group of more than 64 sets
// shares the last bit among the rest, which then do not walk into what
// one of them reached.
type visitedSet struct {
	shards [256]struct {
		mtx  sync.Mutex
		seen map[refKey]uint64
	}
	bits map[int64]uint64
}

func newVisitedSet() *visitedSet {
	v := &visitedSet{bits: make(map[int64]uint64)}
	for i := range v.shards {
		v.shards[i].seen = make(map[refKey]uint64)
	}

	return v
}

// bit is the set's bit in the group, given out in the order sets come.
func (v *visitedSet) bit(set int64) uint64 {
	if b, ok := v.bits[set]; ok {
		return b
	}

	b := uint64(1) << min(len(v.bits), 63)
	v.bits[set] = b

	return b
}

// claim returns the keys the set with that bit has not reached before,
// and remembers that it has.
func (v *visitedSet) claim(keys []refKey, bit uint64) []refKey {
	fresh := keys[:0]
	for _, key := range keys {
		shard := &v.shards[key[0]]
		shard.mtx.Lock()
		if seen := shard.seen[key]; seen&bit == 0 {
			shard.seen[key] = seen | bit
			fresh = append(fresh, key)
		}
		shard.mtx.Unlock()
	}

	return fresh
}

// claimUnreached returns the keys no set has reached, and marks them
// reached by all.
func (v *visitedSet) claimUnreached(keys []refKey) []refKey {
	fresh := keys[:0]
	for _, key := range keys {
		shard := &v.shards[key[0]]
		shard.mtx.Lock()
		if shard.seen[key] == 0 {
			shard.seen[key] = ^uint64(0)
			fresh = append(fresh, key)
		}
		shard.mtx.Unlock()
	}

	return fresh
}

// snapshotID identifies the generation, the run format, the archives it
// covers and the sorted roots with all that splits them into batches, so a checkpoint is only resumed against the same snapshot and
// the same batches, by a build that reads its runs.
func (r *gcRun) snapshotID() string {
	h := sha256.New()
	fmt.Fprintf(h, "%d %d %d\n", r.gen, liveRefSize, rootBatch)
	for _, ga := range r.order {
		fmt.Fprintf(h, "%s\n", ga.a.name)
	}

	for _, root := range r.roots {
		h.Write(root.key[:])
		fmt.Fprintf(h, " %d %d %t %d\n", root.owner.Group, root.owner.Set, root.shallow, root.death)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// staleRunDir is how long a run directory goes unwritten before any
// collection sharing its parent deletes it.
const staleRunDir = 7 * 24 * time.Hour

// removeStaleRunDirs deletes the run directories under parent other than
// keep that nothing has written to for staleRunDir.
func removeStaleRunDirs(parent, keep string, now time.Time) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}

	for _, e := range entries {
		dir := filepath.Join(parent, e.Name())
		if !e.IsDir() || dir == keep {
			continue
		}

		if written, ok := lastWritten(dir); ok && now.Sub(written) >= staleRunDir {
			_ = os.RemoveAll(dir)
		}
	}
}

// lastWritten is the newest modification time of dir and what it holds.
func lastWritten(dir string) (time.Time, bool) {
	info, err := os.Stat(dir)
	if err != nil {
		return time.Time{}, false
	}

	newest := info.ModTime()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return time.Time{}, false
	}

	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return time.Time{}, false
		}

		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}

	return newest, true
}

// openRunDir prepares the run directory, named after the snapshot so that
// stores sharing a TempDir keep apart, keeping the runs of batches an
// interrupted mark of the same snapshot completed and that read back as
// their checkpoints recorded them; any other batch is marked again.
func (r *gcRun) openRunDir(batches [][]gcRoot) (*liveRuns, map[int]bool, error) {
	id := r.snapshotID()
	parent := filepath.Join(r.opts.TempDir, "goback-gc")
	r.runDir = filepath.Join(parent, fmt.Sprintf("gen-%d-%s", r.gen, id[:16]))
	manifest := filepath.Join(r.runDir, "snapshot")

	removeStaleRunDirs(parent, r.runDir, time.Now())

	if data, err := os.ReadFile(manifest); err != nil || string(data) != id {
		if err := os.RemoveAll(r.runDir); err != nil {
			return nil, nil, err
		}

		if err := os.MkdirAll(r.runDir, 0o755); err != nil {
			return nil, nil, err
		}

		if err := os.WriteFile(manifest, []byte(id), 0o644); err != nil {
			return nil, nil, err
		}
	}

	entries, err := os.ReadDir(r.runDir)
	if err != nil {
		return nil, nil, err
	}

	live := newLiveRuns(r.runDir, liveRunLimit)
	done := make(map[int]bool)
	adopted := make(map[string]bool)

	for _, e := range entries {
		batch, ok := strings.CutSuffix(strings.TrimPrefix(e.Name(), "batch-"), ".done")
		if !ok || !strings.HasPrefix(e.Name(), "batch-") {
			continue
		}

		n, err := strconv.Atoi(batch)
		if err != nil || n >= len(batches) {
			continue
		}

		runs, ok := checkpointed(r.runDir, n)
		if !ok {
			if err := os.Remove(filepath.Join(r.runDir, e.Name())); err != nil {
				return nil, nil, err
			}

			continue
		}

		done[n] = true
		for _, path := range runs {
			adopted[path] = true
			live.adopt(path, batches[n][0].owner)
		}
	}

	for _, e := range entries {
		if path := filepath.Join(r.runDir, e.Name()); strings.HasSuffix(e.Name(), ".run") && !adopted[path] {
			_ = os.Remove(path)
		}
	}

	return live, done, nil
}

// mark walks breadth-first from the roots, in checkpointed batches, and
// returns the live refs. Blobs are never read: the File that names one,
// or a root that is one, marks it.
func (r *gcRun) mark(ctx context.Context) (*liveRuns, error) {
	sortRoots(r.roots)
	batches := r.batches()

	live, done, err := r.openRunDir(batches)
	if err != nil {
		return nil, err
	}

	// objects are counted once per group, so each group walks with a
	// visited set of its own; the roots are sorted by group, so only the
	// one being walked is ever held
	var group int64
	visited := newVisitedSet()

	name := progress.PhaseMark
	if r.check {
		name = progress.PhaseConfirm
	}

	phase := progress.Start(ctx, progress.OpCollect, name, int64(len(batches)), 0)
	started, logged := time.Now(), time.Now()

	for batch, roots := range batches {
		if time.Since(logged) >= time.Minute {
			logged = time.Now()
			r.ps.logger.Info("gc marking", "generation", r.gen, "batch", batch, "of", len(batches),
				"elapsed", time.Since(started).Round(time.Second))
		}

		if owner := roots[0].owner; owner.Group != group {
			group, visited = owner.Group, newVisitedSet()
		}

		if done[batch] {
			r.resumed++
			phase.Add(1, 0)
			continue
		}

		live.begin(batchPrefix(batch), roots[0].owner)

		bit := visited.bit(roots[0].owner.Set)
		claim := func(keys []refKey) []refKey { return visited.claim(keys, bit) }

		// the roots that die later walk first, so what they reach is
		// claimed for them before a root that dies sooner reaches it
		for start := 0; start < len(roots); {
			end := start + 1
			for end < len(roots) && roots[end].death == roots[start].death {
				end++
			}

			if err := r.markRoots(ctx, roots[start:end], claim, live); err != nil {
				return nil, err
			}

			start = end
		}

		if err := live.checkpoint(batch); err != nil {
			return nil, err
		}

		if gcAfterBatch != nil {
			if err := gcAfterBatch(batch); err != nil {
				return nil, err
			}
		}

		phase.Add(1, 0)
	}
	phase.Finish()

	return live, nil
}

// markRoots walks from roots of one set, shallowness and death.
func (r *gcRun) markRoots(ctx context.Context, roots []gcRoot, claim func([]refKey) []refKey, live *liveRuns) error {
	var blobs []liveRef
	frontier := claim(keysOf(roots))
	walked := frontier[:0]

	for _, key := range frontier {
		if r.blobs[key] {
			blobs = append(blobs, liveRef{key: key, death: roots[0].death})
		} else {
			walked = append(walked, key)
		}
	}

	if err := live.add(blobs); err != nil {
		return err
	}

	return r.markBatch(ctx, walked, claim, live, roots[0].shallow, roots[0].death)
}

// markBatch walks breadth-first from the frontier, reading each run of
// records that sit close together in an archive at once, up to Readers runs
// at a time, and records what it reaches as dying with death. A shallow
// walk leaves the parts of files out.
func (r *gcRun) markBatch(ctx context.Context, frontier []refKey, claim func([]refKey) []refKey, live *liveRuns, shallow bool, death deathClass) error {
	for len(frontier) > 0 {
		records, err := r.placeAll(ctx, frontier)
		if err != nil {
			return err
		}

		var next []refKey
		var nextMtx sync.Mutex

		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(r.opts.Readers)

		eachSpan(records, func(start, end int, span int64) {
			run := records[start:end]

			grp.Go(func() error {
				var children []refKey
				var found []liveRef

				reads, err := r.readSpan(gctx, run, span)
				if err != nil {
					return err
				}

				for _, read := range reads {
					found = append(found, liveRef{key: read.key, size: uint64(len(read.obj.GetFile().GetInline()))})

					if shallow {
						var parts []liveRef
						children = appendChildren(children, &parts, read.obj)
					} else {
						children = appendChildren(children, &found, read.obj)
					}
				}

				for i := range found {
					found[i].death = death
				}

				if err := live.add(found); err != nil {
					return err
				}

				fresh := claim(children)
				if len(fresh) > 0 {
					nextMtx.Lock()
					next = append(next, fresh...)
					nextMtx.Unlock()
				}

				return nil
			})
		})

		if err := grp.Wait(); err != nil {
			return err
		}

		frontier = next
	}

	return nil
}

// placeAll places the keys in archive and offset order, leaving out those
// with no copy to read. An object that is gone is skipped: a mark cannot
// mend it, and the sweep will not drop what it never saw.
func (r *gcRun) placeAll(ctx context.Context, keys []refKey) ([]placed, error) {
	records := make([]placed, len(keys))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(r.opts.Readers)

	for i, key := range keys {
		if p, ok := r.located[key]; ok {
			records[i] = p
			continue
		}

		grp.Go(func() error {
			p, err := r.place(gctx, key)
			records[i] = p

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	found := records[:0]
	for i, p := range records {
		if p.rec == nil {
			r.missing(keys[i])
			continue
		}

		found = append(found, p)
	}

	sort.Slice(found, func(i, j int) bool { return placedBefore(found[i], found[j]) })

	return found, nil
}

func placedBefore(a, b placed) bool {
	if a.a != b.a {
		return a.a.name < b.a.name
	}

	return a.rec.Offset < b.rec.Offset
}

// eachSpan calls fn with each run of the sorted records, from start to
// end, that sits close together in one archive, and the bytes it covers.
func eachSpan(sorted []placed, fn func(start, end int, span int64)) {
	for start := 0; start < len(sorted); {
		archiveEnd := start + 1
		for archiveEnd < len(sorted) && sorted[archiveEnd].a == sorted[start].a {
			archiveEnd++
		}

		for start < archiveEnd {
			end, span := spanOf(sorted[:archiveEnd], start)
			fn(start, end, span)
			start = end
		}
	}
}

const (
	// markGap is how far apart two records may sit and still be worth
	// fetching together; markSpan bounds what one such read covers.
	markGap  = 64 << 10
	markSpan = 8 << 20
)

// markRead is one object the mark read, with the key it was asked for.
type markRead struct {
	key refKey
	obj *proto.Object
}

// placed is a record's position, kept so a run of them can be read at once.
type placed struct {
	key refKey
	a   *archive
	rec *IndexRecord
}

// missing reports a reachable object the store lacks, which only a store
// in front of another expects.
func (r *gcRun) missing(key refKey) {
	if r.opts.Behind {
		r.ps.logger.Debug("reachable object is held behind", "ref", fmt.Sprintf("%x", key))
		return
	}

	r.lost.Add(1)
	r.ps.logger.Warn("reachable object is missing", "ref", fmt.Sprintf("%x", key))
}

// place finds a copy of the object to read: in the snapshot, or, for one
// written since, through the index, or in a live session's archive. It
// returns a zero placed when there is none.
func (r *gcRun) place(ctx context.Context, key refKey) (placed, error) {
	if p, ok := r.located[key]; ok {
		return p, nil
	}

	a, rec, err := r.ps.indexLocation(ctx, &proto.Ref{Hash: key[:]})
	if err != nil {
		return placed{}, errors.Wrapf(err, "locating %x", key)
	}

	if rec != nil {
		return placed{key: key, a: a, rec: rec}, nil
	}

	return r.pendingAt[key], nil
}

// spanOf extends the run starting at start for as long as the records stay
// close together, and returns where it ends and how many bytes it covers.
func spanOf(records []placed, start int) (int, int64) {
	from := int64(records[start].rec.Offset)
	span := int64(records[start].rec.Length)

	end := start + 1
	for end < len(records) {
		previous := int64(records[end-1].rec.Offset) + int64(records[end-1].rec.Length)
		grown := int64(records[end].rec.Offset) + int64(records[end].rec.Length) - from

		if int64(records[end].rec.Offset)-previous > markGap || grown > markSpan {
			break
		}

		span = grown
		end++
	}

	return end, span
}

// readSpan reads one run of records in a single read and decodes each one.
func (r *gcRun) readSpan(ctx context.Context, records []placed, span int64) ([]markRead, error) {
	a := records[0].a
	from := int64(records[0].rec.Offset)

	// an archive still being written is read the ordinary way, which
	// finalizes it first
	if len(records) == 1 || !a.readOnlyNow() {
		reads := make([]markRead, 0, len(records))

		for _, record := range records {
			ref := &proto.Ref{Hash: record.key[:]}

			var obj *proto.Object
			var err error

			// a session's own object is in no index until it commits; one
			// that ended without committing took the archive with it
			if p, ok := r.located[record.key]; ok && p.a == a {
				obj, err = a.getRaw(ctx, ref, record.rec)
				if err != nil {
					obj, err = r.ps.Get(ctx, ref)
				}
			} else if p, ok := r.pendingAt[record.key]; ok && p.a == a {
				obj, err = a.getRaw(ctx, ref, record.rec)
				if notExist(err) {
					err = backup.ErrNotFound
				}
			} else {
				obj, err = r.ps.Get(ctx, ref)
			}

			if errors.Is(err, backup.ErrNotFound) {
				r.missing(record.key)
				continue
			}

			if err != nil {
				return nil, errors.Wrapf(err, "reading %x", record.key)
			}

			reads = append(reads, markRead{key: record.key, obj: obj})
		}

		return reads, nil
	}

	buf, _, err := a.readSpan(from, span)
	if err != nil {
		return nil, errors.Wrapf(err, "reading %d bytes at %d of %s", span, from, a.name)
	}

	reads := make([]markRead, 0, len(records))

	for _, record := range records {
		at := int64(record.rec.Offset) - from

		obj, err := a.objectFromRecord(ctx, &proto.Ref{Hash: record.key[:]}, record.rec, buf[at:at+int64(record.rec.Length)], 0)
		if err != nil {
			return nil, errors.Wrapf(err, "reading %x", record.key)
		}

		reads = append(reads, markRead{key: record.key, obj: obj})
	}

	return reads, nil
}

// appendChildren adds the object's metadata children to children and its
// blob parts straight to live.
func appendChildren(children []refKey, live *[]liveRef, obj *proto.Object) []refKey {
	switch obj.Type() {
	case proto.ObjectType_COMMIT:
		if tree := obj.GetCommit().GetTree(); tree != nil {
			children = append(children, keyOf(tree.Hash))
		}
	case proto.ObjectType_TREE:
		tree := obj.GetTree()
		for _, node := range tree.GetNodes() {
			if node.GetRef() != nil {
				children = append(children, keyOf(node.Ref.Hash))
			}
			for _, ref := range node.GetStat().MetadataRefs() {
				children = append(children, keyOf(ref.Hash))
			}
		}
		for _, split := range tree.GetSplits() {
			children = append(children, keyOf(split.Hash))
		}
	case proto.ObjectType_FILE:
		file := obj.GetFile()
		for _, part := range file.GetParts() {
			if part.GetRef() != nil {
				*live = append(*live, liveRef{key: keyOf(part.Ref.Hash), size: part.GetLength()})
			}
		}
		for _, split := range file.GetSplits() {
			children = append(children, keyOf(split.Hash))
		}
	case proto.ObjectType_PIN:
		if target := obj.GetPin().GetTarget(); target != nil {
			children = append(children, keyOf(target.Hash))
		}
	}

	return children
}

type mergeHead struct {
	ga      *gcArchive
	pos     int
	rec     *IndexRecord
	scanner indexScanner
}

type mergeHeap []mergeHead

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].rec.Sum[:], h[j].rec.Sum[:]) < 0
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeHead)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]

	return x
}

// merge walks the live set and every archive index in lockstep and sets the
// bit of each index position that is live. A tombstone is live while the
// snapshot still holds its target. A root the snapshot holds that the live
// set lacks fails the merge: the mark missed it, and what it reaches.
func (r *gcRun) merge(live *liveRuns) (uint64, error) {
	var marked uint64

	// reached is whether the live set holds each root, held whether the
	// snapshot does
	reached := make(map[refKey]bool, len(r.roots))
	for _, root := range r.roots {
		reached[root.key] = false
	}

	held := make(map[refKey]bool)

	var counted refKey
	err := r.scan(live, func(ga *gcArchive, pos int, rec *IndexRecord, found reach) {
		if _, root := reached[rec.Sum]; root {
			reached[rec.Sum] = true
		}

		ga.cur.Set(uint(pos))
		ga.death.set(pos, ga.count, found.death)
		marked++

		owners, size := found.owners, found.size

		// a second copy of an object adds to what it takes up, not to the
		// content it carries
		first := rec.Sum != counted
		counted = rec.Sum

		// owners come in group order, so the first set of each group is
		// the one that group carries the object in
		group := int64(0)
		attributed := false
		for _, owner := range owners {
			if owner.Set == 0 || (attributed && owner.Group == group) {
				continue
			}

			group, attributed = owner.Group, true
			r.setBytes[owner.Set] += uint64(rec.Length)

			if first {
				r.setDeduplicated[owner.Set] += size
			}
		}

		if !attributed && r.opts.Owner != nil {
			r.unattributed += uint64(rec.Length)
		}

		content := uint64(0)
		if first {
			content = size
		}

		r.share(owners, uint64(rec.Length), content)
	}, func(sum refKey) {
		if _, isTarget := r.targets[sum]; isTarget {
			r.targets[sum] = true
		}

		if _, root := reached[sum]; root {
			held[sum] = true
		}
	})
	if err != nil {
		return 0, err
	}

	var unmarked []refKey
	for key := range held {
		if !reached[key] {
			unmarked = append(unmarked, key)
		}
	}

	if len(unmarked) > 0 {
		return 0, errors.Errorf("the mark left %d of %d roots unmarked, %x among them", len(unmarked), len(r.roots), unmarked[0])
	}

	for _, t := range r.tombstones {
		if r.targets[t.target] {
			t.ga.cur.Set(uint(t.pos))
			marked++
		}
	}

	return marked, nil
}

// share counts a record for every set that reached it, and for the one
// set of a group that alone reached it; content is the file content it
// carries, zero for a second copy.
func (r *gcRun) share(owners []Attribution, length, content uint64) {
	for start := 0; start < len(owners); {
		end, reached := start, 0
		for ; end < len(owners) && owners[end].Group == owners[start].Group; end++ {
			if owners[end].Set != 0 {
				r.setAlone[owners[end].Set] += length
				r.setDeduplicatedAlone[owners[end].Set] += content
				reached++
			}
		}

		if reached == 1 {
			for _, owner := range owners[start:end] {
				if owner.Set != 0 {
					r.setExclusive[owner.Set] += length
				}
			}
		}

		start = end
	}
}

// scan walks every index record of the snapshot in ref order alongside the
// sorted runs; hit sees the records the runs name, with what the runs hold
// of them, and each sees every record.
func (r *gcRun) scan(runs *liveRuns, hit func(ga *gcArchive, pos int, rec *IndexRecord, found reach), each func(sum refKey)) error {
	return r.scanOf(r.order, runs, hit, each)
}

// scanOf is scan over archives alone.
func (r *gcRun) scanOf(archives []*gcArchive, runs *liveRuns, hit func(ga *gcArchive, pos int, rec *IndexRecord, found reach), each func(sum refKey)) error {
	it, err := runs.iterator()
	if err != nil {
		return err
	}
	defer it.close()

	var h mergeHeap
	defer func() {
		for _, head := range h {
			_ = head.scanner.close()
		}
	}()

	for _, ga := range archives {
		scanner := &sliceScanner{idx: ga.idx}

		record, err := scanner.next()
		if err != nil {
			_ = scanner.close()

			return errors.Wrapf(err, "reading index of %s", ga.a.name)
		}

		if record == nil {
			_ = scanner.close()

			continue
		}

		h = append(h, mergeHead{ga: ga, rec: record, scanner: scanner})
	}
	heap.Init(&h)

	for h.Len() > 0 {
		top := h[0]
		sum := top.rec.Sum

		if found, ok := it.at(sum); ok {
			hit(top.ga, top.pos, top.rec, found)
		}

		if each != nil {
			each(sum)
		}

		record, err := top.scanner.next()
		if err != nil {
			return errors.Wrapf(err, "reading index of %s", top.ga.a.name)
		}

		if record == nil {
			_ = top.scanner.close()
			heap.Pop(&h)

			continue
		}

		top.pos++
		top.rec = record
		h[0] = top
		heap.Fix(&h, 0)
	}

	return it.err()
}

// flagErased walks the subtrees of the erased commits still in the
// snapshot and flags the archives holding what of it the live mark left
// unmarked.
func (r *gcRun) flagErased(ctx context.Context) error {
	var roots []refKey
	for _, target := range r.erased {
		if r.targets[target] {
			roots = append(roots, target)
		}
	}

	if len(roots) == 0 {
		return nil
	}

	runs := newLiveRuns(r.runDir, liveRunLimit)
	runs.begin("erased-", Attribution{})
	defer runs.close()

	visited := newVisitedSet()
	if err := r.markBatch(ctx, visited.claimUnreached(roots), visited.claimUnreached, runs, false, never); err != nil {
		return err
	}

	return r.scan(runs, func(ga *gcArchive, pos int, _ *IndexRecord, _ reach) {
		if !ga.cur.Test(uint(pos)) {
			ga.erase = true
		}
	}, nil)
}

// writeResults records each archive's mark result beside it, all of them
// stored when it returns without error.
func (r *gcRun) writeResults() error {
	grp := new(errgroup.Group)
	grp.SetLimit(r.opts.Readers)

	for _, ga := range r.order {
		next := &gcFile{Generation: r.gen, Snapshot: r.snapshot, Current: ga.cur, Erase: ga.erase, Death: ga.death}

		if ga.prev != nil && ga.prev.Generation == r.gen-1 {
			next.Previous = ga.prev.Current
		}

		ga.droppable = 0

		ga.each(func(pos int, rec *IndexRecord) {
			if r.droppable(ga, next, pos, rec) {
				ga.droppable += uint64(rec.Length)
			}

			if ga.cur.Test(uint(pos)) {
				return
			}

			if r.uncondemned(ga, rec) {
				newestAt(r.condemned, keyOf(rec.Sum[:]), ga.a.version(*rec))
			}

			next.DeadObjects++
			next.DeadBytes += uint64(rec.Length)
			next.Dead = append(next.Dead, prefixOf(rec.Sum[:]))
		})

		sort.Slice(next.Dead, func(i, j int) bool { return next.Dead[i] < next.Dead[j] })

		if next.DeadObjects > 0 {
			next.DeadSince = r.snapshot
			if ga.prev != nil && !ga.prev.DeadSince.IsZero() {
				next.DeadSince = ga.prev.DeadSince
			}
		}

		ga.next = next

		grp.Go(func() error {
			if err := writeGCFile(r.ps.storage, ga.a.name, next); err != nil {
				return errors.Wrapf(err, "writing gc result of %s", ga.a.name)
			}

			ga.a.setGCResult(next)

			return nil
		})
	}

	return grp.Wait()
}

// releaseMark lets go of what only the mark and its check read, so the
// sweep runs with what decides the drops alone.
func (r *gcRun) releaseMark() {
	r.reads, r.located, r.pending, r.pendingAt = nil, nil, nil, nil
	r.roots, r.tombstones, r.targets, r.blobs = nil, nil, nil, nil
	r.newestTomb, r.condemned, r.untombed, r.oldestCopy = nil, nil, nil, nil
}

// sweepBlocker returns why this generation must not sweep, or "".
func (r *gcRun) sweepBlocker() string {
	if r.opts.NoSweep {
		return "sweep disabled"
	}

	if lost := r.lost.Load(); lost > 0 {
		return fmt.Sprintf("%d reachable objects are missing", lost)
	}

	if r.prev == nil {
		return "first generation"
	}

	reason, err := r.ps.halted()
	if err != nil {
		return fmt.Sprintf("reading whether collections are halted: %s", err)
	}

	return reason
}

// sweep rewrites the archives whose dead share or age selects them, dropping
// objects unmarked in two consecutive generations.
func (r *gcRun) sweep(ctx context.Context, report *CollectReport) error {
	group := &compactionGroup{keep: r.keep, marked: r.marked, classes: r.classes, classed: r.classedOf}

	for _, ga := range r.order {
		if !r.selected(ga) {
			continue
		}

		group.candidates = append(group.candidates, ga.a)
		group.total += ga.a.size
	}

	if len(group.candidates) == 0 {
		return nil
	}

	r.ps.logger.Info("gc sweeping archives", "count", len(group.candidates))

	group.progress = progress.Start(ctx, progress.OpCollect, progress.PhaseSweep, int64(len(group.candidates)), int64(group.total))
	if err := r.ps.compactGroup(ctx, group); err != nil {
		return err
	}
	group.progress.Finish()

	report.Swept += len(group.candidates)
	report.ReclaimedObjects += group.droppedObjects
	report.ReclaimedBytes += group.droppedBytes
	report.CopiedBytes += group.copiedBytes

	return nil
}

// anySelected reports whether a sweep would rewrite any archive.
func (r *gcRun) anySelected() bool {
	for _, ga := range r.order {
		if r.selected(ga) {
			return true
		}
	}

	return false
}

// selected reports whether an archive is worth rewriting this generation.
func (r *gcRun) selected(ga *gcArchive) bool {
	if ga.next.Previous == nil || ga.bytes == 0 {
		return false
	}

	if ga.droppable == 0 {
		return false
	}

	if ga.next.Erase || float64(ga.droppable)/float64(ga.bytes) >= r.opts.DeadRatio {
		return true
	}

	return r.opts.Now.Sub(ga.next.DeadSince) >= r.opts.ErasureBound
}

// keep decides during a rewrite whether the candidate's object survives.
func (r *gcRun) keep(candidate *archive, hdr *proto.ObjectHeader) bool {
	ga := r.archives[candidate.name]
	if ga == nil {
		return true
	}

	pos := ga.idx.position(hdr.Ref.Hash)
	if pos < 0 || !r.droppable(ga, ga.next, pos, &ga.idx[pos]) {
		return true
	}

	if hdr.Timestamp != nil && r.opts.Now.Sub(hdr.Timestamp.AsTime()) < r.opts.MinAge {
		return true
	}

	return false
}

// marked reports whether the copy at loc was reachable in this generation.
// A copy outside the snapshot is younger than it and counts as reachable.
func (r *gcRun) marked(loc *IndexLocation) bool {
	ga := r.archives[loc.Archive]
	if ga == nil {
		return true
	}

	pos := ga.idx.position(loc.Record.Sum[:])

	return pos < 0 || ga.cur.Test(uint(pos))
}

// droppable reports whether a rewrite may drop the record at pos. A copy
// goes once two generations in a row found it unreachable and a tombstone
// newer than it was stored before the previous generation's horizon;
// what an un-tombstone takes back is marked. A tombstone goes once spent,
// or once its target is gone, the previous horizon covers it and every
// session of that horizon has ended.
func (r *gcRun) droppable(ga *gcArchive, g *gcFile, pos int, rec *IndexRecord) bool {
	if proto.ObjectType(rec.Type) == proto.ObjectType_TOMBSTONE {
		if r.spent[recordAt{ga.a.name, pos}] {
			return true
		}

		return g.dead(pos) && r.horizonEnded && r.condemnedAt[ga.a.version(*rec).Time.UnixNano()]
	}

	if !g.dead(pos) {
		return false
	}

	target := keyOf(rec.Sum[:])

	tomb, ok := r.condemning[target]

	return ok && ga.a.version(*rec).Before(tomb)
}

// uncondemned reports whether an unreachable record still needs a
// tombstone: none newer than it stands.
func (r *gcRun) uncondemned(ga *gcArchive, rec *IndexRecord) bool {
	if proto.ObjectType(rec.Type) == proto.ObjectType_TOMBSTONE {
		return false
	}

	tomb, ok := r.newestTomb[keyOf(rec.Sum[:])]

	return !ok || !ga.a.version(*rec).Before(tomb)
}

func newestAt(versions map[refKey]Version, key refKey, v Version) {
	if old, ok := versions[key]; !ok || old.Before(v) {
		versions[key] = v
	}
}

// condemnAttempts bounds how often a collection writes a tombstone that
// came out no newer than the copies it condemns.
const condemnAttempts = 3

// condemn stores a tombstone for every unreachable object that has none
// standing, in archives of their own, each newer than every copy it
// condemns: a session that begins after the seal must find those copies
// absent.
func (r *gcRun) condemn(ctx context.Context) error {
	pending := r.condemned

	for attempt := 1; len(pending) > 0; attempt++ {
		if attempt > condemnAttempts {
			return errors.Errorf("%d tombstones came out no newer than the copies they condemn", len(pending))
		}

		for target := range pending {
			if err := r.ps.Delete(ctx, &proto.Ref{Hash: append([]byte(nil), target[:]...)}); err != nil {
				return errors.Wrap(err, "condemning an unreachable object")
			}
		}

		if err := r.ps.Flush(); err != nil {
			return err
		}

		stale, err := r.notNewer(pending)
		if err != nil {
			return err
		}

		pending = stale
	}

	return nil
}

// notNewer returns the targets whose newest tombstone is not newer than
// their newest copy.
func (r *gcRun) notNewer(targets map[refKey]Version) (map[refKey]Version, error) {
	keys := make([]refKey, 0, len(targets))
	tombs := make([]*proto.Ref, 0, len(targets))

	for target := range targets {
		keys = append(keys, target)
		tombs = append(tombs, proto.TombstoneRef(&proto.Ref{Hash: append([]byte(nil), target[:]...)}))
	}

	found, err := r.ps.index.LocateTombstones(tombs, Scope{})
	if err != nil {
		return nil, err
	}

	stale := make(map[refKey]Version)

	for i, target := range keys {
		newest, err := r.ps.newest(found[string(tombs[i].Hash)])
		if err != nil {
			return nil, err
		}

		if newest == nil || !targets[target].Before(*newest) {
			stale[target] = targets[target]
		}
	}

	return stale, nil
}

// horizon lists, once the tombstones are stored, the versions of the
// committed archives there are and of the snapshot's tombstones, and the
// sessions that have begun and not ended.
func (r *gcRun) horizon() ([]time.Time, []string, error) {
	if err := r.ps.refreshArchives(); err != nil {
		return nil, nil, errors.Wrap(err, "catching up with the storage's archives")
	}

	seen := make(map[int64]bool)
	var condemned []time.Time

	// a compaction may have moved a tombstone out of its archive before
	// any horizon listed that archive
	for ns := range r.tombTimes {
		seen[ns] = true
		condemned = append(condemned, time.Unix(0, ns).UTC())
	}

	r.ps.mtx.RLock()
	for _, a := range r.ps.archives {
		a.mtx.RLock()
		created, committed := a.created, a.readOnly && a.state == ArchiveCommitted
		a.mtx.RUnlock()

		if committed && !created.IsZero() && !seen[created.UnixNano()] {
			seen[created.UnixNano()] = true
			condemned = append(condemned, created)
		}
	}
	r.ps.mtx.RUnlock()

	sort.Slice(condemned, func(i, j int) bool { return condemned[i].Before(condemned[j]) })

	live, err := r.liveSessions()
	if err != nil {
		return nil, nil, err
	}

	horizon := make([]string, 0, len(live))
	for id := range live {
		horizon = append(horizon, id)
	}

	sort.Strings(horizon)

	return condemned, horizon, nil
}

// liveSessions are the sessions that began and have not ended. A session
// the index still holds is live even with an end marker, since its commit
// may not have reached the index yet.
func (r *gcRun) liveSessions() (map[string]bool, error) {
	begun, err := r.ps.markerIDs(SessionBeginExt)
	if err != nil {
		return nil, err
	}

	ended, err := r.ps.markerIDs(SessionEndExt)
	if err != nil {
		return nil, err
	}

	indexed, err := r.ps.index.ListSessions()
	if err != nil {
		return nil, err
	}

	live := make(map[string]bool, len(begun)+len(indexed))
	for id := range begun {
		if !ended[id] {
			live[id] = true
		}
	}

	for _, s := range indexed {
		live[s.ID] = true
	}

	return live, nil
}

func prefixOf(hash []byte) uint64 {
	var prefix uint64
	for i := 0; i < 8 && i < len(hash); i++ {
		prefix = prefix<<8 | uint64(hash[i])
	}

	return prefix
}
