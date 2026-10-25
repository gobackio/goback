package pack

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/bits-and-blooms/bitset"
	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type compactionGroup struct {
	candidates []*archive
	total      uint64

	// keep, when set, decides whether a candidate's object survives the
	// rewrite; marked, when set, says whether a copy found elsewhere was
	// reachable and may replace the candidate's.
	keep   func(candidate *archive, hdr *proto.ObjectHeader) bool
	marked func(loc *IndexLocation) bool
	// classes, when set, numbers the output classes of the rewrite, and
	// classed gives the class of each record of a candidate, nil for one
	// it has none for; each class is written apart.
	classes *classes
	classed func(a *archive) *classed
	// progress counts the candidates rewritten and their bytes.
	progress *progress.Phase

	droppedObjects uint64
	droppedBytes   uint64
	copiedBytes    uint64

	// counted as the rewrite goes, under its mutex, so a stopped one still
	// reports what it finished
	retired, written           int
	retiredBytes, writtenBytes uint64
	moved, superseded          uint64
}

// CompactReport summarizes one Compact. Alongside an error it counts what
// the compaction finished before it stopped.
type CompactReport struct {
	// Waiting is the generation whose published plan has not been
	// rewritten yet; a compaction that waits does nothing else.
	Waiting uint64
	// Candidates counts the small committed archives Compact found, and
	// CandidateBytes what they take up; it merges them only once they add
	// up to a batch or are more than MinimumCandidates.
	Candidates     int
	CandidateBytes uint64
	// Rewritten counts the candidates rewritten and retired, and ReadBytes
	// what they took up.
	Rewritten int
	ReadBytes uint64
	// Written counts the archives written, and WrittenBytes what they take
	// up.
	Written      int
	WrittenBytes uint64
	// Moved counts the objects copied into the written archives, and
	// CopiedBytes their bytes.
	Moved       uint64
	CopiedBytes uint64
	// Superseded counts the objects not copied because another copy,
	// outside the candidates or written by the same run, stands in for them.
	Superseded uint64
	// ReclaimedBytes is ReadBytes less WrittenBytes, or zero.
	ReclaimedBytes uint64
	Duration       time.Duration
}

// Summary renders the report as a line for a log or an operator.
func (r *CompactReport) Summary() string {
	if r.Waiting > 0 {
		return fmt.Sprintf("Compaction waits for the rewrite of generation %d", r.Waiting)
	}

	if r.Rewritten == 0 {
		return fmt.Sprintf("Compaction merged nothing: %d small archives (%s)", r.Candidates, humanize.Bytes(r.CandidateBytes))
	}

	return fmt.Sprintf("Compaction rewrote %d of %d small archives (%s) into %d (%s): moved %d objects (%s), %d superseded, reclaimed %s, %s",
		r.Rewritten, r.Candidates, humanize.Bytes(r.ReadBytes), r.Written, humanize.Bytes(r.WrittenBytes), r.Moved, humanize.Bytes(r.CopiedBytes),
		r.Superseded, humanize.Bytes(r.ReclaimedBytes), r.Duration.Round(time.Millisecond))
}

// Compact merges the small committed archives into archives at the root
// once they add up to a batch, or are more than MinimumCandidates, and
// returns what it did when it is done. It writes blobs and the other
// objects into archives of their own, and what the candidates' last mark
// expected to die about the same time together, each too large to be
// merged again unless the candidates held too little of it. While a
// published plan waits for its rewrite it does nothing and names the plan's
// generation in Waiting.
func (ps *PackStorage) Compact(ctx context.Context) (*CompactReport, error) {
	started := time.Now()

	ps.compactorMtx.Lock()
	defer ps.compactorMtx.Unlock()

	pending, err := ps.PendingPlans()
	if err != nil {
		return nil, errors.Wrap(err, "listing published plans")
	}

	if len(pending) > 0 {
		return &CompactReport{Waiting: pending[0], Duration: time.Since(started)}, nil
	}

	group := &compactionGroup{}

	ps.mtx.RLock()
	for _, candidate := range ps.archives {
		candidate.mtx.RLock()
		eligible := candidate.readOnly && candidate.state == ArchiveCommitted && candidate.size < ps.compaction.small()
		candidate.mtx.RUnlock()

		if !eligible {
			continue
		}

		group.candidates = append(group.candidates, candidate)
		group.total += candidate.size
	}
	ps.mtx.RUnlock()

	report := &CompactReport{Candidates: len(group.candidates), CandidateBytes: group.total}

	if len(group.candidates) > ps.compaction.MinimumCandidates || group.total >= ps.compaction.batch() {
		ps.logger.Info("compacting archives", "count", len(group.candidates), "size", humanize.Bytes(group.total))

		group.progress = progress.Start(ctx, progress.OpCompact, progress.PhaseRewrite, int64(len(group.candidates)), int64(group.total))
		err := ps.compactGroup(ctx, group)

		group.report(report)
		report.Duration = time.Since(started)

		if err != nil {
			return report, err
		}

		group.progress.Finish()
	}

	report.Duration = time.Since(started)

	return report, nil
}

// report fills in what the group's rewrite did.
func (g *compactionGroup) report(r *CompactReport) {
	r.Rewritten, r.ReadBytes = g.retired, g.retiredBytes
	r.Written, r.WrittenBytes = g.written, g.writtenBytes
	r.Moved, r.CopiedBytes, r.Superseded = g.moved, g.copiedBytes, g.superseded

	if r.ReadBytes > r.WrittenBytes {
		r.ReclaimedBytes = r.ReadBytes - r.WrittenBytes
	}
}

// compactionChunk is how many candidates a rewrite takes at a time unless
// configured otherwise. A chunk's inputs are retired as soon as it is
// done, so a rewrite that is stopped keeps what it finished.
const compactionChunk = 1000

// chunkRecords and chunkOutputs bound a chunk of classed candidates: a
// chunk keeps state for each record it reads and an upload buffer for
// each output it holds open.
var (
	chunkRecords = 1 << 19
	chunkOutputs = 32
)

// rewriteAfterChunk, when set, runs after every chunk; tests use it to
// interrupt a rewrite.
var rewriteAfterChunk func(chunk int) error

// chunks splits the candidates, in order, into the runs a rewrite takes
// one at a time: at most size archives each, and, where the candidates
// are classed, at most chunkRecords records and chunkOutputs outputs
// unless a single archive holds more.
func (g *compactionGroup) chunks(size int, small uint64) [][]*archive {
	var out [][]*archive
	var chunk []*archive
	var records int
	var sizes map[int32]uint64

	for _, a := range g.candidates {
		var cl *classed
		if g.classes != nil && g.classed != nil {
			cl = g.classed(a)
		}

		grown := sizes
		if cl != nil {
			grown = maps.Clone(sizes)
			if grown == nil {
				grown = make(map[int32]uint64, len(cl.Bytes))
			}

			for id, n := range cl.Bytes {
				grown[id] += n
			}
		}

		fits := len(chunk) < size
		if fits && cl != nil {
			fits = records+len(cl.Class) <= chunkRecords && g.classes.outputs(grown, small) <= chunkOutputs
		}

		if len(chunk) > 0 && !fits {
			out = append(out, chunk)
			chunk, records, sizes = nil, 0, nil

			if cl != nil {
				grown = maps.Clone(cl.Bytes)
			}
		}

		chunk = append(chunk, a)
		sizes = grown

		if cl != nil {
			records += len(cl.Class)
		}
	}

	if len(chunk) > 0 {
		out = append(out, chunk)
	}

	return out
}

// maxWorkers bounds the default number of workers: each holds an open
// output archive and a lookup's rows, which a many-core host would
// otherwise multiply into gigabytes.
const maxWorkers = 16

// lookupBatch is how many objects one lookup of a rewrite asks for.
const lookupBatch = 1000

const (
	defaultSmall = 16 << 20
	defaultBatch = 256 << 20
)

func (c CompactionConfig) small() uint64 {
	if c.Small == 0 {
		return defaultSmall
	}

	return c.Small
}

func (c CompactionConfig) batch() uint64 {
	if c.Batch == 0 {
		return defaultBatch
	}

	return c.Batch
}

// workersFor is at most workers, and few enough that each writes at least
// four times what counts as small out of total: an output that came out
// small would be merged again by the next Compact.
func (c CompactionConfig) workersFor(total uint64, workers int) int {
	return max(1, min(workers, int(total/(4*c.small()))))
}

func sizeOf(archives []*archive) uint64 {
	var total uint64
	for _, a := range archives {
		total += a.size
	}

	return total
}

// compactGroup rewrites the group's candidates, several at once. An object
// with a usable copy committed outside the group is dropped; the rest is
// copied into root archives with its timestamp kept.
func (ps *PackStorage) compactGroup(ctx context.Context, group *compactionGroup) error {
	ctx, span := tracer.Start(ctx, "PackStorage.rewrite")
	defer span.End()

	rw := &rewrite{started: time.Now(), ps: ps, group: group, inGroup: make(map[string]bool, len(group.candidates)),
		open: make(map[outputKey]*rewriteOutput)}
	for _, candidate := range group.candidates {
		rw.inGroup[candidate.name] = true
	}

	workers := ps.compaction.Workers
	if workers <= 0 {
		workers = min(4*runtime.GOMAXPROCS(0), maxWorkers)
	}

	size := ps.compaction.Chunk
	if size <= 0 {
		size = compactionChunk
	}

	chunks := group.chunks(size, ps.compaction.small())

	span.SetAttributes(attribute.Int("candidates", len(group.candidates)), attribute.Int("workers", workers), attribute.Int("chunks", len(chunks)))

	for n, chunk := range chunks {
		if err := rw.chunk(ctx, chunk, ps.compaction.workersFor(sizeOf(chunk), workers)); err != nil {
			span.RecordError(err)
			return err
		}

		if rewriteAfterChunk != nil {
			if err := rewriteAfterChunk(n); err != nil {
				return err
			}
		}
	}

	span.SetAttributes(attribute.Int64("dropped_objects", int64(group.droppedObjects)), attribute.Int64("copied_bytes", int64(group.copiedBytes)))

	ps.logger.Info("rewrote archives", "archives", len(group.candidates), "dropped", group.droppedObjects, "saved", humanize.Bytes(group.droppedBytes),
		"copied", humanize.Bytes(group.copiedBytes), "took", time.Since(rw.started).Round(time.Second))

	return nil
}

// rewrite is the state the workers of one compactGroup share.
type rewrite struct {
	ps      *PackStorage
	group   *compactionGroup
	inGroup map[string]bool

	started time.Time

	mtx      sync.Mutex
	written  map[string]bool
	outputs  []*archive
	obsolete []*archive
	done     int
	// unmarked names the copied objects their input's last mark result
	// left unreachable, with that result's generation and DeadSince, and
	// deaths those it expected to die
	unmarked map[string]unmarkedIn
	deaths   map[string]deathClass
	// blobCopies holds every copy of the blobs of the chunk that have
	// more than one, where a sealed copy may shadow a verified one
	blobCopies map[string][]IndexLocation
	// tombs holds the newest record of each tombstone the chunk keeps, which
	// it writes once every candidate is read: two tombstones of one ref
	// differ by their versions
	tombs map[string]*heldTomb
	// open holds the outputs being written; class is the classes of the
	// chunk being written, and writers how many outputs each class shares
	// out among the workers when more than one
	open    map[outputKey]*rewriteOutput
	class   func(candidate *archive, pos int) int32
	writers map[int32]int
}

// outputKey names an open output: a class of the chunk and the writer of
// it a worker writes with. Blobs and other records never share an output.
type outputKey struct {
	class  int32
	writer int
	data   bool
}

// shares splits the chunk into at most workers runs of about the same size,
// one per worker, each writing its own outputs.
func shares(chunk []*archive, workers int) [][]int {
	var left uint64
	for _, a := range chunk {
		left += a.size
	}

	workers = max(1, min(workers, len(chunk)))
	out := make([][]int, 0, workers)

	var share []int
	var taken uint64

	for i, a := range chunk {
		share = append(share, i)
		taken += a.size

		// each share aims at an equal part of what the earlier ones left
		if remaining := uint64(workers - len(out)); remaining > 1 && taken*remaining >= left {
			out = append(out, share)
			left -= taken
			share, taken = nil, 0
		}
	}

	if len(share) > 0 {
		out = append(out, share)
	}

	return out
}

// chunk rewrites some of the group's candidates and retires them.
func (rw *rewrite) chunk(ctx context.Context, chunk []*archive, workers int) error {
	rw.blobCopies = make(map[string][]IndexLocation)

	indexes, standIn, err := rw.lookUp(ctx, chunk, workers)
	if err != nil {
		return err
	}

	rw.written, rw.unmarked, rw.deaths = make(map[string]bool), make(map[string]unmarkedIn), make(map[string]deathClass)
	rw.tombs = make(map[string]*heldTomb)

	if rw.group.classes != nil {
		rw.class, rw.writers = chunkClasses(rw.group.classes, rw.ps.compaction.small(), chunk, rw.group.classed), nil
	} else {
		rw.class, rw.writers = rw.deathClasses(chunk, indexes, workers)
	}

	grp, gctx := errgroup.WithContext(ctx)

	held := semaphore.NewWeighted(prefetchBytes)

	for worker, share := range shares(chunk, workers) {
		grp.Go(func() error {
			inputs := make([]*archive, len(share))
			for n, i := range share {
				inputs[n] = chunk[i]
			}

			for n, read := range prefetch(gctx, inputs, held) {
				if err := gctx.Err(); err != nil {
					return err
				}

				input := <-read
				err := input.err
				if err == nil {
					err = rw.candidate(gctx, worker, inputs[n], indexes[share[n]], standIn, input.data)
				}

				input.release()

				if err != nil {
					return err
				}
			}

			return gctx.Err()
		})
	}

	if err := grp.Wait(); err != nil {
		rw.abort()
		return err
	}

	if err := rw.writeTombs(ctx); err != nil {
		rw.abort()
		return err
	}

	if err := rw.finish(ctx, workers); err != nil {
		return err
	}

	rw.mtx.Lock()
	obsolete, outputs := rw.obsolete, rw.outputs
	rw.obsolete, rw.outputs = nil, nil
	rw.mtx.Unlock()

	if err := rw.ps.carryErasureClock(obsolete, outputs, rw.unmarked, rw.deaths); err != nil {
		return err
	}

	byName := make(map[string]IndexFile, len(chunk))
	for i, a := range chunk {
		byName[a.name] = indexes[i]
	}

	retired, retiredBytes := rw.ps.retireRewritten(obsolete, byName)

	rw.mtx.Lock()
	rw.group.retired += retired
	rw.group.retiredBytes += retiredBytes
	rw.mtx.Unlock()

	return nil
}

// deathClasses classes the records of a chunk no sweep classed by kind
// and by how soon their input's last mark expected them to die, folded as
// a sweep folds its classes, and shares each class out among as many
// writers as its bytes fill.
func (rw *rewrite) deathClasses(chunk []*archive, indexes []IndexFile, workers int) (func(candidate *archive, pos int) int32, map[int32]int) {
	c := newClasses(nil)
	held := make(map[*archive]*classed, len(chunk))
	sizes := make(map[int32]uint64)

	for i, a := range chunk {
		mark := a.gcResult()
		cl := &classed{Class: make([]int32, len(indexes[i]))}

		for pos := range indexes[i] {
			rec := &indexes[i][pos]

			death := never
			if mark != nil {
				death = mark.death(pos)
			}

			id := c.id(outputClass{Data: isData(rec.Type), Death: death})
			cl.Class[pos] = id
			sizes[id] += uint64(rec.Length)
		}

		held[a] = cl
	}

	into := c.fold(sizes, rw.ps.compaction.small())

	folded := make(map[int32]uint64)
	for id, n := range sizes {
		folded[into[id]] += n
	}

	writers := make(map[int32]int, len(folded))
	for id, n := range folded {
		writers[id] = rw.ps.compaction.workersFor(n, workers)
	}

	return func(candidate *archive, pos int) int32 {
		cl := held[candidate]
		if cl == nil || pos < 0 || pos >= len(cl.Class) {
			return -1
		}

		return into[cl.Class[pos]]
	}, writers
}

// lookUp reads the indexes of the chunk and works out once for every
// object they hold whether a copy outside the group may stand in for it.
// A ref in the result has such a copy; for a tombstone it is the newest
// such copy's version, as only a tombstone at least as new stands in.
func (rw *rewrite) lookUp(ctx context.Context, chunk []*archive, workers int) ([]IndexFile, map[string]*Version, error) {
	indexes := make([]IndexFile, len(chunk))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(workers)

	for i, candidate := range chunk {
		grp.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			idx, err := candidate.getIndex()
			if err != nil {
				return errors.Wrapf(err, "reading the index of %s", candidate.name)
			}

			indexes[i] = idx

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, nil, err
	}

	seen := make(map[refKey]bool)
	tombs := make(map[refKey]bool)
	var refs []*proto.Ref

	for _, idx := range indexes {
		for i := range idx {
			key := keyOf(idx[i].Sum[:])
			if !seen[key] {
				seen[key] = true
				refs = append(refs, &proto.Ref{Hash: idx[i].Sum[:]})
			}

			// an index too old to record types may hold tombstones anywhere
			if t := proto.ObjectType(idx[i].Type); t == proto.ObjectType_TOMBSTONE || t == proto.ObjectType_INVALID {
				tombs[key] = true
			}
		}
	}

	var mtx sync.Mutex
	standIn := make(map[string]*Version)

	grp, gctx = errgroup.WithContext(ctx)
	grp.SetLimit(workers)

	for start := 0; start < len(refs); start += lookupBatch {
		batch := refs[start:min(start+lookupBatch, len(refs))]

		grp.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			copies, err := rw.ps.index.LocateCopies(batch, Scope{})
			if err != nil {
				return errors.Wrap(err, "locating the objects of a rewrite")
			}

			found := make(map[string]*Version)
			shared := make(map[string][]IndexLocation)

			for _, ref := range batch {
				if all := copies[string(ref.Hash)]; len(all) > 1 && proto.ObjectType(all[0].Record.Type) == proto.ObjectType_BLOB {
					shared[string(ref.Hash)] = all
				}

				locs := rw.standIns(copies[string(ref.Hash)])
				if len(locs) == 0 {
					continue
				}

				if !tombs[keyOf(ref.Hash)] {
					found[string(ref.Hash)] = nil
					continue
				}

				newest, err := rw.ps.newest(locs)
				if err != nil {
					return errors.Wrap(err, "reading the versions of the tombstones of a rewrite")
				}

				if newest != nil {
					found[string(ref.Hash)] = newest
				}
			}

			mtx.Lock()
			maps.Copy(standIn, found)
			mtx.Unlock()

			rw.mtx.Lock()
			maps.Copy(rw.blobCopies, shared)
			rw.mtx.Unlock()

			return nil
		})
	}

	return indexes, standIn, grp.Wait()
}

// retireRewritten retires the inputs of a rewrite whose outputs are
// indexed, so readers that still land on one find their copy elsewhere;
// indexes holds the index of each input by name. It returns how many it
// retired and what they took up.
func (ps *PackStorage) retireRewritten(obsolete []*archive, indexes map[string]IndexFile) (int, uint64) {
	if len(obsolete) == 0 {
		return 0, 0
	}

	names := make([]string, len(obsolete))
	for i, archive := range obsolete {
		names[i] = archive.name
	}

	// stored before the index forgets them, so no process loads them again
	if err := ps.quarantineArchives(names, time.Now()); err != nil {
		ps.logger.Warn("retiring obsolete archives failed", "archives", len(names), "err", err)
		return 0, 0
	}

	var retired []IndexFile
	var size uint64

	for _, archive := range obsolete {
		size += archive.size
		ps.retireArchive(archive)

		if e := archive.Close(); e != nil {
			ps.logger.Warn("closing obsolete archive failed", "archive", archive.name, "err", e)
		}

		retired = append(retired, indexes[archive.name])
	}

	ps.forgetArchives(names)
	ps.forgetCached(context.Background(), retired)

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	// a restore needs neither: the index file says the archive was
	// committed
	for _, name := range names {
		for _, ext := range []string{GCExt, CommittedExt} {
			grp.Go(func() error {
				if err := ps.storage.Delete(name + ext); err != nil && !notExist(err) {
					ps.logger.Warn("deleting a file of a retired archive failed", "file", name+ext, "err", err)
				}

				return nil
			})
		}
	}

	_ = grp.Wait()

	ps.archivesDeleted(names)

	return len(names), size
}

// forgetArchives deletes the index rows of retired archives, and of those
// an earlier call failed to, keeping all of them to try again when it
// fails too.
func (ps *PackStorage) forgetArchives(names []string) {
	ps.forgetMtx.Lock()
	defer ps.forgetMtx.Unlock()

	all := slices.Clone(names)
	for name := range ps.unforgotten {
		all = append(all, name)
	}

	slices.Sort(all)
	all = slices.Compact(all)

	if len(all) == 0 {
		return
	}

	if err := ps.index.DeleteArchives(all); err != nil {
		ps.logger.Warn("removing the index rows of retired archives failed", "archives", len(all), "err", err)

		if ps.unforgotten == nil {
			ps.unforgotten = make(map[string]bool, len(all))
		}

		for _, name := range all {
			ps.unforgotten[name] = true
		}

		return
	}

	ps.unforgotten = nil
}

// progressEvery is how many rewritten archives pass between progress logs.
const progressEvery = 500

// claim reports whether the caller is the first to write the object, and
// records the mark the input's last generation gave it and the death it
// expected.
func (rw *rewrite) claim(hash []byte, mark *gcFile, pos int) bool {
	rw.mtx.Lock()
	defer rw.mtx.Unlock()

	if rw.written[string(hash)] {
		return false
	}

	rw.written[string(hash)] = true

	if mark == nil || pos < 0 {
		return true
	}

	if !mark.Current.Test(uint(pos)) {
		rw.unmarked[string(hash)] = unmarkedIn{generation: mark.Generation, since: mark.DeadSince}
	}

	if death := mark.death(pos); death != never {
		rw.deaths[string(hash)] = death
	}

	return true
}

// shadows weighs the blob of hdr at offset in candidate against its other
// copies: a sealed one is shadowed, and left behind, where a verified copy
// of its ref is rewritten with it or stands in outside the group, and a
// verified one is kept unless a verified copy stands in for it. A copy
// whose kind cannot be read counts as neither.
func (rw *rewrite) shadows(candidate *archive, offset uint32, hdr *proto.ObjectHeader) (shadowed, keepPlain bool) {
	if hdr.Type != proto.ObjectType_BLOB {
		return false, false
	}

	rw.mtx.Lock()
	copies := rw.blobCopies[string(hdr.Ref.Hash)]
	rw.mtx.Unlock()

	var others []IndexLocation
	for _, loc := range copies {
		if loc.Archive != candidate.name || loc.Record.Offset != offset {
			others = append(others, loc)
		}
	}

	if len(others) == 0 {
		return false, false
	}

	var inGroup []IndexLocation
	for _, loc := range others {
		if rw.inGroup[loc.Archive] {
			inGroup = append(inGroup, loc)
		}
	}

	outside := rw.standIns(others)

	if hdr.Encryption != proto.Encryption_PLAINTEXT {
		return rw.anyPlain(inGroup) || rw.anyPlain(outside), false
	}

	return false, !rw.anyPlain(outside)
}

func (rw *rewrite) anyPlain(copies []IndexLocation) bool {
	for _, loc := range copies {
		if plain, err := rw.ps.plainAt(loc); err == nil && plain {
			return true
		}
	}

	return false
}

// standIns returns the copies outside the group that may stand in for the
// candidate's.
func (rw *rewrite) standIns(copies []IndexLocation) []IndexLocation {
	var out []IndexLocation

	for i := range copies {
		if rw.inGroup[copies[i].Archive] {
			continue
		}

		if rw.group.marked == nil || rw.group.marked(&copies[i]) {
			out = append(out, copies[i])
		}
	}

	return out
}

// output is the open output the worker writes the record of type t at
// pos of candidate into.
func (rw *rewrite) output(worker int, candidate *archive, pos int, t uint32) *rewriteOutput {
	key := outputKey{class: -1, data: isData(t)}
	if rw.class != nil {
		key.class = rw.class(candidate, pos)
	}

	if n := rw.writers[key.class]; n > 1 {
		key.writer = worker % n
	}

	rw.mtx.Lock()
	defer rw.mtx.Unlock()

	out := rw.open[key]
	if out == nil {
		out = &rewriteOutput{rw: rw}
		rw.open[key] = out
	}

	return out
}

// finish closes and indexes every open output.
func (rw *rewrite) finish(ctx context.Context, workers int) error {
	rw.mtx.Lock()
	open := rw.open
	rw.open = make(map[outputKey]*rewriteOutput)
	rw.mtx.Unlock()

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(workers)

	for _, out := range open {
		grp.Go(func() error {
			if err := gctx.Err(); err != nil {
				out.abort()
				return err
			}

			return out.finish()
		})
	}

	return grp.Wait()
}

func (rw *rewrite) abort() {
	rw.mtx.Lock()
	defer rw.mtx.Unlock()

	for _, out := range rw.open {
		out.abort()
	}

	rw.open = make(map[outputKey]*rewriteOutput)
}

// heldTomb is a tombstone record a chunk keeps, read from the candidate at
// pos by the worker.
type heldTomb struct {
	worker    int
	candidate *archive
	pos       int
	hdr       *proto.ObjectHeader
	bytes     []byte
	length    uint32
	version   Version
}

// holdTomb keeps the tombstone record unless the chunk holds one of its
// ref at least as new, and reports whether it took the place of another.
func (rw *rewrite) holdTomb(held *heldTomb) (kept, replaced bool) {
	rw.mtx.Lock()
	defer rw.mtx.Unlock()

	key := string(held.hdr.Ref.Hash)

	prior := rw.tombs[key]
	if prior != nil && !prior.version.Before(held.version) {
		return false, false
	}

	rw.tombs[key] = held

	return true, prior != nil
}

// writeTombs writes the tombstones the chunk kept into the outputs.
func (rw *rewrite) writeTombs(ctx context.Context) error {
	rw.mtx.Lock()
	tombs := rw.tombs
	rw.tombs = make(map[string]*heldTomb)
	rw.mtx.Unlock()

	for _, held := range tombs {
		if err := ctx.Err(); err != nil {
			return err
		}

		rw.claim(held.hdr.Ref.Hash, held.candidate.gcResult(), held.pos)

		if err := rw.put(ctx, held.worker, held.candidate, held.pos, held.hdr, held.bytes, held.version); err != nil {
			return err
		}

		rw.mtx.Lock()
		rw.group.copiedBytes += uint64(held.length)
		rw.group.moved++
		rw.mtx.Unlock()
	}

	return nil
}

// put writes a record of the candidate at pos into the worker's output.
func (rw *rewrite) put(ctx context.Context, worker int, candidate *archive, pos int, hdr *proto.ObjectHeader, bytes []byte, version Version) error {
	out := rw.output(worker, candidate, pos, uint32(hdr.Type))
	out.mtx.Lock()
	defer out.mtx.Unlock()

	ar, err := out.archive()
	if err != nil {
		return err
	}

	return ar.putVersioned(ctx, hdr, bytes, &version)
}

// candidate copies what survives of one candidate, whose bytes data
// holds or, when nil, the storage streams, into the worker's outputs;
// standIn names the objects a copy outside the group stands in for, as
// lookUp returns it.
func (rw *rewrite) candidate(ctx context.Context, worker int, candidate *archive, idx IndexFile, standIn map[string]*Version, data []byte) error {
	started := time.Now()
	var copied, moved, superseded uint64

	mark := candidate.gcResult()

	// records come as stored, so one the rewrite leaves behind is never
	// opened
	walk := candidate.foreachStored
	if data != nil {
		walk = func(load loadPredicate, fn func(*proto.ObjectHeader, []byte, uint32, uint32) error) error {
			return candidate.foreachStoredReader(bytes.NewReader(data), load, fn)
		}
	}

	err := walk(loadAll, func(hdr *proto.ObjectHeader, stored []byte, offset, length uint32) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if rw.group.keep != nil && !rw.group.keep(candidate, hdr) {
			atomic.AddUint64(&rw.group.droppedObjects, 1)
			atomic.AddUint64(&rw.group.droppedBytes, uint64(length))

			return nil
		}

		pos := idx.position(hdr.Ref.Hash)
		if pos < 0 {
			return errors.Errorf("object %x in archive %s is missing from its index", hdr.Ref.Hash, candidate.name)
		}

		version := idx[pos].Version(candidate.created)
		tomb := hdr.Type == proto.ObjectType_TOMBSTONE

		shadowed, keepPlain := rw.shadows(candidate, offset, hdr)
		if shadowed {
			superseded++
			return nil
		}

		if newer, ok := standIn[string(hdr.Ref.Hash)]; ok && !keepPlain && (!tomb || newer != nil && !newer.Before(version)) {
			superseded++
			return nil
		}

		bytes, err := openAtRest(candidate.atRest, hdr, stored)
		if err != nil {
			return errors.Wrapf(err, "object %x in archive %s", hdr.Ref.Hash, candidate.name)
		}

		// a copy between archives is a trust boundary: never carry a
		// corrupted payload forward under a valid ref
		if err := proto.VerifyStored(hdr, bytes); err != nil {
			return errors.Wrapf(err, "object %x in archive %s", hdr.Ref.Hash, candidate.name)
		}

		if tomb {
			kept, replaced := rw.holdTomb(&heldTomb{worker: worker, candidate: candidate, pos: pos, hdr: hdr, bytes: bytes, length: length, version: version})
			if !kept || replaced {
				superseded++
			}

			return nil
		}

		if !rw.claim(hdr.Ref.Hash, mark, pos) {
			superseded++
			return nil
		}

		copied += uint64(length)
		moved++

		return rw.put(ctx, worker, candidate, pos, hdr, bytes, version)
	})
	if err != nil {
		return err
	}

	rewriteArchives.Add(ctx, 1)
	rewriteCopied.Add(ctx, int64(copied))
	rewriteDuration.Record(ctx, time.Since(started).Seconds())

	rw.mtx.Lock()
	rw.obsolete = append(rw.obsolete, candidate)
	rw.group.copiedBytes += copied
	rw.group.moved += moved
	rw.group.superseded += superseded
	rw.done++
	done, total := rw.done, len(rw.group.candidates)
	rw.mtx.Unlock()

	rw.group.progress.Add(1, int64(candidate.size))

	if done%progressEvery == 0 {
		elapsed := time.Since(rw.started)
		rw.ps.logger.Info("rewriting archives", "done", done, "of", total, "elapsed", elapsed.Round(time.Second),
			"left", (elapsed / time.Duration(done) * time.Duration(total-done)).Round(time.Second))
	}

	return nil
}

const (
	// prefetchReads bounds the inputs a worker reads ahead at once,
	// prefetchBytes what all the workers of a rewrite hold read ahead, and
	// prefetchLimit the size of an input read ahead whole.
	prefetchReads = 8
	prefetchBytes = 128 << 20
	prefetchLimit = 32 << 20
)

// prefetched is an input read ahead of the worker that rewrites it; data
// is nil for one too large to read ahead, which the worker streams.
type prefetched struct {
	data    []byte
	err     error
	release func()
}

// prefetch reads the inputs of at most prefetchLimit bytes ahead of their
// worker, prefetchReads at a time and in order, holding no more than held
// allows, and hands each over on its channel. The worker releases what it
// took once done with it.
func prefetch(ctx context.Context, inputs []*archive, held *semaphore.Weighted) []chan prefetched {
	reads := make([]chan prefetched, len(inputs))
	for i := range reads {
		reads[i] = make(chan prefetched, 1)
	}

	go func() {
		running := semaphore.NewWeighted(prefetchReads)

		for i, input := range inputs {
			weight := int64(input.size)
			if weight > prefetchLimit {
				reads[i] <- prefetched{release: func() {}}
				continue
			}

			if err := running.Acquire(ctx, 1); err != nil {
				reads[i] <- prefetched{err: err, release: func() {}}
				continue
			}

			if err := held.Acquire(ctx, weight); err != nil {
				running.Release(1)
				reads[i] <- prefetched{err: err, release: func() {}}

				continue
			}

			go func() {
				data, err := input.readAll()
				running.Release(1)

				reads[i] <- prefetched{data: data, err: err, release: func() { held.Release(weight) }}
			}()
		}
	}()

	return reads
}

// rewriteOutput is the root archive one output key is written into.
type rewriteOutput struct {
	rw   *rewrite
	mtx  sync.Mutex
	open *archive
}

func (o *rewriteOutput) archive() (*archive, error) {
	ps := o.rw.ps

	if o.open != nil && o.open.size >= ps.maxSize {
		if err := o.finish(); err != nil {
			return nil, err
		}
	}

	if o.open == nil {
		a, err := newArchive(ps.storage, "", ps.atRest, ps.logger)
		if err != nil {
			return nil, err
		}

		a.stored = ps.archiveStored
		o.open = a
	}

	return o.open, nil
}

// finish closes and indexes the open archive, if any.
func (o *rewriteOutput) finish() error {
	if o.open == nil {
		return nil
	}

	a, ps := o.open, o.rw.ps
	o.open = nil

	index, err := a.CloseWriter()
	if err != nil {
		return errors.Wrap(err, "closing compaction output")
	}

	a.created, err = a.indexCreated()
	if err != nil {
		return errors.Wrap(err, "reading when the compaction output was created")
	}

	if err := ps.index.IndexArchive(ArchiveInfo{Name: a.name, Created: a.created}, index); err != nil {
		return err
	}

	a.releaseWriteIndex()

	ps.mtx.Lock()
	ps.archives = append(ps.archives, a)
	ps.mtx.Unlock()

	o.rw.mtx.Lock()
	o.rw.outputs = append(o.rw.outputs, a)
	o.rw.group.written++
	o.rw.group.writtenBytes += a.size
	o.rw.mtx.Unlock()

	return nil
}

func (o *rewriteOutput) abort() {
	if o.open != nil {
		_ = o.open.Close()
		o.open = nil
	}
}

// unmarkedIn is the mark an object's input last gave it: the generation
// that left it unreachable and that input's DeadSince.
type unmarkedIn struct {
	generation uint64
	since      time.Time
}

// carryErasureClock seeds the outputs of a rewrite with the mark of the
// newest generation among its inputs, so that an unreachable object keeps
// counting towards its second unmarked generation and one expected to die
// keeps its death. An output's DeadSince
// is the earliest its unreachable objects carry: their input's, but no
// earlier than the object's own version, which it cannot have outlived.
// An output without unreachable objects has none. An object whose input
// was last marked in an older generation counts as reachable.
func (ps *PackStorage) carryErasureClock(inputs, outputs []*archive, unmarked map[string]unmarkedIn, deaths map[string]deathClass) error {
	var generation uint64
	var snapshot time.Time

	for _, a := range inputs {
		if g := a.gcResult(); g != nil && g.Generation > generation {
			generation, snapshot = g.Generation, g.Snapshot
		}
	}

	if generation == 0 {
		return nil
	}

	for _, a := range outputs {
		idx, err := a.getIndex()
		if err != nil {
			return errors.Wrapf(err, "reading index of %s", a.name)
		}

		seed := &gcFile{
			Generation: generation,
			Snapshot:   snapshot,
			Current:    bitset.New(uint(len(idx))).SetAll(),
		}

		for pos, rec := range idx {
			seed.Death.set(pos, len(idx), deaths[string(rec.Sum[:])])

			in, ok := unmarked[string(rec.Sum[:])]
			if !ok || in.generation != generation {
				continue
			}

			seed.Current.Clear(uint(pos))
			seed.DeadObjects++
			seed.DeadBytes += uint64(rec.Length)
			seed.Dead = append(seed.Dead, prefixOf(rec.Sum[:]))

			since := in.since
			if since.IsZero() {
				since = snapshot
			}

			if written := a.version(rec).Time; written.After(since) {
				since = written
			}

			if seed.DeadSince.IsZero() || since.Before(seed.DeadSince) {
				seed.DeadSince = since
			}
		}

		sort.Slice(seed.Dead, func(i, j int) bool { return seed.Dead[i] < seed.Dead[j] })

		if err := writeGCFile(ps.storage, a.name, seed); err != nil {
			return errors.Wrapf(err, "writing gc result of %s", a.name)
		}

		a.setGCResult(seed)
	}

	return nil
}
