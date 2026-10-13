package pack

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"runtime"
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
	if r.Rewritten == 0 {
		return fmt.Sprintf("Compaction merged nothing: %d small archives (%s)", r.Candidates, humanize.Bytes(r.CandidateBytes))
	}

	return fmt.Sprintf("Compaction rewrote %d of %d small archives (%s) into %d (%s): moved %d objects (%s), %d superseded, reclaimed %s, %s",
		r.Rewritten, r.Candidates, humanize.Bytes(r.ReadBytes), r.Written, humanize.Bytes(r.WrittenBytes), r.Moved, humanize.Bytes(r.CopiedBytes),
		r.Superseded, humanize.Bytes(r.ReclaimedBytes), r.Duration.Round(time.Millisecond))
}

// Compact merges the small committed archives into archives at the root
// once they add up to a batch, or are more than MinimumCandidates, and
// returns what it did when it is done. What it writes is too large to be
// merged again.
func (ps *PackStorage) Compact(ctx context.Context) (*CompactReport, error) {
	started := time.Now()

	ps.compactorMtx.Lock()
	defer ps.compactorMtx.Unlock()

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
// four times what counts as small out of chunk: an output that came out
// small would be merged again by the next Compact.
func (c CompactionConfig) workersFor(chunk []*archive, workers int) int {
	var total uint64
	for _, a := range chunk {
		total += a.size
	}

	return max(1, min(workers, int(total/(4*c.small()))))
}

// compactGroup rewrites the group's candidates, several at once. An object
// with a usable copy committed outside the group is dropped; the rest is
// copied into root archives with its timestamp kept.
func (ps *PackStorage) compactGroup(ctx context.Context, group *compactionGroup) error {
	ctx, span := tracer.Start(ctx, "PackStorage.rewrite")
	defer span.End()

	rw := &rewrite{started: time.Now(), ps: ps, group: group, inGroup: make(map[string]bool, len(group.candidates)),
		open: make(map[int64]*rewriteOutput)}
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
		if err := rw.chunk(ctx, chunk, ps.compaction.workersFor(chunk, workers)); err != nil {
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
	// left unreachable, and that result's generation
	unmarked map[string]uint64
	// open holds the outputs being written, by class, or by worker for
	// what has none; class is the classes of the chunk being written
	open  map[int64]*rewriteOutput
	class func(candidate *archive, pos int) int32
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
	indexes, standIn, err := rw.lookUp(ctx, chunk, workers)
	if err != nil {
		return err
	}

	rw.written, rw.unmarked = make(map[string]bool), make(map[string]uint64)

	rw.class = nil
	if rw.group.classes != nil {
		rw.class = chunkClasses(rw.group.classes, rw.ps.compaction.small(), chunk, rw.group.classed)
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

	if err := rw.finish(ctx, workers); err != nil {
		return err
	}

	rw.mtx.Lock()
	obsolete, outputs := rw.obsolete, rw.outputs
	rw.obsolete, rw.outputs = nil, nil
	rw.mtx.Unlock()

	if err := rw.ps.carryErasureClock(obsolete, outputs, rw.unmarked); err != nil {
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

// lookUp reads the indexes of the chunk and works out once for every
// object they hold whether a copy outside the group may stand in for it.
func (rw *rewrite) lookUp(ctx context.Context, chunk []*archive, workers int) ([]IndexFile, map[string]bool, error) {
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
	var refs []*proto.Ref

	for _, idx := range indexes {
		for i := range idx {
			if key := keyOf(idx[i].Sum[:]); !seen[key] {
				seen[key] = true
				refs = append(refs, &proto.Ref{Hash: idx[i].Sum[:]})
			}
		}
	}

	var mtx sync.Mutex
	standIn := make(map[string]bool)

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

			mtx.Lock()
			defer mtx.Unlock()

			for _, ref := range batch {
				if rw.elsewhere(copies[string(ref.Hash)]) {
					standIn[string(ref.Hash)] = true
				}
			}

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
	marked := make([]bool, len(obsolete))
	now := time.Now()

	grp := new(errgroup.Group)
	grp.SetLimit(fileWorkers)

	// marked before the index forgets it, so no process loads it again
	for i, archive := range obsolete {
		grp.Go(func() error {
			if err := ps.quarantineArchive(archive.name, now); err != nil {
				ps.logger.Warn("retiring an obsolete archive failed", "archive", archive.name, "err", err)
				return nil
			}

			marked[i] = true

			return nil
		})
	}

	_ = grp.Wait()

	var names []string
	var retired []IndexFile
	var size uint64

	for i, archive := range obsolete {
		if !marked[i] {
			continue
		}

		size += archive.size
		ps.retireArchive(archive)

		if e := archive.Close(); e != nil {
			ps.logger.Warn("closing obsolete archive failed", "archive", archive.name, "err", e)
		}

		names = append(names, archive.name)
		retired = append(retired, indexes[archive.name])
	}

	if len(names) == 0 {
		return 0, 0
	}

	if err := ps.index.DeleteArchives(names); err != nil {
		ps.logger.Warn("removing the index rows of retired archives failed", "archives", len(names), "err", err)
	}

	ps.forgetCached(context.Background(), retired)

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

// progressEvery is how many rewritten archives pass between progress logs.
const progressEvery = 500

// claim reports whether the caller is the first to write the object, and
// records the mark the input's last generation gave it.
func (rw *rewrite) claim(hash []byte, mark *gcFile, pos int) bool {
	rw.mtx.Lock()
	defer rw.mtx.Unlock()

	if rw.written[string(hash)] {
		return false
	}

	rw.written[string(hash)] = true

	if mark != nil && pos >= 0 && !mark.Current.Test(uint(pos)) {
		rw.unmarked[string(hash)] = mark.Generation
	}

	return true
}

// elsewhere reports whether one of the copies outside the group may stand
// in for the candidate's.
func (rw *rewrite) elsewhere(copies []IndexLocation) bool {
	for i := range copies {
		if rw.inGroup[copies[i].Archive] {
			continue
		}

		if rw.group.marked == nil || rw.group.marked(&copies[i]) {
			return true
		}
	}

	return false
}

// output is the open output the worker writes the record at pos of
// candidate into.
func (rw *rewrite) output(worker int, candidate *archive, pos int) *rewriteOutput {
	key := -1 - int64(worker)
	if rw.class != nil {
		if class := rw.class(candidate, pos); class >= 0 {
			key = int64(class)
		}
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
	rw.open = make(map[int64]*rewriteOutput)
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

	rw.open = make(map[int64]*rewriteOutput)
}

// candidate copies what survives of one candidate, whose bytes data
// holds or, when nil, the storage streams, into the worker's outputs;
// standIn names the objects a copy outside the group stands in for.
func (rw *rewrite) candidate(ctx context.Context, worker int, candidate *archive, idx IndexFile, standIn map[string]bool, data []byte) error {
	started := time.Now()
	var copied, moved, superseded uint64

	mark := candidate.gcResult()

	walk := candidate.foreach
	if data != nil {
		walk = func(load loadPredicate, fn func(*proto.ObjectHeader, []byte, uint32, uint32) error) error {
			return candidate.foreachReader(bytes.NewReader(data), load, fn)
		}
	}

	err := walk(loadAll, func(hdr *proto.ObjectHeader, bytes []byte, offset, length uint32) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if rw.group.keep != nil && !rw.group.keep(candidate, hdr) {
			atomic.AddUint64(&rw.group.droppedObjects, 1)
			atomic.AddUint64(&rw.group.droppedBytes, uint64(length))

			return nil
		}

		if standIn[string(hdr.Ref.Hash)] {
			superseded++
			return nil
		}

		// a copy between archives is a trust boundary: never carry a
		// corrupted payload forward under a valid ref
		if err := proto.VerifyStored(hdr, bytes); err != nil {
			return errors.Wrapf(err, "object %x in archive %s", hdr.Ref.Hash, candidate.name)
		}

		pos := idx.position(hdr.Ref.Hash)
		if !rw.claim(hdr.Ref.Hash, mark, pos) {
			superseded++
			return nil
		}

		out := rw.output(worker, candidate, pos)
		out.mtx.Lock()
		defer out.mtx.Unlock()

		ar, err := out.archive()
		if err != nil {
			return err
		}

		copied += uint64(length)
		moved++

		version := idx[pos].Version(candidate.created)

		return ar.putVersioned(ctx, hdr, bytes, &version)
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

// rewriteOutput is the root archive one class is written into.
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

// carryErasureClock seeds the outputs of a rewrite with the mark of the
// newest generation among its inputs, so that an unreachable object keeps
// counting towards its second unmarked generation, and with the earliest
// DeadSince of the inputs, so the erasure bound keeps counting from when
// the objects first went unmarked. An object whose input was last marked
// in an older generation counts as reachable.
func (ps *PackStorage) carryErasureClock(inputs, outputs []*archive, unmarked map[string]uint64) error {
	var since time.Time
	var generation uint64
	var snapshot time.Time

	for _, a := range inputs {
		g := a.gcResult()
		if g == nil {
			continue
		}

		if g.Generation > generation {
			generation, snapshot = g.Generation, g.Snapshot
		}

		if !g.DeadSince.IsZero() && (since.IsZero() || g.DeadSince.Before(since)) {
			since = g.DeadSince
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
			if g, ok := unmarked[string(rec.Sum[:])]; !ok || g != generation {
				continue
			}

			seed.Current.Clear(uint(pos))
			seed.DeadObjects++
			seed.DeadBytes += uint64(rec.Length)
			seed.Dead = append(seed.Dead, prefixOf(rec.Sum[:]))
		}

		if seed.DeadObjects > 0 {
			sort.Slice(seed.Dead, func(i, j int) bool { return seed.Dead[i] < seed.Dead[j] })

			seed.DeadSince = since
			if seed.DeadSince.IsZero() {
				seed.DeadSince = snapshot
			}
		} else if !since.IsZero() {
			seed.DeadSince = since
		}

		if err := writeGCFile(ps.storage, a.name, seed); err != nil {
			return errors.Wrapf(err, "writing gc result of %s", a.name)
		}

		a.setGCResult(seed)
	}

	return nil
}
