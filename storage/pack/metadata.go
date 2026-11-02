package pack

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// cacheReaders bounds the archives CacheMetadata reads at once.
const cacheReaders = 16

var _ backup.MetadataCacher = (*PackStorage)(nil)

// uncachedMetadata is what CacheMetadata reads of one archive: the
// metadata records the cache lacks, in offset order, or the whole archive
// when its index records no types.
type uncachedMetadata struct {
	records []locatedRecord
	stream  bool
}

// CacheMetadata implements backup.MetadataCacher over the archives closed
// to writes, reporting as the metadata phase of a rebuild. Neighbouring
// records come back in one range read.
func (ps *PackStorage) CacheMetadata(ctx context.Context) error {
	if ps.cache == nil {
		return nil
	}

	var archives []*archive

	ps.mtx.RLock()
	for _, a := range ps.archives {
		a.mtx.RLock()
		ready := a.readOnly && a.state != ArchivePending
		a.mtx.RUnlock()

		if ready {
			archives = append(archives, a)
		}
	}
	ps.mtx.RUnlock()

	plans := make([]uncachedMetadata, len(archives))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(cacheReaders)

	for i, a := range archives {
		grp.Go(func() error {
			plan, err := ps.uncachedMetadata(gctx, a)
			plans[i] = plan

			return ps.unlessGone(a, err)
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}

	var size int64
	for i, plan := range plans {
		if plan.stream {
			size += int64(archives[i].size)
		}

		for _, r := range plan.records {
			size += int64(r.rec.Length)
		}
	}

	phase := progress.Start(ctx, progress.OpReIndex, progress.PhaseMetadata, int64(len(archives)), size)

	grp, gctx = errgroup.WithContext(ctx)
	grp.SetLimit(cacheReaders)

	for i, a := range archives {
		grp.Go(func() error {
			err := ps.cacheArchive(gctx, a, plans[i], phase)
			phase.Add(1, 0)

			return ps.unlessGone(a, err)
		})
	}

	err := grp.Wait()
	phase.Finish()

	return err
}

// unlessGone is err, or nil when it comes from an archive deleted
// meanwhile, which the store no longer serves.
func (ps *PackStorage) unlessGone(a *archive, err error) error {
	if err != nil && notExist(err) {
		ps.logger.Info("skipping an archive deleted while its metadata was cached", "archive", a.name)
		return nil
	}

	return err
}

func (ps *PackStorage) uncachedMetadata(ctx context.Context, a *archive) (uncachedMetadata, error) {
	idx, err := a.getIndex()
	if err != nil {
		return uncachedMetadata{}, err
	}

	// a copy of the records to read, so the archive's whole index is freed
	var uncached IndexFile

	for i := range idx {
		t := proto.ObjectType(idx[i].Type)
		if t == proto.ObjectType_INVALID {
			return uncachedMetadata{stream: true}, nil
		}

		if !t.Metadata() {
			continue
		}

		held, err := ps.cache.Has(ctx, &proto.Ref{Hash: idx[i].Sum[:]})
		if err != nil {
			return uncachedMetadata{}, err
		}

		if !held {
			uncached = append(uncached, idx[i])
		}
	}

	plan := uncachedMetadata{records: make([]locatedRecord, len(uncached))}
	for i := range uncached {
		plan.records[i] = locatedRecord{rec: &uncached[i]}
	}

	sort.Slice(plan.records, func(i, j int) bool { return plan.records[i].rec.Offset < plan.records[j].rec.Offset })

	return plan, nil
}

// cacheArchive reads what plan names of the archive into the cache.
func (ps *PackStorage) cacheArchive(ctx context.Context, a *archive, plan uncachedMetadata, phase *progress.Phase) error {
	if plan.stream {
		err := a.foreach(func(hdr *proto.ObjectHeader) bool { return hdr.Type.Metadata() }, func(hdr *proto.ObjectHeader, bytes []byte, _, _ uint32) error {
			if !hdr.Type.Metadata() {
				return nil
			}

			obj, err := proto.ObjectFromStored(hdr, bytes)
			if err != nil {
				ps.skipUncacheable(a, hdr.Ref, err)
				return nil
			}

			return ps.cache.Put(ctx, obj)
		})
		phase.Add(0, int64(a.size))

		return err
	}

	for start := 0; start < len(plan.records); {
		if err := ctx.Err(); err != nil {
			return err
		}

		end, span := runOf(plan.records, start, runGap)
		from := int64(plan.records[start].rec.Offset)
		began := time.Now()

		buf, _, err := a.readSpan(ctx, from, span)
		if err != nil {
			return errors.Wrapf(err, "reading %d bytes at %d of %s", span, from, a.name)
		}

		latency := float64(time.Since(began)) / float64(time.Millisecond)

		for _, r := range plan.records[start:end] {
			offset := int64(r.rec.Offset) - from

			phase.Add(0, int64(r.rec.Length))

			ref := &proto.Ref{Hash: r.rec.Sum[:]}

			obj, err := a.objectFromRecord(ctx, ref, r.rec, buf[offset:offset+int64(r.rec.Length)], latency)
			if err != nil {
				ps.skipUncacheable(a, ref, err)
				continue
			}

			if err := ps.cache.Put(ctx, obj); err != nil {
				return err
			}
		}

		start = end
	}

	return nil
}

// skipUncacheable logs a record that does not decode, which a read of it
// reports again should anything reach it.
func (ps *PackStorage) skipUncacheable(a *archive, ref *proto.Ref, err error) {
	ps.logger.Warn("leaving a record that does not decode out of the metadata cache", "archive", a.name, "ref", fmt.Sprintf("%x", ref.GetHash()), "err", err)
}
