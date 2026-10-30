package pack

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/gobackio/goback/progress"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// SplitOptions tunes Split.
type SplitOptions struct {
	// Age is how long ago an archive's index must have been created for
	// Split to take it; zero means 7 days.
	Age time.Duration
	// Budget bounds the bytes of the archives one Split rewrites; zero
	// means 30 GB.
	Budget uint64
	// Now is the clock; zero means time.Now.
	Now time.Time
}

const (
	defaultSplitAge    = 7 * 24 * time.Hour
	defaultSplitBudget = 30_000_000_000
)

// SplitReport summarizes one Split. Candidates counts the mixed archives
// Split found within its age and budget.
type SplitReport struct {
	CompactReport
}

// Summary renders the report as a line for a log or an operator.
func (r *SplitReport) Summary() string {
	if r.Waiting > 0 {
		return fmt.Sprintf("Splitting waits for the rewrite of generation %d", r.Waiting)
	}

	if r.Busy {
		return "Splitting left to another process maintaining the store"
	}

	if r.Rewritten == 0 {
		return "Split no archives"
	}

	return fmt.Sprintf("Split %d mixed archives (%s) into %d (%s): moved %d objects (%s), %d superseded, %s",
		r.Rewritten, humanize.Bytes(r.ReadBytes), r.Written, humanize.Bytes(r.WrittenBytes), r.Moved, humanize.Bytes(r.CopiedBytes),
		r.Superseded, r.Duration.Round(time.Millisecond))
}

// Split rewrites the committed archives that hold blobs beside other
// objects and are older than opts.Age, oldest first and at most
// opts.Budget bytes of them, into archives that hold blobs alone or no
// blob, as every rewrite writes them. A collection's mark then reads none
// of the blob archives, which can go cold. While a published plan waits
// for its rewrite it does nothing and names the plan's generation in
// Waiting, and while another process maintains the store it does nothing
// and sets Busy.
func (ps *PackStorage) Split(ctx context.Context, opts SplitOptions) (*SplitReport, error) {
	started := time.Now()

	if opts.Age <= 0 {
		opts.Age = defaultSplitAge
	}

	if opts.Budget == 0 {
		opts.Budget = defaultSplitBudget
	}

	if opts.Now.IsZero() {
		opts.Now = started
	}

	ps.maintenanceMtx.Lock()
	defer ps.maintenanceMtx.Unlock()

	lease, ctx, err := ps.leaseMaintenance(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "leasing the maintenance")
	}

	report := &SplitReport{}

	if lease == nil {
		report.Busy, report.Duration = true, time.Since(started)
		return report, nil
	}
	defer lease.release()

	if err := ps.refreshArchives(); err != nil {
		return nil, errors.Wrap(err, "refreshing the archives")
	}

	if report.Waiting, err = ps.waitingPlan(); err != nil || report.Waiting > 0 {
		report.Duration = time.Since(started)
		return report, err
	}

	group, err := ps.mixedArchives(ctx, opts.Now.Add(-opts.Age), opts.Budget)
	if err != nil {
		return nil, err
	}

	report.Candidates, report.CandidateBytes = len(group.candidates), group.total

	if len(group.candidates) > 0 {
		ps.logger.Info("splitting archives", "count", len(group.candidates), "size", humanize.Bytes(group.total))

		group.grace = ps.compaction.Grace
		group.progress = progress.Start(ctx, progress.OpSplit, progress.PhaseRewrite, int64(len(group.candidates)), int64(group.total))
		err = ps.compactGroup(ctx, group)

		group.report(&report.CompactReport)

		if err == nil {
			group.progress.Finish()
		}
	}

	report.Duration = time.Since(started)

	return report, err
}

// mixedArchives groups the committed archives whose index was created
// before before and that hold blobs beside other objects, oldest first,
// until the next would take them past budget.
func (ps *PackStorage) mixedArchives(ctx context.Context, before time.Time, budget uint64) (*compactionGroup, error) {
	type old struct {
		a       *archive
		created time.Time
		size    uint64
		mixed   bool
	}

	var olds []*old

	ps.mtx.RLock()
	for _, a := range ps.archives {
		a.mtx.RLock()
		if a.readOnly && a.state == ArchiveCommitted && !a.created.IsZero() && a.created.Before(before) {
			olds = append(olds, &old{a: a, created: a.created, size: a.size})
		}
		a.mtx.RUnlock()
	}
	ps.mtx.RUnlock()

	slices.SortFunc(olds, func(x, y *old) int { return x.created.Compare(y.created) })

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(maxWorkers)

	for _, o := range olds {
		grp.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			mixed, err := o.a.mixed()
			o.mixed = mixed

			return errors.Wrapf(err, "reading the index of %s", o.a.name)
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	group := &compactionGroup{}

	for _, o := range olds {
		if !o.mixed {
			continue
		}

		if len(group.candidates) > 0 && group.total+o.size > budget {
			break
		}

		group.candidates = append(group.candidates, o.a)
		group.total += o.size
	}

	return group, nil
}

// mixed reports whether the archive holds blobs beside other records.
func (a *archive) mixed() (bool, error) {
	scanner, err := a.scanIndex()
	if err != nil {
		return false, err
	}
	defer scanner.close()

	var data, other bool

	for !data || !other {
		rec, err := scanner.next()
		if err != nil || rec == nil {
			return false, err
		}

		if isData(rec.Type) {
			data = true
		} else {
			other = true
		}
	}

	return true, nil
}
