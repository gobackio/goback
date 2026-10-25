package sql

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	entschedule "github.com/gobackio/goback/index/sql/ent/schedule"

	entsql "entgo.io/ent/dialect/sql"
)

var _ schedule.Reporter = (*Index)(nil)

// ReportSchedule implements schedule.Reporter. It records each set r lists
// as r.Agent expects it, keeping since when the agent reports the set, and
// drops the sets the agent reported before and r leaves out, and those no
// agent reported within schedule.StaleAfter. A set need not exist yet.
func (x *Index) ReportSchedule(ctx context.Context, r schedule.Report) error {
	listed := make([]string, len(r.Sets))
	for i, e := range r.Sets {
		if err := e.Validate(); err != nil {
			return err
		}

		listed[i] = e.Set
	}

	now := x.now()

	return x.tx(ctx, func(tx *ent.Tx) error {
		gone := entschedule.AgentID(r.Agent)
		if len(listed) > 0 {
			gone = entschedule.And(gone, entschedule.IDNotIn(listed...))
		}

		_, err := tx.Schedule.Delete().Where(entschedule.Or(gone, entschedule.ReportedAtLT(now.Add(-schedule.StaleAfter)))).Exec(ctx)
		if err != nil {
			return err
		}

		since := map[string]time.Time{}

		err = inBatches(listed, func(batch []string) error {
			rows, err := tx.Schedule.Query().Where(entschedule.IDIn(batch...), entschedule.AgentID(r.Agent)).All(ctx)
			for _, row := range rows {
				since[row.ID] = row.Since
			}

			return err
		})
		if err != nil {
			return err
		}

		return inBatches(r.Sets, func(batch []schedule.Expectation) error {
			builders := make([]*ent.ScheduleCreate, len(batch))
			for i, e := range batch {
				first, ok := since[e.Set]
				if !ok {
					first = now
				}

				builders[i] = tx.Schedule.Create().SetID(e.Set).SetAgentID(r.Agent).SetKind(e.Kind).
					SetLongestGap(e.LongestGap).SetJitter(e.Jitter).SetCommitEvery(e.CommitEvery).
					SetSince(first).SetReportedAt(now)
			}

			return tx.Schedule.CreateBulk(builders...).OnConflictColumns(entschedule.FieldID).UpdateNewValues().Exec(ctx)
		})
	})
}

// scheduled fills in what agents expect of sets and when each received its
// newest live commit.
func (x *Index) scheduled(ctx context.Context, sets []index.SetInfo) error {
	if len(sets) == 0 {
		return nil
	}

	names := make([]string, len(sets))
	ids := make([]int64, len(sets))
	for i, s := range sets {
		names[i], ids[i] = s.Name, s.ID
	}

	reported := map[string]*schedule.Reported{}
	fresh := x.now().Add(-schedule.StaleAfter)

	err := inBatches(names, func(batch []string) error {
		rows, err := x.client.Schedule.Query().Where(entschedule.IDIn(batch...), entschedule.ReportedAtGTE(fresh)).All(ctx)
		for _, row := range rows {
			r := m.Reported(row)
			r.Since, r.At = r.Since.UTC(), r.At.UTC()
			reported[row.ID] = &r
		}

		return err
	})
	if err != nil {
		return err
	}

	newest := map[int64]time.Time{}

	err = inBatches(ids, func(batch []int64) error {
		rows, err := x.client.CommitRow.Query().
			Where(commitrow.SetIDIn(batch...), liveCommit(), noNewerLiveCommit).
			Select(commitrow.FieldSetID, commitrow.FieldReceivedAt).All(ctx)
		for _, row := range rows {
			newest[row.SetID] = row.ReceivedAt.UTC()
		}

		return err
	})
	if err != nil {
		return err
	}

	for i := range sets {
		sets[i].Schedule = reported[sets[i].Name]
		sets[i].NewestAt = newest[sets[i].ID]
	}

	return nil
}

func noNewerLiveCommit(s *entsql.Selector) {
	c := entsql.Table(commitrow.Table).As("newer")
	s.Where(entsql.NotExists(entsql.Select().From(c).Where(entsql.And(
		entsql.ColumnsEQ(c.C(commitrow.FieldSetID), s.C(commitrow.FieldSetID)),
		entsql.ColumnsGT(c.C(commitrow.FieldReceivedAt), s.C(commitrow.FieldReceivedAt)),
		liveCommitColumns(c),
	))))
}
