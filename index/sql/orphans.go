package sql

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent/archive"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/deletedref"
	"github.com/gobackio/goback/index/sql/ent/object"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	entsql "entgo.io/ent/dialect/sql"
)

// OrphanCommits lists, by ref, the commits the store's committed archives
// hold that the index has no row for and no tombstone retires.
func (x *Index) OrphanCommits(ctx context.Context) ([]index.OrphanCommit, error) {
	rows, err := x.client.Object.Query().
		Where(object.Type(uint32(proto.ObjectType_COMMIT)), object.HasArchiveWith(archive.State(int(pack.ArchiveCommitted))),
			x.refAbsentFrom(commitrow.Table, commitrow.FieldRef), x.refAbsentFrom(deletedref.Table, deletedref.FieldRef)).
		All(ctx)
	if err != nil {
		return nil, err
	}

	found := make(map[string]*index.OrphanCommit)
	var commits, tombs []*proto.Ref

	for _, row := range rows {
		orphan := found[string(row.Ref)]
		if orphan == nil {
			ref := &proto.Ref{Hash: row.Ref}
			orphan = &index.OrphanCommit{Ref: ref}
			found[string(row.Ref)] = orphan
			commits = append(commits, ref)
			tombs = append(tombs, proto.TombstoneRef(ref))
		}

		orphan.Copies++
		orphan.Bytes += int64(row.Length)
	}

	tombstoned, err := x.LocateTombstones(tombs, pack.Scope{})
	if err != nil {
		return nil, err
	}

	orphans := make([]index.OrphanCommit, 0, len(found))
	for i, tomb := range tombs {
		if len(tombstoned[string(tomb.Hash)]) == 0 {
			orphans = append(orphans, *found[string(commits[i].Hash)])
		}
	}

	sort.Slice(orphans, func(i, j int) bool { return bytes.Compare(orphans[i].Ref.Hash, orphans[j].Ref.Hash) < 0 })

	return orphans, nil
}

// DropOrphans tombstones the commits OrphanCommits lists that were
// received before before, unless a row names one by then, and returns
// them; with dryRun it only returns them. A commit the store took that is
// not indexed yet is an orphan too, which before must leave out.
func (x *Index) DropOrphans(ctx context.Context, before time.Time, dryRun bool) ([]index.OrphanCommit, error) {
	orphans, err := x.OrphanCommits(ctx)
	if err != nil {
		return nil, err
	}

	var dropped []index.OrphanCommit

	for _, orphan := range orphans {
		obj, err := x.ObjectStore.Get(ctx, orphan.Ref)
		if err != nil {
			return dropped, fmt.Errorf("reading orphan commit %x: %w", orphan.Ref.Hash, err)
		}

		if !time.Unix(0, obj.GetCommit().GetReceivedAtNs()).Before(before) {
			continue
		}

		if !dryRun {
			tombstoned, err := x.tombstoneUnindexed(ctx, proto.ObjectType_COMMIT, orphan.Ref)
			if err != nil {
				return dropped, err
			}

			if !tombstoned {
				continue
			}
		}

		dropped = append(dropped, orphan)
	}

	if dryRun || len(dropped) == 0 {
		return dropped, nil
	}

	return dropped, x.flush()
}

// refAbsentFrom keeps the objects whose ref no row of table holds in column.
func (x *Index) refAbsentFrom(table, column string) func(*entsql.Selector) {
	return func(s *entsql.Selector) {
		t := entsql.Dialect(x.dialect).Table(table)
		s.Where(entsql.NotExists(entsql.Dialect(x.dialect).Select(t.C(column)).From(t).
			Where(entsql.ColumnsEQ(t.C(column), s.C(object.FieldRef)))))
	}
}
