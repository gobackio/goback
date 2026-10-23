package sql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"sort"
	"time"

	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/file"

	entsql "entgo.io/ent/dialect/sql"
)

// MeasureSets stores the sizes of every set whose live commits changed
// since they were last measured, or that was never measured, which
// ListSets, QuerySets and GetSet then report. It returns the sets it
// measured as those describe them.
func (x *Index) MeasureSets(ctx context.Context) ([]index.SetInfo, error) {
	rows, err := x.client.Set.Query().All(ctx)
	if err != nil {
		return nil, err
	}

	commits, err := x.client.CommitRow.Query().Where(liveCommit()).
		Order(ent.Asc(commitrow.FieldReceivedAt), ent.Asc(commitrow.FieldID)).
		Select(commitrow.FieldSetID, commitrow.FieldReceivedAt, commitrow.FieldLogicalSize).All(ctx)
	if err != nil {
		return nil, err
	}

	bySet := make(map[int64][]*ent.CommitRow, len(rows))
	for _, c := range commits {
		bySet[c.SetID] = append(bySet[c.SetID], c)
	}

	var measured []index.SetInfo
	for _, row := range rows {
		live := bySet[row.ID]

		digest := sizesDigest(live)
		if bytes.Equal(row.SizesDigest, digest) {
			continue
		}

		var latest int64
		if len(live) > 0 {
			latest = deref(live[len(live)-1].LogicalSize)
		}

		unique, err := x.uniqueSize(ctx, row.ID, live)
		if err != nil {
			return measured, err
		}

		updated, err := x.client.Set.UpdateOneID(row.ID).SetLogicalSize(latest).SetUniqueSize(unique).
			SetSizesDigest(digest).Save(ctx)
		if ent.IsNotFound(err) {
			continue
		}

		if err != nil {
			return measured, err
		}

		measured = append(measured, m.Set(updated))
	}

	return measured, nil
}

// sizesDigest identifies the live commits a set's sizes count: a commit
// added, retired, tombstoned, deleted or brought back, or its size
// filled in, changes it.
func sizesDigest(live []*ent.CommitRow) []byte {
	h := sha256.New()
	for _, c := range live {
		_ = binary.Write(h, binary.BigEndian, [2]int64{int64(c.ID), deref(c.LogicalSize)})
	}

	return h.Sum(nil)
}

// uniqueSize sums, once per file object, the set's file versions one of
// the live commits holds, which are in receipt order. It streams the
// versions in ref order, which the files_versions index covers.
func (x *Index) uniqueSize(ctx context.Context, setID int64, live []*ent.CommitRow) (int64, error) {
	held := func(from time.Time, until sql.NullTime) bool {
		i := sort.Search(len(live), func(i int) bool { return !live[i].ReceivedAt.Before(from) })
		return i < len(live) && (!until.Valid || live[i].ReceivedAt.Before(until.Time))
	}

	d := entsql.Dialect(x.dialect)
	t := d.Table(file.Table)
	query, args := d.Select(t.C(file.FieldRef), t.C(file.FieldSize), t.C(file.FieldValidFrom), t.C(file.FieldValidUntil)).From(t).
		Where(entsql.And(entsql.EQ(t.C(file.FieldSetID), setID), entsql.NotNull(t.C(file.FieldRef)))).
		OrderBy(t.C(file.FieldRef)).Query()

	rows, err := x.client.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var (
		sum, largest, size int64
		current, ref       []byte
		from               time.Time
		until              sql.NullTime
	)

	for rows.Next() {
		if err := rows.Scan(&ref, &size, &from, &until); err != nil {
			return 0, err
		}

		if !bytes.Equal(ref, current) {
			sum += largest
			largest = 0
			current = append(current[:0], ref...)
		}

		if size > largest && held(from, until) {
			largest = size
		}
	}

	return sum + largest, rows.Err()
}
