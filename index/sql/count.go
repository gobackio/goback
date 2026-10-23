package sql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	_ "time/tzdata"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/mapping"
	"github.com/gobackio/goback/proto"

	entsql "entgo.io/ent/dialect/sql"
)

// CountCommits implements backup.Retention.
func (x *Index) CountCommits(ctx context.Context, backupSet string, period proto.Period, from, to time.Time, zone string, deleted bool) ([]*proto.CommitCount, error) {
	starts, err := periodStarts(period, from, to, zone)
	if err != nil {
		return nil, err
	}

	setID, err := findSet(ctx, x.client, backupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	d := entsql.Dialect(x.dialect)
	t := d.Table(commitrow.Table)
	state := liveCommitColumns(t)
	if deleted {
		state = entsql.And(entsql.NotNull(t.C(commitrow.FieldDeletedAt)), entsql.IsNull(t.C(commitrow.FieldTombstonedAt)))
	}

	received := t.C(commitrow.FieldReceivedAt)
	inner := d.Select().AppendSelectExpr(periodOf(received, starts)).
		AppendSelect(t.C(commitrow.FieldID), t.C(commitrow.FieldRef), received).From(t).Where(entsql.And(
		entsql.EQ(t.C(commitrow.FieldSetID), setID),
		entsql.EQ(t.C(commitrow.FieldPartial), false),
		state,
		entsql.GTE(received, from.UTC()),
		entsql.LT(received, to.UTC()),
	)).As("received")

	ranked := d.Select("period", commitrow.FieldRef, commitrow.FieldReceivedAt).
		AppendSelectExpr(newestFirst("nth"), periodSize("n")).From(inner).As("ranked")

	query, args := d.Select("period", "n", commitrow.FieldRef, commitrow.FieldReceivedAt).From(ranked).
		Where(entsql.EQ("nth", 1)).OrderBy("period").Query()

	rows, err := x.client.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var counts []*proto.CommitCount
	for rows.Next() {
		var i, n int64
		var ref []byte
		var at time.Time
		if err := rows.Scan(&i, &n, &ref, &at); err != nil {
			return nil, err
		}

		counts = append(counts, &proto.CommitCount{
			StartNs: starts[i].UnixNano(), Count: n,
			Newest: mapping.Ref(ref), NewestReceivedAtNs: at.UnixNano(),
		})
	}

	return counts, rows.Err()
}

// newestFirst numbers the rows of each period from its newest, as the
// column as.
func newestFirst(as string) entsql.Querier {
	return entsql.ExprFunc(func(b *entsql.Builder) {
		b.WriteString("ROW_NUMBER() OVER (PARTITION BY ").Ident("period").
			WriteString(" ORDER BY ").Ident(commitrow.FieldReceivedAt).WriteString(" DESC, ").
			Ident(commitrow.FieldID).WriteString(" DESC) AS ").Ident(as)
	})
}

// periodSize is how many rows each period holds, as the column as.
func periodSize(as string) entsql.Querier {
	return entsql.ExprFunc(func(b *entsql.Builder) {
		b.WriteString("COUNT(*) OVER (PARTITION BY ").Ident("period").WriteString(") AS ").Ident(as)
	})
}

// periodOf is the index into starts of the period a receipt instant
// falls in, as the column period.
func periodOf(received string, starts []time.Time) entsql.Querier {
	return entsql.ExprFunc(func(b *entsql.Builder) {
		if len(starts) == 1 {
			b.WriteString("0 AS ").Ident("period")
			return
		}

		b.WriteString("CASE")
		for i := 1; i < len(starts); i++ {
			b.WriteString(" WHEN ").WriteString(received).WriteOp(entsql.OpLT).Arg(starts[i].UTC())
			b.WriteString(" THEN " + strconv.Itoa(i-1))
		}

		b.WriteString(" ELSE " + strconv.Itoa(len(starts)-1) + " END AS ").Ident("period")
	})
}

// periodStarts lists the first instants of the periods [from, to)
// overlaps in zone, oldest first.
func periodStarts(period proto.Period, from, to time.Time, zone string) ([]time.Time, error) {
	if from.IsZero() || to.IsZero() || !from.Before(to) {
		return nil, fmt.Errorf("%w: the range is empty", backup.ErrInvalidCount)
	}

	if zone == "Local" {
		return nil, fmt.Errorf("%w: time zone %q", backup.ErrInvalidCount, zone)
	}

	loc, err := time.LoadLocation(zone)
	if err != nil {
		return nil, fmt.Errorf("%w: time zone %q", backup.ErrInvalidCount, zone)
	}

	local := from.In(loc)
	year, month, day := local.Date()

	var nth func(i int) time.Time
	switch period {
	case proto.Period_PERIOD_YEAR:
		nth = func(i int) time.Time { return midnight(year+i, time.January, 1, loc) }
	case proto.Period_PERIOD_MONTH:
		nth = func(i int) time.Time { return midnight(year, month+time.Month(i), 1, loc) }
	case proto.Period_PERIOD_DAY:
		nth = func(i int) time.Time { return midnight(year, month, day+i, loc) }
	case proto.Period_PERIOD_HOUR:
		first := hourOf(from, loc)
		nth = func(i int) time.Time { return hourOf(first.Add(time.Duration(i)*time.Hour), loc) }
	default:
		return nil, fmt.Errorf("%w: period %v", backup.ErrInvalidCount, period)
	}

	var starts []time.Time
	for start := nth(0); start.Before(to); start = nth(len(starts)) {
		if len(starts) == backup.MaxCountPeriods {
			return nil, fmt.Errorf("%w: the range spans more than %d periods", backup.ErrInvalidCount, backup.MaxCountPeriods)
		}

		starts = append(starts, start)
	}

	return starts, nil
}

// midnight is the first instant of a day in loc, normalising the date as
// time.Date does.
func midnight(year int, month time.Month, day int, loc *time.Location) time.Time {
	t := time.Date(year, month, day, 0, 0, 0, 0, loc)
	_, _, want := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Date()

	// A clock change that skips midnight puts time.Date before the day;
	// the day then begins where that change ends the zone in force.
	if t.Day() != want {
		_, t = t.ZoneBounds()
	}

	return t
}

// hourOf is the first instant of the hour of the clock in loc that t
// falls in, which is the UTC hour only where loc's offset is whole hours.
func hourOf(t time.Time, loc *time.Location) time.Time {
	_, offset := t.In(loc).Zone()
	local := t.Unix() + int64(offset)

	return time.Unix(t.Unix()-(local%3600+3600)%3600, 0).In(loc)
}
