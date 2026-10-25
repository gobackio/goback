package sql

import (
	"context"
	"slices"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/archive"
	"github.com/gobackio/goback/index/sql/ent/object"
	"github.com/gobackio/goback/index/sql/ent/predicate"
	"github.com/gobackio/goback/index/sql/ent/session"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
	"go.opentelemetry.io/otel/metric"
)

// objectBatch is how many object rows one insert carries.
const objectBatch = 1000

var _ pack.ClaimIndex = (*Index)(nil)

// LocateObject implements pack.ArchiveIndex: the committed archives plus
// the pending archives of the scope's session.
func (x *Index) LocateObject(ref *proto.Ref, scope pack.Scope, exclude ...string) (pack.IndexLocation, error) {
	ctx := context.Background()
	defer recordLookup(ctx, "object", time.Now())

	where := []predicate.Object{object.Ref(ref.Hash), object.HasArchiveWith(visibleTo(scope))}
	if len(exclude) > 0 {
		where = append(where, object.ArchiveIDNotIn(exclude...))
	}

	row, err := x.client.Object.Query().Where(where...).Order(ent.Asc(object.FieldID)).First(ctx)
	if ent.IsNotFound(err) {
		return pack.IndexLocation{}, pack.ErrRecordNotFound
	}

	if err != nil {
		return pack.IndexLocation{}, err
	}

	return m.Location(row), nil
}

// LocateCopies implements pack.ArchiveIndex.
func (x *Index) LocateCopies(refs []*proto.Ref, scope pack.Scope) (map[string][]pack.IndexLocation, error) {
	ctx := context.Background()
	defer recordLookup(ctx, "copies", time.Now())

	return x.locateCopies(ctx, refs, scope)
}

// LocateTombstones implements pack.ArchiveIndex through the index of
// tombstone rows alone.
func (x *Index) LocateTombstones(refs []*proto.Ref, scope pack.Scope) (map[string][]pack.IndexLocation, error) {
	ctx := context.Background()
	defer recordLookup(ctx, "tombstones", time.Now())

	return x.locateCopies(ctx, refs, scope, object.Type(uint32(proto.ObjectType_TOMBSTONE)))
}

func (x *Index) locateCopies(ctx context.Context, refs []*proto.Ref, scope pack.Scope, where ...predicate.Object) (map[string][]pack.IndexLocation, error) {
	copies := make(map[string][]pack.IndexLocation)

	for start := 0; start < len(refs); start += objectBatch {
		chunk := refs[start:min(start+objectBatch, len(refs))]

		hashes := make([][]byte, len(chunk))
		for i, ref := range chunk {
			hashes[i] = ref.Hash
		}

		rows, err := x.client.Object.Query().
			Where(append(where, object.RefIn(hashes...), object.HasArchiveWith(visibleTo(scope)))...).
			Order(ent.Asc(object.FieldID)).
			All(ctx)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			copies[string(row.Ref)] = append(copies[string(row.Ref)], m.Location(row))
		}
	}

	return copies, nil
}

// visibleTo selects the committed archives plus the pending archives of
// the scope's session.
func visibleTo(scope pack.Scope) predicate.Archive {
	visible := archive.State(int(pack.ArchiveCommitted))
	if scope.Session != "" {
		visible = archive.Or(visible, archive.And(archive.SessionID(scope.Session), archive.State(int(pack.ArchivePending))))
	}

	return visible
}

func recordLookup(ctx context.Context, lookup string, started time.Time) {
	lookupDuration.Record(ctx, float64(time.Since(started))/float64(time.Millisecond), metric.WithAttributes(keyLookup.String(lookup)))
}

// LookupArchive implements pack.ArchiveIndex.
func (x *Index) LookupArchive(name string) (pack.ArchiveInfo, bool, error) {
	row, err := x.client.Archive.Get(context.Background(), name)
	if ent.IsNotFound(err) {
		return pack.ArchiveInfo{}, false, nil
	}

	if err != nil {
		return pack.ArchiveInfo{}, false, err
	}

	return m.Archive(row), true, nil
}

// IndexArchive implements pack.ArchiveIndex; a pending archive needs a
// live session.
func (x *Index) IndexArchive(info pack.ArchiveInfo, index pack.IndexFile) error {
	ctx := context.Background()

	// the archive row and its objects land together: a known archive
	// without objects would never be indexed again
	return x.tx(ctx, func(tx *ent.Tx) error {
		if info.Session != "" {
			live, err := tx.Session.Query().Where(session.ID(info.Session)).Exist(ctx)
			if err != nil {
				return err
			}

			if !live {
				return backup.ErrNoSession
			}
		}

		exists, err := tx.Archive.Query().Where(archive.ID(info.Name)).Exist(ctx)
		if err != nil {
			return err
		}

		if exists {
			if info.Created.IsZero() {
				return nil
			}

			return tx.Archive.Update().Where(archive.ID(info.Name), archive.CreatedAtIsNil()).SetCreatedAt(info.Created).Exec(ctx)
		}

		err = tx.Archive.Create().SetID(info.Name).SetNillableSessionID(nilIfZero(info.Session)).SetState(int(info.State)).
			SetNillableCreatedAt(nilIfZeroTime(info.Created)).Exec(ctx)
		if err != nil {
			return err
		}

		return insertObjects(ctx, tx, info.Name, index)
	})
}

// insertObjects adds the object rows of an archive, in batches.
func insertObjects(ctx context.Context, tx *ent.Tx, name string, records []pack.IndexRecord) error {
	for len(records) > 0 {
		stop := min(len(records), objectBatch)

		builders := make([]*ent.ObjectCreate, stop)
		for i, record := range records[:stop] {
			builders[i] = tx.Object.Create().SetRef(record.Sum[:]).SetArchiveID(name).SetStart(record.Offset).SetLength(record.Length).SetType(record.Type)
			if record.CarriedTime != 0 {
				builders[i].SetCarriedTime(record.CarriedTime).SetCarriedOffset(record.CarriedOffset)
			}
		}

		err := tx.Object.CreateBulk(builders...).OnConflict().DoNothing().Exec(ctx)
		if err != nil {
			return err
		}

		records = records[stop:]
	}

	return nil
}

// DeleteArchives implements pack.ArchiveIndex; the objects go with the
// archives.
func (x *Index) DeleteArchives(names []string) error {
	ctx := context.Background()

	return x.tx(ctx, func(tx *ent.Tx) error {
		for start := 0; start < len(names); start += objectBatch {
			batch := names[start:min(start+objectBatch, len(names))]

			if _, err := tx.Object.Delete().Where(object.ArchiveIDIn(batch...)).Exec(ctx); err != nil {
				return err
			}

			if _, err := tx.Archive.Delete().Where(archive.IDIn(batch...)).Exec(ctx); err != nil {
				return err
			}
		}

		return nil
	})
}

// BeginSession implements pack.SessionIndex.
func (x *Index) BeginSession(s *backup.Session) error {
	create := x.client.Session.Create().SetID(s.ID).SetAgentID(s.AgentID).SetBackupSet(s.Set).
		SetStartedAt(s.Started.UTC()).SetLastSeen(s.LastSeen.UTC()).SetRestoreRef(s.Restore.GetHash())
	if s.InitialRetention != nil {
		create.SetInitialRetention(s.InitialRetention)
	}

	return create.Exec(context.Background())
}

// TouchSession implements pack.SessionIndex.
func (x *Index) TouchSession(id string, at time.Time) error {
	n, err := x.client.Session.Update().Where(session.ID(id)).SetLastSeen(at.UTC()).Save(context.Background())
	if err == nil && n == 0 {
		return backup.ErrNoSession
	}

	return err
}

// GetSession implements pack.SessionIndex.
func (x *Index) GetSession(id string) (*backup.Session, error) {
	row, err := x.client.Session.Get(context.Background(), id)
	if ent.IsNotFound(err) {
		return nil, backup.ErrNoSession
	}

	if err != nil {
		return nil, err
	}

	return m.Session(row), nil
}

// ListSessions implements pack.SessionIndex.
func (x *Index) ListSessions() ([]*backup.Session, error) {
	rows, err := x.client.Session.Query().All(context.Background())
	if err != nil {
		return nil, err
	}

	return mapAll(rows, m.Session), nil
}

// EndSession implements pack.SessionIndex: the session's uncommitted
// archives go with it.
func (x *Index) EndSession(id string) ([]string, error) {
	ctx := context.Background()

	var dropped []string

	err := x.tx(ctx, func(tx *ent.Tx) error {
		pending, err := tx.Archive.Query().Where(archive.SessionID(id), archive.StateNEQ(int(pack.ArchiveCommitted))).IDs(ctx)
		if err != nil {
			return err
		}

		dropped = pending

		uncommitted := []predicate.Archive{archive.SessionID(id), archive.StateNEQ(int(pack.ArchiveCommitted))}

		if _, err := tx.Object.Delete().Where(object.HasArchiveWith(uncommitted...)).Exec(ctx); err != nil {
			return err
		}

		if _, err := tx.Archive.Delete().Where(uncommitted...).Exec(ctx); err != nil {
			return err
		}

		_, err = tx.Session.Delete().Where(session.ID(id)).Exec(ctx)

		return err
	})

	return dropped, err
}

// MarkIncomplete implements pack.IncompleteIndex.
func (x *Index) MarkIncomplete(id string, refs []*proto.Ref) error {
	ctx := context.Background()

	for chunk := range slices.Chunk(refs, objectBatch) {
		err := x.client.Object.Update().
			Where(object.RefIn(refHashes(chunk)...), object.HasArchiveWith(archive.SessionID(id), archive.StateNEQ(int(pack.ArchiveCommitted)))).
			SetIncomplete(true).Exec(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// ClearIncomplete implements pack.IncompleteIndex.
func (x *Index) ClearIncomplete(refs []*proto.Ref) error {
	ctx := context.Background()

	for chunk := range slices.Chunk(refs, objectBatch) {
		err := x.client.Object.Update().Where(object.RefIn(refHashes(chunk)...), object.Incomplete(true)).SetIncomplete(false).Exec(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func refHashes(refs []*proto.Ref) [][]byte {
	hashes := make([][]byte, len(refs))
	for i, ref := range refs {
		hashes[i] = ref.Hash
	}

	return hashes
}

// PendingArchives implements pack.SessionIndex.
func (x *Index) PendingArchives(id string) ([]string, error) {
	return x.client.Archive.Query().Where(archive.SessionID(id), archive.State(int(pack.ArchivePending))).IDs(context.Background())
}

// CommitSession implements pack.SessionIndex: the session must still be
// live, then its pending archives become committed and its lost ones go.
func (x *Index) CommitSession(id string) error {
	ctx := context.Background()

	return x.tx(ctx, func(tx *ent.Tx) error {
		live, err := tx.Session.Query().Where(session.ID(id)).Exist(ctx)
		if err != nil {
			return err
		}

		if !live {
			return backup.ErrNoSession
		}

		lost := []predicate.Archive{archive.SessionID(id), archive.State(int(pack.ArchiveLost))}

		if _, err := tx.Object.Delete().Where(object.HasArchiveWith(lost...)).Exec(ctx); err != nil {
			return err
		}

		if _, err := tx.Archive.Delete().Where(lost...).Exec(ctx); err != nil {
			return err
		}

		return tx.Archive.Update().Where(archive.SessionID(id), archive.State(int(pack.ArchivePending))).
			SetState(int(pack.ArchiveCommitted)).ClearSessionID().Exec(ctx)
	})
}

// CountObjects implements pack.ArchiveIndex: the object rows and the
// distinct refs among them.
func (x *Index) CountObjects() (uint64, uint64, error) {
	ctx := context.Background()

	finalized := object.HasArchiveWith(archive.StateIn(int(pack.ArchiveCommitted), int(pack.ArchivePending)))

	total, err := x.client.Object.Query().Where(finalized).Count(ctx)
	if err != nil {
		return 0, 0, err
	}

	unique, err := x.client.Object.Query().Where(finalized).Unique(true).Select(object.FieldRef).Count(ctx)
	if err != nil {
		return 0, 0, err
	}

	return uint64(total), uint64(unique), nil
}

// clock is the time claims are aged by: the database's own clock on
// Postgres, which every process shares, and Now on SQLite, which only one
// machine opens.
func (x *Index) clock(ctx context.Context) (time.Time, error) {
	if !x.locking {
		return x.now().UTC(), nil
	}

	var now time.Time
	if err := x.db.QueryRowContext(ctx, "SELECT now()").Scan(&now); err != nil {
		return time.Time{}, err
	}

	return now.UTC(), nil
}

// SharedNow implements pack.SharedClock.
func (x *Index) SharedNow(ctx context.Context) (time.Time, error) {
	return x.clock(ctx)
}

// OpenArchive implements pack.ClaimIndex.
func (x *Index) OpenArchive(name, id string) error {
	ctx := context.Background()

	now, err := x.clock(ctx)
	if err != nil {
		return err
	}

	return x.tx(ctx, func(tx *ent.Tx) error {
		live, err := tx.Session.Query().Where(session.ID(id)).Exist(ctx)
		if err != nil {
			return err
		}

		if !live {
			return backup.ErrNoSession
		}

		return tx.Archive.Create().SetID(name).SetSessionID(id).SetState(int(pack.ArchiveOpen)).SetOpenedAt(now).Exec(ctx)
	})
}

// AddObjects implements pack.ClaimIndex. The archive's row is locked for
// the insert, so a finalize or an abandon cannot come between the check
// and the rows.
func (x *Index) AddObjects(name string, records []pack.IndexRecord) error {
	ctx := context.Background()

	return x.tx(ctx, func(tx *ent.Tx) error {
		query := tx.Archive.Query().Where(archive.ID(name), archive.State(int(pack.ArchiveOpen)))
		if x.locking {
			query = query.ForUpdate()
		}

		open, err := query.Exist(ctx)
		if err != nil {
			return err
		}

		if !open {
			return claimGone(ctx, tx.Archive, name)
		}

		return insertObjects(ctx, tx, name, records)
	})
}

// FinalizeArchive implements pack.ClaimIndex.
func (x *Index) FinalizeArchive(name string, within time.Duration, created time.Time) error {
	ctx := context.Background()

	now, err := x.clock(ctx)
	if err != nil {
		return err
	}

	n, err := x.client.Archive.Update().
		Where(archive.ID(name), archive.State(int(pack.ArchiveOpen)), archive.OpenedAtGT(now.Add(-within))).
		SetState(int(pack.ArchivePending)).ClearOpenedAt().SetCreatedAt(created).Save(ctx)
	if err != nil {
		return err
	}

	if n == 0 {
		return claimGone(ctx, x.client.Archive, name)
	}

	return nil
}

// claimGone is why the archive name is not open: its claim lapsed, or its
// session ended and took the row with it.
func claimGone(ctx context.Context, archives *ent.ArchiveClient, name string) error {
	known, err := archives.Query().Where(archive.ID(name)).Exist(ctx)
	if err != nil {
		return err
	}

	if !known {
		return backup.ErrNoSession
	}

	return pack.ErrClaimLapsed
}

// Holds implements pack.ClaimIndex.
func (x *Index) Holds(refs []*proto.Ref, id string) (map[string]bool, error) {
	ctx := context.Background()
	defer recordLookup(ctx, "claims", time.Now())

	held := make(map[string]bool)

	for start := 0; start < len(refs); start += objectBatch {
		chunk := refs[start:min(start+objectBatch, len(refs))]

		hashes := make([][]byte, len(chunk))
		for i, ref := range chunk {
			hashes[i] = ref.Hash
		}

		found, err := x.client.Object.Query().Where(
			object.RefIn(hashes...),
			object.HasArchiveWith(archive.SessionID(id), archive.StateIn(int(pack.ArchivePending), int(pack.ArchiveOpen))),
		).Select(object.FieldRef).Strings(ctx)
		if err != nil {
			return nil, err
		}

		for _, ref := range found {
			held[ref] = true
		}
	}

	return held, nil
}

// Claims implements pack.ClaimIndex.
func (x *Index) Claims(id string) ([]pack.Claim, error) {
	ctx := context.Background()

	rows, err := x.client.Archive.Query().Where(archive.SessionID(id), archive.State(int(pack.ArchiveOpen))).All(ctx)
	if err != nil {
		return nil, err
	}

	now, err := x.clock(ctx)
	if err != nil {
		return nil, err
	}

	claims := make([]pack.Claim, len(rows))
	for i, row := range rows {
		claims[i] = pack.Claim{Archive: row.ID}
		if row.OpenedAt != nil {
			claims[i].Age = now.Sub(*row.OpenedAt)
		}
	}

	return claims, nil
}

// Abandon implements pack.ClaimIndex.
func (x *Index) Abandon(name string, within time.Duration) (bool, error) {
	ctx := context.Background()

	now, err := x.clock(ctx)
	if err != nil {
		return false, err
	}

	n, err := x.client.Archive.Update().
		Where(archive.ID(name), archive.State(int(pack.ArchiveOpen)), archive.OpenedAtLTE(now.Add(-within))).
		SetState(int(pack.ArchiveLost)).Save(ctx)

	return n > 0, err
}

// Lost implements pack.ClaimIndex.
func (x *Index) Lost(id string) ([]*proto.Ref, error) {
	ctx := context.Background()

	lostRows, err := x.client.Object.Query().
		Where(
			object.HasArchiveWith(archive.SessionID(id), archive.State(int(pack.ArchiveLost))),
			object.TypeNEQ(uint32(proto.ObjectType_COMMIT)),
		).
		All(ctx)
	if err != nil {
		return nil, err
	}

	commits, err := x.client.Object.Query().
		Where(
			object.HasArchiveWith(archive.SessionID(id), archive.StateIn(int(pack.ArchiveLost), int(pack.ArchivePending))),
			object.Type(uint32(proto.ObjectType_COMMIT)),
		).
		Select(object.FieldRef).All(ctx)
	if err != nil {
		return nil, err
	}

	untombs := make(map[string]bool, len(commits))
	for _, row := range commits {
		untombs[string(proto.TombstoneRef(proto.TombstoneRef(&proto.Ref{Hash: row.Ref})).Hash)] = true
	}

	kept := archive.Or(
		archive.State(int(pack.ArchiveCommitted)),
		archive.And(archive.SessionID(id), archive.State(int(pack.ArchivePending))),
	)

	seen := make(map[string]bool)

	var refs [][]byte
	for _, row := range lostRows {
		if !seen[string(row.Ref)] && !untombs[string(row.Ref)] {
			seen[string(row.Ref)] = true
			refs = append(refs, row.Ref)
		}
	}

	held := make(map[string]bool)

	err = inBatches(refs, func(batch [][]byte) error {
		found, err := x.client.Object.Query().Where(object.RefIn(batch...), object.HasArchiveWith(kept)).Select(object.FieldRef).All(ctx)
		for _, row := range found {
			held[string(row.Ref)] = true
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	var lost []*proto.Ref
	for _, ref := range refs {
		if !held[string(ref)] {
			lost = append(lost, &proto.Ref{Hash: ref})
		}
	}

	return lost, nil
}
