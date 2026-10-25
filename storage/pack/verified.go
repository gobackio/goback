package pack

import (
	"context"
	"fmt"
	"sync"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// plainCacheSize bounds the copies plainCache remembers.
const plainCacheSize = 1 << 18

// plainCache remembers which copies of blobs are stored in the clear;
// a record never changes, so nothing it holds goes stale.
type plainCache struct {
	mtx sync.Mutex
	m   map[plainKey]bool
}

type plainKey struct {
	archive string
	offset  uint32
}

func (c *plainCache) get(key plainKey) (bool, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	plain, ok := c.m[key]

	return plain, ok
}

func (c *plainCache) put(key plainKey, plain bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.m == nil || len(c.m) >= plainCacheSize {
		c.m = make(map[plainKey]bool)
	}

	c.m[key] = plain
}

// wroteSealed reports whether this process wrote the archive, and then
// whether the record at offset is sealed.
func (a *archive) wroteSealed(offset uint32) (sealed, known bool) {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	if a.sealedAt == nil {
		return false, false
	}

	return a.sealedAt[offset], true
}

// headerPrefix is how much of a record readHeaderOnly reads first, more
// than the headers goback writes take.
const headerPrefix = 1 << 10

// readHeaderOnly decodes the object header of one index record, reading
// the payload only where the header is larger than headerPrefix.
func (a *archive) readHeaderOnly(loc *IndexRecord) (*proto.ObjectHeader, error) {
	length := min(int64(loc.Length), headerPrefix)

	buf, _, err := a.readSpan(int64(loc.Offset), length)
	if err != nil {
		return nil, err
	}

	size, consumed := proto.DecodeVarint(buf)
	if consumed > 0 && uint64(len(buf)-consumed) < size && length < int64(loc.Length) {
		if buf, _, err = a.readSpan(int64(loc.Offset), min(int64(loc.Length), int64(consumed)+int64(size))); err != nil {
			return nil, err
		}
	}

	header, _, err := splitRecord(buf)
	if err != nil {
		return nil, errors.Wrapf(err, "reading a header in %s", a.name)
	}

	return proto.NewObjectHeaderFromBytes(header)
}

// plain reports whether the copy rec of archive a is one the store
// verified. Only blobs are ever sealed.
func (ps *PackStorage) plain(a *archive, rec *IndexRecord) (bool, error) {
	if t := proto.ObjectType(rec.Type); t != proto.ObjectType_BLOB && t != proto.ObjectType_INVALID {
		return true, nil
	}

	if sealed, known := a.wroteSealed(rec.Offset); known {
		return !sealed, nil
	}

	key := plainKey{archive: a.name, offset: rec.Offset}
	if plain, ok := ps.plains.get(key); ok {
		return plain, nil
	}

	hdr, err := a.readHeaderOnly(rec)
	if err != nil {
		return false, err
	}

	plain := hdr.Encryption == proto.Encryption_PLAINTEXT
	ps.plains.put(key, plain)

	return plain, nil
}

// plainAt is plain for an index location, false for a copy whose archive
// is gone.
func (ps *PackStorage) plainAt(loc IndexLocation) (bool, error) {
	a, err := ps.archiveByName(loc.Archive)
	if err != nil && !errors.Is(err, errArchiveRetired) {
		return false, err
	}

	if a == nil {
		return false, nil
	}

	return ps.plain(a, &loc.Record)
}

// verifyWorkers bounds the headers HasVerified reads at once.
const verifyWorkers = 32

var _ backup.VerifiedHaser = (*PackStorage)(nil)

// HasVerified implements backup.VerifiedHaser: HasAll counting only the
// copies the store verified, so a sealed copy never answers for a ref, nor
// does an archive another process has not finalized yet.
func (ps *PackStorage) HasVerified(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	scope := ScopeOf(ctx)

	tombs := make([]*proto.Ref, 0, 2*len(refs))
	for _, ref := range refs {
		tomb := proto.TombstoneRef(ref)
		tombs = append(tombs, tomb, proto.TombstoneRef(tomb))
	}

	located, err := ps.index.LocateTombstones(tombs, scope)
	if err != nil {
		return nil, err
	}

	found, err := ps.index.LocateCopies(refs, scope)
	if err != nil {
		return nil, err
	}

	has := make([]bool, len(refs))

	grp, _ := errgroup.WithContext(ctx)
	grp.SetLimit(verifyWorkers)

	for i, ref := range refs {
		grp.Go(func() error {
			tomb, untomb := tombs[2*i], tombs[2*i+1]

			held, err := ps.holdsCopy(scope, ref, located[string(tomb.Hash)], located[string(untomb.Hash)], found[string(ref.Hash)], true)
			has[i] = held

			return err
		})
	}

	return has, grp.Wait()
}

// refuseShadow refuses a sealed blob whose ref the store holds a verified
// copy of: no sealed ref equals a plaintext one, so it can only be a
// forgery meant to stand in for that content.
func (ps *PackStorage) refuseShadow(ctx context.Context, object *proto.Object) error {
	sealed := object.GetSealed()
	if sealed == nil {
		return nil
	}

	found, err := ps.index.LocateCopies([]*proto.Ref{sealed.Ref}, ScopeOf(ctx))
	if err != nil {
		return err
	}

	pending := false
	if ws := ps.lookupWriteSession(ScopeOf(ctx).Session); ws != nil {
		ws.pendingMtx.RLock()
		_, pending = ws.pending[string(sealed.Ref.Hash)]
		ws.pendingMtx.RUnlock()
	}

	if len(found[string(sealed.Ref.Hash)]) == 0 && !pending {
		return nil
	}

	has, err := ps.HasVerified(ctx, []*proto.Ref{sealed.Ref})
	if err != nil {
		return err
	}

	if has[0] {
		return fmt.Errorf("%w: sealed blob %x: the store holds that ref in the clear", proto.ErrInvalidObject, sealed.Ref.Hash)
	}

	return nil
}

// verifiedCopy finds a copy of ref the store verified that the scope may
// read, committed or pending in its session, other than the sealed one at
// rec in served; none is a nil archive.
func (ps *PackStorage) verifiedCopy(scope Scope, ref *proto.Ref, served *archive, rec *IndexRecord) (*archive, *IndexRecord, error) {
	archives, records, err := ps.committedCopies(scope, []*proto.Ref{ref}, false)
	if err != nil {
		return nil, nil, err
	}

	if archives[0] != nil && (archives[0] != served || records[0].Offset != rec.Offset) {
		if plain, err := ps.plain(archives[0], records[0]); err != nil || plain {
			return archives[0], records[0], err
		}
	}

	if ws := ps.lookupWriteSession(scope.Session); ws != nil {
		ws.pendingMtx.RLock()
		p, pending := ws.pending[string(ref.Hash)]
		ws.pendingMtx.RUnlock()

		if pending {
			if plain, err := ps.plain(p.archive, p.record); err != nil || plain {
				return p.archive, p.record, err
			}
		}
	}

	return nil, nil, nil
}

// preferPlain returns the copy of a blob a read serves among live copies
// of it: the first the store verified, else the first.
func (ps *PackStorage) preferPlain(archives []*archive, records []*IndexRecord) (*archive, *IndexRecord) {
	if len(archives) > 1 && proto.ObjectType(records[0].Type) == proto.ObjectType_BLOB {
		for i, a := range archives {
			if plain, err := ps.plain(a, records[i]); err == nil && plain {
				return a, records[i]
			}
		}
	}

	return archives[0], records[0]
}
