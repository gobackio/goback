package pack

import (
	"context"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"
)

// commitAfterResurrect, when set, runs between a commit's resurrection
// and its end marker; tests use it to interleave collections.
var commitAfterResurrect func()

// resurrect protects what a committing session relied on without storing
// it. The commit's un-tombstone, stored with the commit, keeps what the
// commit reaches; resurrect takes back the tombstones standing against the
// refs the session's objects reference outside its own archives, and when
// a collection sealed tombstones since the session began, it copies
// whatever those condemn under the skipped refs into the session.
func (ps *PackStorage) resurrect(ctx context.Context, ws *writeSession, skipped []*proto.Ref) error {
	if len(skipped) == 0 {
		return nil
	}

	tombstoned, err := ps.tombstoned(ctx, skipped)
	if err != nil {
		return err
	}

	// the un-tombstones are stored before the seals are read, so a
	// collection either reads them or sealed before; they stay the
	// session's, so no other session relies on them before the copies
	// below are made
	for _, ref := range tombstoned {
		if err := ps.putTombstone(ctx, ws, proto.TombstoneRef(ref), false); err != nil {
			return fmt.Errorf("taking back the tombstones of what session %s relied on: %w", ws.id, err)
		}
	}

	if err := ps.flushSession(ws); err != nil {
		return err
	}

	since, err := ps.sealedAtBegin(ws)
	if err != nil {
		return err
	}

	sealed, err := ps.sealedSince(since)
	if err != nil || len(sealed) == 0 {
		return err
	}

	copied, err := ps.copyCondemned(ctx, skipped, sealed)
	if err != nil {
		return err
	}

	if copied == 0 {
		return nil
	}

	ps.logger.Info("session copied what a collection condemned under it", "session", ws.id, "objects", copied)

	return ps.flushSession(ws)
}

// sealedAtBegin returns the generation the session's begin marker
// records.
func (ps *PackStorage) sealedAtBegin(ws *writeSession) (uint64, error) {
	if ws.began {
		return ws.sealed, nil
	}

	marker, err := ps.readBeginMarker(ws.id)
	if err != nil {
		return 0, err
	}

	return marker.Sealed, nil
}

// skippedRefs walks down from the commit while the objects are the
// session's own and returns the refs it reaches outside them: what the
// session deduplicated. It fails with a backup.MissingError naming the refs
// it reaches that no archive the session sees holds. It reports the refs
// it walked as progress.PhaseCheck.
func (ps *PackStorage) skippedRefs(ctx context.Context, ws *writeSession, commit *proto.Object) ([]*proto.Ref, error) {
	pending, err := ps.index.PendingArchives(ws.id)
	if err != nil {
		return nil, err
	}

	own := make(map[string]bool, len(pending))
	for _, name := range pending {
		own[name] = true
	}

	scope := ScopeOf(ctx)
	root := commit.Ref()
	seen := map[string]bool{string(root.Hash): true}
	frontier := []*proto.Ref{root}

	var skipped, missing []*proto.Ref

	phase := progress.Start(ctx, progress.OpCommit, progress.PhaseCheck, 0, 0)
	defer phase.Finish()

	for len(frontier) > 0 {
		found, err := ps.index.LocateCopies(frontier, scope)
		if err != nil {
			return nil, err
		}

		var below []*proto.Ref

		for _, ref := range frontier {
			copies := found[string(ref.Hash)]
			if len(copies) == 0 && !ref.Equal(root) && !ps.foreignRefs {
				missing = append(missing, ref)
				continue
			}

			loc, ours := ownCopy(copies, own)
			if !ours {
				skipped = append(skipped, ref)
				continue
			}

			if proto.ObjectType(loc.Record.Type) != proto.ObjectType_BLOB {
				below = append(below, ref)
			}
		}

		objects, err := ps.readOwn(ctx, ws, commit, below)
		if err != nil {
			return nil, err
		}

		var next []*proto.Ref

		for _, obj := range objects {
			for _, child := range backup.References(obj) {
				if !seen[string(child.Hash)] {
					seen[string(child.Hash)] = true
					next = append(next, child)
				}
			}
		}

		phase.Add(int64(len(frontier)), 0)

		frontier = next
	}

	if len(missing) > 0 {
		return nil, &backup.MissingError{Refs: missing}
	}

	return skipped, nil
}

// readOwn reads the session's objects at refs, the commit being one it
// has at hand, a range read per run of neighbouring records.
func (ps *PackStorage) readOwn(ctx context.Context, ws *writeSession, commit *proto.Object, refs []*proto.Ref) ([]*proto.Object, error) {
	var out []*proto.Object

	stored := make([]*proto.Ref, 0, len(refs))
	for _, ref := range refs {
		if ref.Equal(commit.Ref()) {
			out = append(out, commit)
		} else {
			stored = append(stored, ref)
		}
	}

	objects, err := ps.ReadRecords(ctx, stored)
	if err != nil {
		return nil, err
	}

	for i, ref := range stored {
		if objects[i] == nil {
			if objects[i], err = ps.Get(ctx, ref); err != nil {
				return nil, fmt.Errorf("reading %x of session %s: %w", ref.Hash, ws.id, err)
			}
		}
	}

	return append(out, objects...), nil
}

// tombstoned returns the refs a tombstone the session sees names.
func (ps *PackStorage) tombstoned(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	tombs := make([]*proto.Ref, len(refs))
	for i, ref := range refs {
		tombs[i] = proto.TombstoneRef(ref)
	}

	found, err := ps.index.LocateTombstones(tombs, ScopeOf(ctx))
	if err != nil {
		return nil, err
	}

	var named []*proto.Ref

	for i, ref := range refs {
		if len(found[string(tombs[i].Hash)]) > 0 {
			named = append(named, ref)
		}
	}

	return named, nil
}

func ownCopy(locs []IndexLocation, own map[string]bool) (IndexLocation, bool) {
	for _, loc := range locs {
		if own[loc.Archive] {
			return loc, true
		}
	}

	return IndexLocation{}, false
}

// copyCondemned walks everything under the refs and copies into the
// session each object a sealed tombstone condemns. An object whose old
// copy is gone loses the session.
func (ps *PackStorage) copyCondemned(ctx context.Context, refs []*proto.Ref, sealed map[int64]bool) (int, error) {
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		seen[string(ref.Hash)] = true
	}

	scope := ScopeOf(ctx)
	copied := 0

	for frontier := refs; len(frontier) > 0; {
		condemned, err := ps.sealedTombstones(frontier, sealed)
		if err != nil {
			return 0, err
		}

		found, err := ps.index.LocateCopies(frontier, scope)
		if err != nil {
			return 0, err
		}

		var next []*proto.Ref

		for _, ref := range frontier {
			locs := found[string(ref.Hash)]
			if len(locs) == 0 {
				return 0, fmt.Errorf("%w: a collection took %x", backup.ErrSessionLost, ref.Hash)
			}

			blob := proto.ObjectType(locs[0].Record.Type) == proto.ObjectType_BLOB
			if blob && !condemned[string(ref.Hash)] {
				continue
			}

			obj, err := ps.Get(ctx, ref)
			if errors.Is(err, backup.ErrNotFound) {
				return 0, fmt.Errorf("%w: a collection took %x", backup.ErrSessionLost, ref.Hash)
			}

			if err != nil {
				return 0, err
			}

			if condemned[string(ref.Hash)] && obj.Type() != proto.ObjectType_COMMIT {
				if err := ps.put(ctx, obj); err != nil {
					return 0, err
				}

				copied++
			}

			for _, child := range backup.References(obj) {
				if !seen[string(child.Hash)] {
					seen[string(child.Hash)] = true
					next = append(next, child)
				}
			}
		}

		frontier = next
	}

	return copied, nil
}

// sealedTombstones reports which of refs a tombstone of a sealed version
// condemns.
func (ps *PackStorage) sealedTombstones(refs []*proto.Ref, sealed map[int64]bool) (map[string]bool, error) {
	tombs := make([]*proto.Ref, len(refs))
	for i, ref := range refs {
		tombs[i] = proto.TombstoneRef(ref)
	}

	found, err := ps.index.LocateTombstones(tombs, Scope{})
	if err != nil {
		return nil, err
	}

	condemned := make(map[string]bool)

	for i, ref := range refs {
		for _, loc := range found[string(tombs[i].Hash)] {
			a, err := ps.archiveByName(loc.Archive)
			if err != nil && !errors.Is(err, errArchiveRetired) {
				return nil, err
			}

			if a != nil && sealed[a.version(loc.Record).Time.UnixNano()] {
				condemned[string(ref.Hash)] = true
			}
		}
	}

	return condemned, nil
}
