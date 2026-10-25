package pack

import (
	"context"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

const (
	// revivalMissing bounds how many missing objects a revival names per
	// commit.
	revivalMissing = 20
	// reviveBatch is how many refs one lookup of a revival takes.
	reviveBatch = 1000
)

var _ backup.Reviver = (*PackStorage)(nil)

// Revive implements backup.Reviver. A revival is an un-tombstone of the
// commit newer than its tombstone, which a collection takes as a root;
// what the commit reaches gets un-tombstones as a session's skipped refs
// do. It holds off this process's collections while it runs and refuses
// while a published plan waits for its rewrite.
func (ps *PackStorage) Revive(ctx context.Context, commits []*proto.Ref, dryRun bool) ([]backup.Revival, error) {
	if _, ok := backup.SessionFromContext(ctx); ok {
		return nil, errors.New("a revival runs outside a session")
	}

	ps.compactorMtx.Lock()
	defer ps.compactorMtx.Unlock()

	if err := ps.noPendingPlan(); err != nil {
		return nil, err
	}

	roots := make([]refKey, len(commits))
	for i, c := range commits {
		roots[i] = keyOf(c.GetHash())
	}

	g, err := ps.reach(ctx, roots)
	if err != nil {
		return nil, err
	}

	revivals := make([]backup.Revival, len(commits))
	var whole []*proto.Ref

	for i, c := range commits {
		missing, count := g.missingUnder(roots[i])
		revivals[i] = backup.Revival{Commit: c, Missing: missing, MissingCount: count}

		if count == 0 {
			whole = append(whole, c)
		}
	}

	if dryRun || len(whole) == 0 {
		return revivals, nil
	}

	return revivals, ps.revive(ctx, whole, g)
}

// revive stores the un-tombstones of the commits and of everything under
// them a tombstone stands against, and repeats those of the commits until
// each is newer than the commit's tombstone.
func (ps *PackStorage) revive(ctx context.Context, commits []*proto.Ref, g *reachGraph) error {
	keys := make([]refKey, len(commits))
	for i, c := range commits {
		keys[i] = keyOf(c.Hash)
	}

	reached := g.closure(keys)
	var tombstoned []*proto.Ref

	for start := 0; start < len(reached); start += reviveBatch {
		chunk := reached[start:min(start+reviveBatch, len(reached))]

		refs := make([]*proto.Ref, len(chunk))
		for i, key := range chunk {
			refs[i] = &proto.Ref{Hash: append([]byte(nil), key[:]...)}
		}

		named, err := ps.tombstoned(ctx, refs)
		if err != nil {
			return err
		}

		tombstoned = append(tombstoned, named...)
	}

	ws, err := ps.writeSessionFor(ctx)
	if err != nil {
		return err
	}

	targets := tombstoned
	for attempt := 1; len(targets) > 0; attempt++ {
		if attempt > condemnAttempts {
			return fmt.Errorf("%d revivals came out no newer than the tombstones they take back", len(targets))
		}

		for _, ref := range targets {
			if err := ps.putTombstone(ctx, ws, proto.TombstoneRef(ref), false); err != nil {
				return fmt.Errorf("reviving %x: %w", ref.Hash, err)
			}
		}

		if err := ps.flushSession(ws); err != nil {
			return err
		}

		targets, err = ps.unrevived(commits)
		if err != nil {
			return err
		}
	}

	return nil
}

// Revived implements backup.Reviver.
func (ps *PackStorage) Revived(_ context.Context, commit *proto.Ref) (bool, error) {
	standing, err := ps.unrevived([]*proto.Ref{commit})

	return len(standing) == 0, err
}

// Holds implements backup.Reviver.
func (ps *PackStorage) Holds(_ context.Context, commits []*proto.Ref) ([]bool, error) {
	found, err := ps.index.LocateCopies(commits, Scope{})
	if err != nil {
		return nil, err
	}

	held := make([]bool, len(commits))

	for i, c := range commits {
		rec, err := ps.usableCopy(found[string(c.GetHash())])
		if err != nil {
			return nil, err
		}

		held[i] = rec != nil
	}

	return held, nil
}

// unrevived returns the commits a tombstone still stands against.
func (ps *PackStorage) unrevived(commits []*proto.Ref) ([]*proto.Ref, error) {
	var refs []*proto.Ref
	for _, c := range commits {
		tomb := proto.TombstoneRef(c)
		refs = append(refs, tomb, proto.TombstoneRef(tomb))
	}

	found, err := ps.index.LocateCopies(refs, Scope{})
	if err != nil {
		return nil, err
	}

	var standing []*proto.Ref

	for i, c := range commits {
		bound, err := ps.tombstoneBound(found[string(refs[2*i].Hash)], found[string(refs[2*i+1].Hash)])
		if err != nil {
			return nil, err
		}

		if bound != nil {
			standing = append(standing, c)
		}
	}

	return standing, nil
}

// reachGraph is what a revival found under its commits: the children of
// every object it read, and the objects without a copy.
type reachGraph struct {
	children map[refKey][]refKey
	missing  map[refKey]bool
	broken   map[refKey]bool
}

// reach walks down from the roots through the committed copies the store
// holds, tombstoned or not. A root that is no commit is an error.
func (ps *PackStorage) reach(ctx context.Context, roots []refKey) (*reachGraph, error) {
	g := &reachGraph{children: make(map[refKey][]refKey), missing: make(map[refKey]bool), broken: make(map[refKey]bool)}

	isRoot := make(map[refKey]bool, len(roots))
	seen := make(map[refKey]bool)
	var frontier []refKey

	for _, key := range roots {
		isRoot[key] = true
		if !seen[key] {
			seen[key] = true
			frontier = append(frontier, key)
		}
	}

	for len(frontier) > 0 {
		var next []refKey

		for start := 0; start < len(frontier); start += reviveBatch {
			chunk := frontier[start:min(start+reviveBatch, len(frontier))]

			objects, err := ps.readReached(ctx, g, chunk, isRoot)
			if err != nil {
				return nil, err
			}

			for key, obj := range objects {
				var blobs []liveRef
				kids := appendChildren(nil, &blobs, obj)
				for _, blob := range blobs {
					kids = append(kids, blob.key)
				}

				g.children[key] = kids

				for _, kid := range kids {
					if !seen[kid] {
						seen[kid] = true
						next = append(next, kid)
					}
				}
			}
		}

		frontier = next
	}

	return g, nil
}

// readReached locates the keys, records those without a copy as missing
// and reads every one that is not a blob.
func (ps *PackStorage) readReached(ctx context.Context, g *reachGraph, keys []refKey, roots map[refKey]bool) (map[refKey]*proto.Object, error) {
	refs := make([]*proto.Ref, len(keys))
	for i, key := range keys {
		refs[i] = &proto.Ref{Hash: append([]byte(nil), key[:]...)}
	}

	found, err := ps.index.LocateCopies(refs, Scope{})
	if err != nil {
		return nil, err
	}

	var read []*proto.Ref

	for i, key := range keys {
		rec, err := ps.usableCopy(found[string(key[:])])
		if err != nil {
			return nil, err
		}

		switch {
		case rec == nil:
			g.missing[key] = true
		case roots[key] && proto.ObjectType(rec.Type) != proto.ObjectType_COMMIT:
			return nil, fmt.Errorf("%x is a %s, not a commit", key, proto.ObjectType(rec.Type))
		case proto.ObjectType(rec.Type) != proto.ObjectType_BLOB:
			read = append(read, refs[i])
		}
	}

	objects := make([]*proto.Object, len(read))
	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(defaultGCReaders)

	for i, ref := range read {
		grp.Go(func() error {
			obj, err := ps.Get(gctx, ref)
			if errors.Is(err, backup.ErrNotFound) {
				return nil
			}

			if err != nil {
				return fmt.Errorf("reading %x: %w", ref.Hash, err)
			}

			objects[i] = obj

			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	byKey := make(map[refKey]*proto.Object, len(read))
	for i, ref := range read {
		if objects[i] == nil {
			g.missing[keyOf(ref.Hash)] = true
			continue
		}

		byKey[keyOf(ref.Hash)] = objects[i]
	}

	return byKey, nil
}

// usableCopy returns the record of a copy among locs whose archive is not
// retired, or nil.
func (ps *PackStorage) usableCopy(locs []IndexLocation) (*IndexRecord, error) {
	for _, loc := range locs {
		a, err := ps.archiveByName(loc.Archive)
		if err != nil && !errors.Is(err, errArchiveRetired) {
			return nil, err
		}

		if a != nil {
			return &loc.Record, nil
		}
	}

	return nil, nil
}

// isBroken reports whether anything under key is missing.
func (g *reachGraph) isBroken(key refKey) bool {
	if b, ok := g.broken[key]; ok {
		return b
	}

	b := g.missing[key]
	for _, kid := range g.children[key] {
		if g.isBroken(kid) {
			b = true
		}
	}

	g.broken[key] = b

	return b
}

// missingUnder names up to revivalMissing of the missing objects under
// root and counts them all.
func (g *reachGraph) missingUnder(root refKey) ([]*proto.Ref, int) {
	var named []*proto.Ref
	count := 0
	seen := make(map[refKey]bool)

	var walk func(key refKey)
	walk = func(key refKey) {
		if seen[key] || !g.isBroken(key) {
			return
		}

		seen[key] = true

		if g.missing[key] {
			count++
			if len(named) < revivalMissing {
				named = append(named, &proto.Ref{Hash: append([]byte(nil), key[:]...)})
			}
		}

		for _, kid := range g.children[key] {
			walk(kid)
		}
	}

	walk(root)

	return named, count
}

// closure returns everything under the roots, the roots included.
func (g *reachGraph) closure(roots []refKey) []refKey {
	seen := make(map[refKey]bool)
	var out []refKey

	stack := append([]refKey(nil), roots...)
	for len(stack) > 0 {
		key := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if seen[key] {
			continue
		}

		seen[key] = true
		out = append(out, key)
		stack = append(stack, g.children[key]...)
	}

	return out
}
