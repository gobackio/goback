package sql

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
	pb "google.golang.org/protobuf/proto"
)

// pending is a commit read from the store, waiting to be indexed.
type pending struct {
	commit *proto.Commit
	ref    *proto.Ref
}

// groupBySet splits commits by the set they carry, by id or else by name,
// each group in receipt order. Groups that name a set come first, in the
// order they first appear, so a placeholder set never takes a name a set
// of the store still has to claim.
func groupBySet(commits []pending) [][]pending {
	type setKey struct {
		id   int64
		name string
	}

	bySet := map[setKey][]pending{}
	var keys []setKey

	for _, c := range commits {
		key := setKey{id: int64(c.commit.GetSetId())}
		if key.id == 0 {
			key.name = c.commit.GetBackupSet()
		}

		if _, ok := bySet[key]; !ok {
			keys = append(keys, key)
		}

		bySet[key] = append(bySet[key], c)
	}

	groups := make([][]pending, len(keys))
	for i, key := range keys {
		group := bySet[key]
		sort.Slice(group, func(i, j int) bool {
			if a, b := group[i].commit.GetReceivedAtNs(), group[j].commit.GetReceivedAtNs(); a != b {
				return a < b
			}

			return bytes.Compare(group[i].ref.Hash, group[j].ref.Hash) < 0
		})

		groups[i] = group
	}

	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i][0].commit.GetBackupSet() != "" && groups[j][0].commit.GetBackupSet() == ""
	})

	return groups
}

// describeGroup is what a group of commits holds, before any set is
// resolved for it.
func describeGroup(group []pending) index.IndexedSet {
	first, last := group[0].commit, group[len(group)-1].commit

	described := index.IndexedSet{
		Placeholder: first.GetBackupSet() == "",
		Commits:     len(group),
		Chained:     true,
		Oldest:      time.Unix(0, first.GetReceivedAtNs()).UTC(),
		Newest:      time.Unix(0, last.GetReceivedAtNs()).UTC(),
	}

	for i := 1; i < len(group); i++ {
		if !bytes.Equal(group[i].commit.GetParent().GetHash(), group[i-1].ref.Hash) {
			described.Chained = false
		}
	}

	return described
}

// groupSet resolves the set a group of commits goes to: the one they
// carry, or for commits that name no set a placeholder set. Unless
// dryRun, a set that does not exist is created.
func (x *Index) groupSet(ctx context.Context, group []pending, dryRun bool) (index.IndexedSet, error) {
	target := describeGroup(group)
	first := group[0]
	wantID := int64(first.commit.GetSetId())

	if target.Placeholder {
		s, created, err := x.placeholderSet(ctx, wantID, dryRun)
		target.SetID, target.Set, target.Created = s.ID, s.Name, created

		return target, err
	}

	target.Set = first.commit.GetBackupSet()

	existing, err := x.groupSetRow(ctx, wantID, target.Set)
	if err != nil {
		return target, err
	}

	target.Created = existing == nil
	if existing != nil {
		target.SetID, target.Set = existing.ID, existing.Name
	} else {
		target.SetID = wantID
	}

	if dryRun || existing != nil {
		return target, nil
	}

	target.SetID, err = x.ensureSet(ctx, x.client, first.commit, first.ref, false)

	return target, err
}

// groupSetRow is the set ensureSet would find for the id and name, nil
// for none.
func (x *Index) groupSetRow(ctx context.Context, wantID int64, name string) (*ent.Set, error) {
	if wantID != 0 {
		s, err := x.client.Set.Get(ctx, wantID)
		if !ent.IsNotFound(err) {
			return s, err
		}
	}

	s, err := x.client.Set.Query().Where(set.Name(name)).Only(ctx)
	if ent.IsNotFound(err) {
		return nil, nil
	}

	return s, err
}

// placeholderLimit bounds the placeholder names tried.
const placeholderLimit = 1000

// placeholderSet is the set under wantID if there is one, or else a new
// one, under wantID unless that is 0, named the first of
// index.PlaceholderSet, index.PlaceholderSet-2, … no set has. A new set
// comes up with retention paused; dryRun creates none.
func (x *Index) placeholderSet(ctx context.Context, wantID int64, dryRun bool) (*ent.Set, bool, error) {
	if wantID != 0 {
		s, err := x.client.Set.Get(ctx, wantID)
		if !ent.IsNotFound(err) {
			return s, false, err
		}
	}

	for n := 1; n <= placeholderLimit; n++ {
		name := index.PlaceholderSet
		if n > 1 {
			name += "-" + strconv.Itoa(n)
		}

		taken, err := x.client.Set.Query().Where(set.Name(name)).Exist(ctx)
		if err != nil {
			return nil, false, err
		}

		if taken {
			continue
		}

		if dryRun {
			return &ent.Set{ID: wantID, Name: name}, true, nil
		}

		create := x.client.Set.Create().SetName(name).SetRetentionPaused(true)
		if wantID != 0 {
			create.SetID(wantID)
		}

		s, err := create.Save(ctx)
		if err == nil {
			return s, true, nil
		}

		if !ent.IsConstraintError(err) {
			return nil, false, err
		}

		// the name is taken where this connection cannot see, or another
		// run created the set under wantID first
		if wantID != 0 {
			s, err := x.client.Set.Get(ctx, wantID)
			if !ent.IsNotFound(err) {
				return s, false, err
			}
		}
	}

	return nil, false, fmt.Errorf("no free placeholder set name among %d", placeholderLimit)
}

// indexGroup indexes a group of commits into the set groupSet resolved
// for it, in receipt order, and counts where each went.
func (x *Index) indexGroup(ctx context.Context, target index.IndexedSet, group []pending) ([behind + 1]int, error) {
	var counts [behind + 1]int

	if target.Placeholder {
		x.logger().Error("indexing commits that name no set under a placeholder set", "set", target.Set, "commits", len(group))
	}

	for _, c := range group {
		commit := c.commit
		if target.Placeholder {
			commit = pb.Clone(commit).(*proto.Commit)
			commit.BackupSet, commit.SetId = target.Set, uint64(target.SetID)
		}

		// the caller counts the commits; their walks are no commit operation
		placed, err := x.indexCommit(progress.NewContext(ctx, nil), commit, c.ref, false, false)
		if err != nil {
			return counts, err
		}

		counts[placed]++
	}

	return counts, x.reevaluateSet(ctx, target.SetID)
}

// IndexCommits indexes the commits refs name, already in the store,
// without a rebuild: by set in receipt order, as ReIndex does, those that
// name no set under a placeholder set. It refuses the lot if a ref is not
// a commit, is tombstoned or already has a row (index.ErrIndexed). With
// dryRun it writes nothing and reports what it would do.
func (x *Index) IndexCommits(ctx context.Context, refs []*proto.Ref, dryRun bool) ([]index.IndexedSet, error) {
	var unique []*proto.Ref
	seen := map[string]bool{}

	for _, ref := range refs {
		if !seen[string(ref.Hash)] {
			seen[string(ref.Hash)] = true
			unique = append(unique, ref)
		}
	}

	commits := make([]pending, len(unique))
	errs := make([]error, len(unique))

	grp := new(errgroup.Group)
	grp.SetLimit(diffWorkers)

	for i, ref := range unique {
		grp.Go(func() error {
			c, err := x.unindexedCommit(ctx, ref)
			commits[i], errs[i] = pending{commit: c, ref: ref}, err

			return nil
		})
	}

	_ = grp.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	groups := groupBySet(commits)
	sets := make([]index.IndexedSet, 0, len(groups))

	for _, group := range groups {
		target, err := x.groupSet(ctx, group, dryRun)
		if err != nil {
			return sets, err
		}

		if !dryRun {
			counts, err := x.indexGroup(ctx, target, group)
			target.Indexed, target.Tied, target.Behind = counts[inOrder]+counts[tied], counts[tied], counts[behind]

			if err != nil {
				return append(sets, target), err
			}
		}

		sets = append(sets, target)
	}

	if dryRun {
		return sets, nil
	}

	return sets, x.resetSetSequence(ctx)
}

// unindexedCommit reads the commit ref names, which must have no row and
// no tombstone.
func (x *Index) unindexedCommit(ctx context.Context, ref *proto.Ref) (*proto.Commit, error) {
	exists, err := x.client.CommitRow.Query().Where(commitrow.Ref(ref.Hash)).Exist(ctx)
	if err != nil {
		return nil, err
	}

	if exists {
		return nil, fmt.Errorf("%w: commit %x", index.ErrIndexed, ref.Hash)
	}

	deleted, err := isDeleted(ctx, x.client, ref.Hash)
	if err != nil {
		return nil, err
	}

	if deleted {
		return nil, fmt.Errorf("%w: commit %x", backup.ErrTombstoned, ref.Hash)
	}

	obj, err := x.ObjectStore.Get(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("commit %x: %w", ref.Hash, err)
	}

	commit := obj.GetCommit()
	if commit == nil {
		return nil, fmt.Errorf("%x is a %s object, not a commit", ref.Hash, obj.Type())
	}

	return commit, nil
}
