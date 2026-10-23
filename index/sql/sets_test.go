package sql

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
)

func TestEnsureSetFindsASetByName(t *testing.T) {
	x := openIndex(t, newMemStore())
	ctx := context.Background()

	id, err := ensureSet(ctx, x.client, "world", 0)
	require.NoError(t, err)

	again, err := ensureSet(ctx, x.client, "world", 0)
	require.NoError(t, err)
	require.Equal(t, id, again)

	other, err := ensureSet(ctx, x.client, "logs", 0)
	require.NoError(t, err)
	require.NotEqual(t, id, other)

	found, err := findSet(ctx, x.client, "world")
	require.NoError(t, err)
	require.Equal(t, id, found)

	_, err = findSet(ctx, x.client, "nowhere")
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestEnsureSetRecreatesUnderCarriedID(t *testing.T) {
	x := openIndex(t, newMemStore())
	ctx := context.Background()

	id, err := ensureSet(ctx, x.client, "world", 4200)
	require.NoError(t, err)
	require.EqualValues(t, 4200, id, "a rebuild recreates the set under the id its commits carry")

	again, err := ensureSet(ctx, x.client, "world", 4200)
	require.NoError(t, err)
	require.EqualValues(t, 4200, again)

	require.NoError(t, x.resetSetSequence(ctx))

	fresh, err := ensureSet(ctx, x.client, "other", 0)
	require.NoError(t, err)
	require.NotEqualValues(t, 4200, fresh)
}

var commitClock atomic.Int64

func commitRef(t *testing.T, x *Index, setID int64, seed string) *proto.Ref {
	t.Helper()

	ref := proto.HashPayload(proto.ObjectType_COMMIT, []byte(seed))
	// distinct receipt times even on a clock that ticks coarsely
	now := time.Now().UTC().Add(time.Duration(commitClock.Add(1)) * time.Millisecond)
	require.NoError(t, x.client.CommitRow.Create().SetRef(ref.Hash).SetSetID(setID).SetTimestamp(now).SetReceivedAt(now).SetTree(ref.Hash).Exec(context.Background()))

	return ref
}

func filterOf(set, seed string) *proto.PresenceFilter {
	f := presence.New(10)
	f.Add(proto.HashPayload(proto.ObjectType_BLOB, []byte(seed)).Hash)
	f.Set = set

	return f.Proto()
}

func TestPresenceFollowsTheHead(t *testing.T) {
	x := openIndex(t, newMemStore())
	ctx := context.Background()

	world, err := ensureSet(ctx, x.client, "world", 0)
	require.NoError(t, err)
	logs, err := ensureSet(ctx, x.client, "logs", 0)
	require.NoError(t, err)
	other, err := ensureSet(ctx, x.client, "other", 0)
	require.NoError(t, err)

	first := commitRef(t, x, world, "world-1")
	second := commitRef(t, x, world, "world-2")
	logsHead := commitRef(t, x, logs, "logs-1")
	otherHead := commitRef(t, x, other, "other-1")

	require.NoError(t, storePresence(ctx, x, world, first, filterOf("world", "w1")))
	require.NoError(t, storePresence(ctx, x, logs, logsHead, filterOf("logs", "l1")))
	require.NoError(t, storePresence(ctx, x, other, otherHead, filterOf("other", "o1")))

	sets := func(filters []*proto.PresenceFilter) []string {
		var names []string
		for _, f := range filters {
			names = append(names, f.BackupSet)
		}
		return names
	}

	got, err := loadPresence(ctx, x.client, backup.PresenceSet, "world")
	require.NoError(t, err)
	require.Equal(t, []string{"world"}, sets(got))
	require.True(t, presenceMust(t, got[0]).Test(proto.HashPayload(proto.ObjectType_BLOB, []byte("w1")).Hash))

	got, err = loadPresence(ctx, x.client, backup.PresenceStore, "")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"world", "logs", "other"}, sets(got), "every set of the store, other agents included")

	got, err = loadPresence(ctx, x.client, backup.PresenceOff, "")
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = x.Presence(ctx, backup.PresenceStore, "")
	require.NoError(t, err)
	require.Len(t, got, 3)

	// the new head replaces the old one, the other sets are untouched
	require.NoError(t, storePresence(ctx, x, world, second, filterOf("world", "w2")))

	got, err = loadPresence(ctx, x.client, backup.PresenceStore, "")
	require.NoError(t, err)
	require.Len(t, got, 3)
	for _, f := range got {
		if f.BackupSet == "world" {
			require.True(t, f.Commit == nil || f.Commit.Equal(second))
			require.True(t, presenceMust(t, f).Test(proto.HashPayload(proto.ObjectType_BLOB, []byte("w2")).Hash))
			require.False(t, presenceMust(t, f).Test(proto.HashPayload(proto.ObjectType_BLOB, []byte("w1")).Hash))
		}
	}

	stale, err := x.client.CommitRow.Query().Where(commitrow.PresenceNotNil()).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, stale, "one filter per head")

	// a build of the older commit that lands late does not replace the head's filter
	require.NoError(t, storePresence(ctx, x, world, first, filterOf("world", "w1")))
	got, err = loadPresence(ctx, x.client, backup.PresenceSet, "world")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, presenceMust(t, got[0]).Test(proto.HashPayload(proto.ObjectType_BLOB, []byte("w2")).Hash))
	require.False(t, presenceMust(t, got[0]).Test(proto.HashPayload(proto.ObjectType_BLOB, []byte("w1")).Hash))
}

func presenceMust(t *testing.T, p *proto.PresenceFilter) *presence.Filter {
	t.Helper()

	f, err := presence.FromProto(p)
	require.NoError(t, err)

	return f
}

func TestLogicalSizeFollowsCommits(t *testing.T) {
	f := newFixture(t)

	blob := proto.NewObject(&proto.Blob{Data: []byte("0123456789")})
	require.NoError(t, f.store.Put(f.ctx, blob))
	chunked := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: 10, Ref: blob.Ref()}, {Offset: 10, Length: 10, Ref: blob.Ref()}}})
	require.NoError(t, f.store.Put(f.ctx, chunked))

	root := f.tree(f.file("a.txt", "one"), &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("b.bin"), Type: proto.NodeType_NODE_FILE, MtimeNs: f.epoch.UnixNano(), Size: 20, Mode: 0644},
		Ref:  chunked.Ref(),
	})
	commit := f.commit("world", root, false)

	row := f.commitRow(commit)
	require.NotNil(t, row.LogicalSize)
	require.EqualValues(t, 23, *row.LogicalSize, "three inline bytes and two ten-byte parts")
	require.NotEmpty(t, row.Presence, "the same walk builds the filter")

	f.measure()
	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Len(t, sets, 1)
	require.EqualValues(t, 23, sets[0].LogicalSize)
}

func TestBeginCommitGates(t *testing.T) {
	f := newFixture(t)
	ctx := f.ctx

	grant, err := f.x.BeginCommit(ctx, "world")
	require.NoError(t, err)
	require.Nil(t, grant.Policy, "a policy never set is version 0 and stays out of the grant")

	_, err = f.x.BeginCommit(ctx, "world")
	require.NoError(t, err)
	sets, err := f.x.ListSets(ctx)
	require.NoError(t, err)
	require.Len(t, sets, 1, "the set is created once")
	setID := uint64(sets[0].ID)

	// the store's policy travels with the grant once the operator set one
	wanted := storekey.DefaultPolicy()
	wanted.Mode = storekey.ModeNone
	_, err = f.x.SetStorePolicy(ctx, wanted, false, f.clock)
	require.NoError(t, err)
	grant, err = f.x.BeginCommit(ctx, "world")
	require.NoError(t, err)
	wanted.Version = 1
	require.Equal(t, &wanted, grant.Policy)

	policy, err := f.x.StorePolicy(ctx)
	require.NoError(t, err)
	require.Equal(t, &wanted, policy)

	// the index assigns the set id, whatever the commit carried
	commit := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", "one")).Ref(), BackupSet: "world", AgentId: "node-1", SetId: setID + 1})
	require.NoError(t, f.x.Put(ctx, commit))
	require.Equal(t, setID, commit.GetCommit().GetSetId())
	f.presence()

	other := auth.WithPrincipal(context.Background(), &auth.Principal{AgentID: "node-2"})
	_, err = f.x.BeginCommit(other, "world")
	require.NoError(t, err, "any agent commits into any set")

	require.NoError(t, f.x.DeleteSet(ctx, "world", false))
	_, err = f.x.BeginCommit(ctx, "world")
	require.ErrorIs(t, err, backup.ErrSetClosed)
}

func TestStorePolicyVersionsAndAcknowledgement(t *testing.T) {
	f := newFixture(t)

	stored, err := f.x.GetStorePolicy(f.ctx)
	require.NoError(t, err)
	require.Zero(t, stored.Policy.Version)
	require.Nil(t, stored.KeyAcknowledgedAt)

	policy := storekey.DefaultPolicy()
	stored, err = f.x.SetStorePolicy(f.ctx, policy, false, f.clock)
	require.NoError(t, err)
	require.EqualValues(t, 1, stored.Policy.Version)
	require.Nil(t, stored.KeyAcknowledgedAt)

	stored, err = f.x.SetStorePolicy(f.ctx, policy, true, f.clock)
	require.NoError(t, err)
	require.EqualValues(t, 2, stored.Policy.Version)
	sameInstant(t, stored.KeyAcknowledgedAt, f.clock)

	f.advance(time.Hour)
	stored, err = f.x.SetStorePolicy(f.ctx, policy, true, f.clock)
	require.NoError(t, err)
	sameInstant(t, stored.KeyAcknowledgedAt, f.clock.Add(-time.Hour))

	stored, err = f.x.GetStorePolicy(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, stored.Policy.Version)
}

func (f *fixture) references(ctx context.Context, ref *proto.Ref) bool {
	ok, err := f.x.References(ctx, ref)
	require.NoError(f.t, err)

	all, err := f.x.ReferencesAll(ctx, []*proto.Ref{{Hash: []byte("unknown")}, ref})
	require.NoError(f.t, err)
	require.Equal(f.t, []bool{false, ok}, all, "a batch answers as one ref at a time")

	return ok
}

func (f *fixture) reachable(ref *proto.Ref, sets ...string) bool {
	ok, err := f.x.Reachable(f.ctx, sets, ref)
	require.NoError(f.t, err)
	return ok
}

func TestSetRefsFollowCommits(t *testing.T) {
	f := newFixture(t)

	// a split directory and a file large enough to be split
	partA := f.tree(f.file("a.txt", "one"))
	partB := f.tree(f.file("b.txt", "two"))
	split := proto.NewObject(&proto.Tree{Splits: []*proto.Ref{partA.Ref(), partB.Ref()}})
	require.NoError(t, f.store.Put(f.ctx, split))

	sub := proto.NewObject(&proto.File{Inline: []byte("sub")})
	require.NoError(t, f.store.Put(f.ctx, sub))
	big := proto.NewObject(&proto.File{Splits: []*proto.Ref{sub.Ref()}})
	require.NoError(t, f.store.Put(f.ctx, big))
	bigNode := &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("big.bin"), Type: proto.NodeType_NODE_FILE, MtimeNs: f.epoch.UnixNano(), Size: backup.SplitFileSize, Mode: 0644},
		Ref:  big.Ref(),
	}

	root := f.tree(bigNode, &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("world"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755},
		Ref:  split.Ref(),
	})
	commit := f.commit("world", root, false)

	for name, ref := range map[string]*proto.Ref{
		"commit":     commit,
		"root":       root.Ref(),
		"split tree": split.Ref(),
		"part a":     partA.Ref(),
		"part b":     partB.Ref(),
		"a.txt":      partA.GetTree().Nodes[0].Ref,
		"big file":   big.Ref(),
		"sub file":   sub.Ref(),
	} {
		require.True(t, f.references(f.ctx, ref), name)
		require.True(t, f.reachable(ref, "world"), name)
		require.False(t, f.reachable(ref, "logs"), "%s is not reachable through another set", name)
	}

	require.False(t, f.references(f.ctx, &proto.Ref{Hash: make([]byte, proto.HashSize)}))

	// an unchanged subtree in the next commit keeps its rows
	f.advance(time.Hour)
	root2 := f.tree(f.file("c.txt", "three"), &proto.TreeNode{
		Stat: &proto.FileInfo{Name: []byte("world"), Type: proto.NodeType_NODE_DIRECTORY, Mode: 0755},
		Ref:  split.Ref(),
	})
	f.commit("world", root2, false)
	require.True(t, f.references(f.ctx, partA.Ref()))
	require.True(t, f.references(f.ctx, root2.Ref()))

	// a rebuilt index reproduces the rows
	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))
	count, err := y.client.SetRef.Query().Count(f.ctx)
	require.NoError(t, err)
	want, err := f.x.client.SetRef.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, want, count)

	// purging the set drops them
	require.NoError(t, f.x.DeleteSet(f.ctx, "world", true))
	_, err = f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	count, err = f.x.client.SetRef.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	require.False(t, f.references(f.ctx, commit))
}

func TestLogicalSizeIsKnownBeforeMaintenanceRuns(t *testing.T) {
	f := newFixture(t)

	// no presence job here: indexing alone must know the size
	first := proto.NewObject(&proto.Commit{
		Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", "one"), f.file("b.txt", "three")).Ref(),
		BackupSet: "world", AgentId: "node-1",
	})
	require.NoError(t, f.x.Put(f.ctx, first))

	row := f.commitRow(first.Ref())
	require.NotNil(t, row.LogicalSize)
	require.EqualValues(t, 8, *row.LogicalSize)
	require.Empty(t, row.Presence, "the filter is still the maintenance job's work")

	// a commit that drops a file and grows another is smaller by the
	// difference, not by what it happened to upload
	f.advance(time.Hour)
	second := proto.NewObject(&proto.Commit{
		Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", "one longer")).Ref(),
		BackupSet: "world", AgentId: "node-1",
	})
	require.NoError(t, f.x.Put(f.ctx, second))

	require.EqualValues(t, 10, *f.commitRow(second.Ref()).LogicalSize)
	require.EqualValues(t, 8, *f.commitRow(first.Ref()).LogicalSize, "the older commit keeps its own size")

	f.measure()
	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 10, sets[0].LogicalSize)
}

func TestLogicalSizeLeavesOutWhatHoldsNoContent(t *testing.T) {
	f := newFixture(t)

	root := f.tree(
		f.file("a.txt", "one"),
		f.symlink("link", "a.txt"),
		f.dir("sub", f.file("b.txt", "two")),
	)

	commit := proto.NewObject(&proto.Commit{
		Timestamp: f.clock.Unix(), Tree: root.Ref(), BackupSet: "world", AgentId: "node-1",
	})
	require.NoError(t, f.x.Put(f.ctx, commit))

	require.EqualValues(t, 6, *f.commitRow(commit.Ref()).LogicalSize,
		"two files of three bytes; the symlink and the directory hold none")
	require.EqualValues(t, 2, *f.commitRow(commit.Ref()).FileCount, "nor do they count as files")
}

func TestUniqueSizeCountsEachFileVersionOnce(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "first"), f.file("b.txt", "same")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "second!"), f.file("b.txt", "same"), f.file("copy.txt", "same"), f.symlink("link", "a.txt")), false)

	f.measure()
	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 5+7+4, sets[0].UniqueSize, "both versions of a.txt, and the content b.txt and copy.txt share once")
	require.EqualValues(t, 7+4+4, sets[0].LogicalSize)

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	_, err = f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.NotNil(t, f.commitRow(a).TombstonedAt)

	require.Equal(t, []string{"world"}, f.measure(), "retiring a commit changes what the set's sizes count")
	sets, err = f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 7+4, sets[0].UniqueSize, "a version only a tombstoned commit held is gone")
}

func TestUniqueSizeLeavesOutVersionsOnlyARetiringCommitHolds(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "first")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "second!")), false)

	row := f.commitRow(a)
	require.NotNil(t, row.RetireAt)
	require.Nil(t, row.TombstonedAt)

	f.measure()
	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 7, sets[0].UniqueSize, "the version only the retiring commit holds is left out")
}

func TestListingReadsTheSizesMeasureSetsStored(t *testing.T) {
	f := newFixture(t)

	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Empty(t, sets)

	f.commit("world", f.tree(f.file("a.txt", "first")), false)

	sets, err = f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Zero(t, sets[0].LogicalSize, "a set never measured reports nothing")

	require.Equal(t, []string{"world"}, f.measure())
	require.Empty(t, f.measure(), "a set whose commits did not change is not measured again")

	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "second!")), false)

	set, err := f.x.GetSet(f.ctx, "world")
	require.NoError(t, err)
	require.EqualValues(t, 5, set.LogicalSize, "until the next measurement the set reports the last one")
	require.EqualValues(t, 5, set.UniqueSize)

	require.Equal(t, []string{"world"}, f.measure())
	set, err = f.x.GetSet(f.ctx, "world")
	require.NoError(t, err)
	require.EqualValues(t, 7, set.LogicalSize)
	require.EqualValues(t, 5+7, set.UniqueSize)
}

func TestFillingSizesRepairsCommitsIndexedWithoutOne(t *testing.T) {
	f := newFixture(t)

	first := f.commit("world", f.tree(f.file("a.txt", "one"), f.file("b.txt", "three")), false)
	f.advance(time.Hour)
	second := f.commit("world", f.tree(f.file("a.txt", "one longer")), false)

	// an index older than commit sizes left both rows without one
	_, err := f.x.client.CommitRow.Update().ClearLogicalSize().ClearFileCount().Save(f.ctx)
	require.NoError(t, err)

	n, err := f.x.FillMissingSizes(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	require.EqualValues(t, 8, *f.commitRow(first).LogicalSize)
	require.EqualValues(t, 10, *f.commitRow(second).LogicalSize, "each commit gets the size of the set as it stood then")
	require.EqualValues(t, 2, *f.commitRow(first).FileCount)
	require.EqualValues(t, 1, *f.commitRow(second).FileCount)

	n, err = f.x.FillMissingSizes(f.ctx)
	require.NoError(t, err)
	require.Zero(t, n, "a run with nothing missing changes nothing")
}

func TestFillingSizesLeavesATombstonedCommitAlone(t *testing.T) {
	f := newFixture(t)

	first := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	second := f.commit("world", f.tree(f.file("a.txt", "two longer")), false)

	retired, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, retired)
	require.NotNil(t, f.commitRow(first).TombstonedAt)

	_, err = f.x.client.CommitRow.Update().ClearLogicalSize().Save(f.ctx)
	require.NoError(t, err)

	n, err := f.x.FillMissingSizes(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.EqualValues(t, 10, *f.commitRow(second).LogicalSize)
	require.Nil(t, f.commitRow(first).LogicalSize, "the versions it held are gone, so there is nothing to sum")
}

func TestRootOwnerNamesTheSetBehindACommitOrPin(t *testing.T) {
	f := newFixture(t)

	commit := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	pin, err := f.pin(commit)
	require.NoError(t, err)

	owner, err := f.x.RootOwner(f.ctx)
	require.NoError(t, err)

	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Len(t, sets, 1)

	require.Equal(t, pack.Attribution{Set: sets[0].ID}, owner(commit.Hash))
	require.Equal(t, pack.Attribution{Set: sets[0].ID}, owner(pin.Hash), "a pin belongs to the set of the commit it holds")
	require.Zero(t, owner([]byte("something else")), "a root of nobody's is attributed to nobody")

	// a grouping puts each set in the group that carries what it holds
	f.x.Grouping = func(context.Context) (map[int64]int64, error) {
		return map[int64]int64{sets[0].ID: 42}, nil
	}

	owner, err = f.x.RootOwner(f.ctx)
	require.NoError(t, err)
	require.Equal(t, pack.Attribution{Group: 42, Set: sets[0].ID}, owner(commit.Hash))
}

func TestRecordedSetSizesReplaceTheLastRuns(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one")), false)

	sets, err := f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Zero(t, sets[0].PhysicalSize, "nothing has collected yet")

	id := sets[0].ID
	require.NoError(t, f.x.RecordSetSizes(f.ctx, &pack.CollectReport{
		SetBytes: map[int64]uint64{id: 4096}, SetDeduplicated: map[int64]uint64{id: 10000},
		SetAlone: map[int64]uint64{id: 5000}, SetExclusive: map[int64]uint64{id: 3000},
		SetDeduplicatedAlone: map[int64]uint64{id: 12000},
	}))

	sets, err = f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.EqualValues(t, 4096, sets[0].PhysicalSize)
	require.EqualValues(t, 10000, sets[0].DeduplicatedSize)
	require.EqualValues(t, 5000, sets[0].AloneSize)
	require.EqualValues(t, 3000, sets[0].ExclusiveSize)
	require.EqualValues(t, 12000, sets[0].DeduplicatedAloneSize)

	// a run that reaches nothing of the set's own leaves it holding nothing
	require.NoError(t, f.x.RecordSetSizes(f.ctx, &pack.CollectReport{}))

	sets, err = f.x.ListSets(f.ctx)
	require.NoError(t, err)
	require.Zero(t, sets[0].PhysicalSize)
	require.Zero(t, sets[0].DeduplicatedSize)
	require.Zero(t, sets[0].AloneSize)
	require.Zero(t, sets[0].DeduplicatedAloneSize)
}

func TestRecordedSetSizesReachEverySetAcrossBatchesAndCountUnknownOnes(t *testing.T) {
	f := newFixture(t)

	const sets = objectBatch + 5

	builders := make([]*ent.SetCreate, sets)
	for i := range builders {
		builders[i] = f.x.client.Set.Create().SetID(int64(i + 1)).SetName(fmt.Sprintf("set-%d", i))
	}
	require.NoError(t, f.x.client.Set.CreateBulk(builders...).Exec(f.ctx))

	report := &pack.CollectReport{SetBytes: map[int64]uint64{}, SetDeduplicated: map[int64]uint64{}, SetAlone: map[int64]uint64{},
		SetExclusive: map[int64]uint64{}, SetDeduplicatedAlone: map[int64]uint64{}, Unattributed: 7}
	for id := int64(2); id <= sets+1; id++ {
		report.SetBytes[id] = uint64(id)
		report.SetDeduplicated[id] = uint64(2 * id)
		report.SetAlone[id] = uint64(3 * id)
		report.SetExclusive[id] = uint64(4 * id)
		report.SetDeduplicatedAlone[id] = uint64(5 * id)
	}

	require.NoError(t, f.x.client.Set.UpdateOneID(1).SetPhysicalSize(99).Exec(f.ctx))
	require.NoError(t, f.x.RecordSetSizes(f.ctx, report))
	require.EqualValues(t, 7+sets+1, report.Unattributed, "the one set the index lacks")

	rows, err := f.x.client.Set.Query().All(f.ctx)
	require.NoError(t, err)
	require.Len(t, rows, sets)

	for _, row := range rows {
		if row.ID == 1 {
			require.Nil(t, row.PhysicalSize, "a set the run did not name holds nothing")
			continue
		}

		require.Equal(t, []int64{row.ID, 2 * row.ID, 3 * row.ID, 4 * row.ID, 5 * row.ID},
			[]int64{*row.PhysicalSize, *row.DeduplicatedSize, *row.AloneSize, *row.ExclusiveSize, *row.DeduplicatedAloneSize}, "set %d", row.ID)
	}
}

func TestPendingPresenceIsBuiltForEachSetWhoseNewestLiveCommitLacksItOnceItsFilterIsOld(t *testing.T) {
	f := newFixture(t)

	f.commit("a", f.tree(f.file("a.txt", "one")), false)
	f.commit("b", f.tree(f.file("b.txt", "one")), false)
	f.advance(time.Hour)

	obj := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", "two")).Ref(), BackupSet: "a", AgentId: "node-1"})
	require.NoError(t, f.x.Put(f.ctx, obj))
	require.Empty(t, f.commitRow(obj.Ref()).Presence)

	built, err := f.x.BuildPendingPresence(f.ctx)
	require.NoError(t, err)
	require.Zero(t, built, "a's filter is of a commit received an hour ago")

	f.advance(PresenceEvery)

	built, err = f.x.BuildPendingPresence(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, built, "only a's newest lacks a filter")
	require.NotEmpty(t, f.commitRow(obj.Ref()).Presence)

	built, err = f.x.BuildPendingPresence(f.ctx)
	require.NoError(t, err)
	require.Zero(t, built)
}

func TestASetWhoseRecentFilterWasRetiredGetsANewOne(t *testing.T) {
	f := newFixture(t)

	old := f.commit("a", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	head := f.commit("a", f.tree(f.file("a.txt", "two")), false)
	require.NotEmpty(t, f.commitRow(old).Presence)
	require.Empty(t, f.commitRow(head).Presence, "the old filter is recent")

	require.NoError(t, f.x.SetPolicy(f.ctx, "a", &retention.Policy{KeepLast: 1}))
	_, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)

	built, err := f.x.BuildPendingPresence(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, built)
	require.NotEmpty(t, f.commitRow(head).Presence)
}
