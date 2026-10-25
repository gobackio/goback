package sql

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent/commitrow"
	"github.com/gobackio/goback/index/sql/ent/pin"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/index/sql/ent/setref"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestIndexKeepsSealedOwnerNames(t *testing.T) {
	f := newFixture(t)

	user, group := []byte{0xd7, 0xd2, 0x00, 0x01}, []byte("=wheel")
	file := f.file("a.txt", "one")
	file.Stat.User, file.Stat.Group = user, group
	f.commit("world", f.tree(file), false)

	versions, err := f.x.FileInfo(f.ctx, "world", "a.txt", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, user, versions[0].Stat.User)
	require.Equal(t, group, versions[0].Stat.Group)
}

func TestIndexerRangesFollowVersions(t *testing.T) {
	f := newFixture(t)

	t0 := f.clock
	rootA := f.tree(f.file("a.txt", "one"), f.dir("world", f.file("level.dat", "w1")))
	f.commit("world", rootA, false)

	f.advance(time.Hour)
	t1 := f.clock
	rootB := f.tree(f.file("a.txt", "two"), f.dir("world", f.file("level.dat", "w1")))
	f.commit("world", rootB, false)

	f.advance(time.Hour)
	t2 := f.clock
	rootC := f.tree(f.dir("world", f.file("level.dat", "w2")), f.dir("logs", f.file("x.log", "l")))
	c := f.commit("world", rootC, false)

	files := f.ranges(f.x, "files")
	require.Len(t, files, 5)

	require.Equal(t, "a.txt", files[0].path)
	require.True(t, files[0].validFrom.Equal(t0))
	closedAt(t, files[0], t1)
	require.Equal(t, "a.txt", files[1].path)
	require.True(t, files[1].validFrom.Equal(t1))
	closedAt(t, files[1], t2)

	require.Equal(t, "logs/x.log", files[2].path)
	require.Nil(t, files[2].validUntil)

	require.Equal(t, "world/level.dat", files[3].path)
	require.True(t, files[3].validFrom.Equal(t0))
	closedAt(t, files[3], t2)
	require.Equal(t, "world/level.dat", files[4].path)
	require.True(t, files[4].validFrom.Equal(t2))
	require.Nil(t, files[4].validUntil)

	trees := f.ranges(f.x, "trees")
	require.Len(t, trees, 3, "world is closed and reopened once, logs opened once: %+v", trees)

	versions, err := f.x.FileInfo(f.ctx, "world", "world/level.dat", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, "level.dat", string(versions[0].Stat.Name))
	require.EqualValues(t, 2, versions[0].Stat.Size)
	require.Equal(t, f.epoch.UnixNano(), versions[0].Stat.MtimeNs)

	versions, err = f.x.FileInfo(f.ctx, "world", "a.txt", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 2, "both versions are held by live commits")

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, commits, 3)
	require.Equal(t, t2.UnixNano(), commits[0].ReceivedAtNs)
	require.Equal(t, "world", commits[0].BackupSet)
	require.NotZero(t, commits[0].SetId)
	require.True(t, commits[0].Tree.Equal(rootC.Ref()))

	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(c))

	// the same tree at a later commit changes nothing
	f.advance(time.Hour)
	f.commit("world", rootC, false)
	require.Len(t, f.ranges(f.x, "files"), 5)
}

func TestIndexerKeepsOrderAndRefusesClosedSets(t *testing.T) {
	f := newFixture(t)

	root := f.tree(f.file("a.txt", "one"))
	first := f.commit("world", root, false)

	// the index clock stepping back cannot reorder a set's commits: the
	// next receipt is stamped after the newest
	f.advance(-time.Hour)
	stale := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: root.Ref(), BackupSet: "world", AgentId: "node-1"})
	require.NoError(t, f.x.Put(f.ctx, stale))
	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.False(t, latest.Equal(first), "the commit received last is the newest")
	f.advance(time.Hour)

	require.NoError(t, f.x.DeleteSet(f.ctx, "world", false))

	f.advance(time.Hour)
	fresh := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: root.Ref(), BackupSet: "world", AgentId: "node-1"})
	require.ErrorIs(t, f.x.Put(f.ctx, fresh), backup.ErrSetClosed)

	require.NoError(t, f.x.UndeleteSet(f.ctx, "world"))
	require.NoError(t, f.x.Put(f.ctx, fresh))
}

func TestAStrayNewestCommitIsDeletedAndTheOneBeforeItIsLatestAgain(t *testing.T) {
	f := newFixture(t)

	only := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.ErrorIs(t, f.x.DeleteCommit(f.ctx, only), backup.ErrNewestCommit, "a set keeps one commit")

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	stray := f.commit("world", f.tree(f.file("stray.txt", "elsewhere")), false)
	require.NotNil(t, f.commitRow(only).RetireAt, "keep_last 1 retires the commit before the stray one")

	require.NoError(t, f.x.DeleteCommit(f.ctx, stray))

	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(only))
	require.Nil(t, f.commitRow(only).RetireAt, "the latest is kept again")
	require.Equal(t, "latest,last", f.commitRow(only).RetainedBy)

	versions, err := f.x.FileInfo(f.ctx, "world", "stray.txt", f.clock, 10)
	require.NoError(t, err)
	require.Empty(t, versions, "what only the stray commit held is not offered")

	require.NoError(t, f.x.UndeleteCommit(f.ctx, stray))
	latest, err = f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(stray))
}

func TestAnIncompleteNewestCommitDoesNotRetireTheCompleteOneBeforeIt(t *testing.T) {
	f := newFixture(t)

	complete := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))

	f.advance(time.Hour)
	incomplete := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", "two")).Ref(), BackupSet: "world", AgentId: "node-1", Incomplete: true})
	require.NoError(t, f.x.Put(f.ctx, incomplete))

	require.Equal(t, "latest", f.commitRow(incomplete.Ref()).RetainedBy)
	require.Equal(t, "last", f.commitRow(complete).RetainedBy)
	require.Nil(t, f.commitRow(complete).RetireAt)
}

func TestRetentionLifecycle(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 2}))
	f.advance(time.Hour)
	b := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	c := f.commit("world", f.tree(f.file("a.txt", "three")), false)

	row := f.commitRow(a)
	require.Empty(t, row.RetainedBy)
	require.NotNil(t, row.RetireAt, "a is beyond keep_last 2")
	sameInstant(t, row.ExpiresAt, f.clock)

	row = f.commitRow(c)
	require.Equal(t, "latest,last", row.RetainedBy)
	require.Nil(t, row.RetireAt)

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, commits, 2, "the retired commit is hidden")

	versions, err := f.x.FileInfo(f.ctx, "world", "a.txt", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 2, "the version only a retired commit holds is not offered")

	require.NoError(t, f.x.DeleteCommit(f.ctx, b))

	row = f.commitRow(b)
	require.NotNil(t, row.DeletedAt)
	sameInstant(t, row.ExpiresAt, f.clock.Add(14*24*time.Hour))

	require.NoError(t, f.x.UndeleteCommit(f.ctx, b))
	require.Nil(t, f.commitRow(b).DeletedAt)

	flushed := f.store.flushed
	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 1, n, "what retention retired goes at once")
	require.True(t, f.store.tombstoned(a))
	require.False(t, f.store.tombstoned(b))
	require.Equal(t, flushed+1, f.store.flushed)

	require.NotNil(t, f.commitRow(a).TombstonedAt)

	require.Len(t, f.ranges(f.x, "files"), 2, "the version only a held was dropped")

	require.ErrorIs(t, f.x.UndeleteCommit(f.ctx, a), backup.ErrTombstoned)
	_, err = f.pin(a)
	require.ErrorIs(t, err, backup.ErrTombstoned)

	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(c))
}

func TestRetireAgesOutASetThatStoppedCommitting(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1, Brackets: []retention.Bracket{{Period: retention.Daily, Count: 2}}}))
	f.advance(24 * time.Hour)
	b := f.commit("world", f.tree(f.file("a.txt", "two")), false)

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Zero(t, n)

	n, err = f.x.Retire(f.ctx, f.clock.Add(5*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NotNil(t, f.commitRow(a).TombstonedAt)
	require.Nil(t, f.commitRow(b).TombstonedAt, "keep-last holds the newest")
}

func TestARetirementFarPastTheLastRunRetiresNothing(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1, Brackets: []retention.Bracket{{Period: retention.Daily, Count: 2}}}))
	f.advance(24 * time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Zero(t, n)

	ahead := f.clock.Add(40 * 24 * time.Hour)
	n, err = f.x.Retire(f.ctx, ahead)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Nil(t, f.commitRow(a).TombstonedAt)

	n, err = f.x.Retire(f.ctx, ahead.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n, "the next run goes on from the one before")
}

func TestARetirementFarPastASetsNewestCommitLeavesTheSetAsItIs(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1, Brackets: []retention.Bracket{{Period: retention.Daily, Count: 2}}}))
	f.advance(24 * time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	n, err := f.x.Retire(f.ctx, f.clock.Add(401*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)
	require.Nil(t, f.commitRow(a).TombstonedAt)
}

func TestRetireMarksRowsOnlyAfterFlush(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	f.store.flushErr = errors.New("disk full")
	later := f.clock.Add(15 * 24 * time.Hour)

	_, err := f.x.Retire(f.ctx, later)
	require.ErrorIs(t, err, f.store.flushErr)
	require.True(t, f.store.tombstoned(a))

	require.Nil(t, f.commitRow(a).TombstonedAt, "the row waits for the tombstone to be durable")
	require.True(t, f.deleted(f.x, a), "but the commit is spoken for from the moment the tombstone is written")

	// nothing revives a commit whose tombstone is on its way
	require.ErrorIs(t, f.x.UndeleteCommit(f.ctx, a), backup.ErrTombstoned)
	require.ErrorIs(t, f.x.DeleteCommit(f.ctx, a), backup.ErrTombstoned)
	_, err = f.pin(a)
	require.ErrorIs(t, err, backup.ErrTombstoned)

	f.store.flushErr = nil
	n, err := f.x.Retire(f.ctx, later)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the next run tombstones the commit again")

	require.NotNil(t, f.commitRow(a).TombstonedAt)
	require.True(t, f.deleted(f.x, a))

	readable, err := f.x.References(f.ctx, a)
	require.NoError(t, err)
	require.False(t, readable, "a tombstoned commit is no longer readable")

	_, err = f.pin(a)
	require.ErrorIs(t, err, backup.ErrTombstoned)
}

// TestPinsFollowTheSetState: a closing set accepts neither pins nor
// undeletes, or its commits would outlive the deletion.
func TestPinsFollowTheSetState(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	require.NoError(t, f.x.DeleteSet(f.ctx, "world", false))
	_, err := f.pin(a)
	require.ErrorIs(t, err, backup.ErrSetClosed)
	require.ErrorIs(t, f.x.UndeleteCommit(f.ctx, a), backup.ErrSetClosed)

	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

func TestDeleteCommitRefusesAPinnedCommit(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	p, err := f.pin(a)
	require.NoError(t, err)
	require.ErrorIs(t, f.x.DeleteCommit(f.ctx, a), backup.ErrPinned)

	require.NoError(t, f.x.Unpin(f.ctx, p))
	require.NoError(t, f.x.DeleteCommit(f.ctx, a))
}

func TestRebuiltSetKeepsEverythingWhilePaused(t *testing.T) {
	f := newFixture(t)

	var refs []*proto.Ref
	for i := 0; i < 3; i++ {
		refs = append(refs, f.commit("world", f.tree(f.file("a.txt", fmt.Sprint(i))), false))
		f.advance(time.Hour)
	}

	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	commits, err := y.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, commits, 3, "no policy applies while retention is paused")

	// the policy the operator sets once the set is rebuilt retires what it
	// does not keep from then, not from when the commits were taken
	f.advance(20 * 24 * time.Hour)
	require.NoError(t, y.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 2}))

	row := f.commitRowIn(y, refs[0])
	sameInstant(t, row.RetireAt, f.clock)
	sameInstant(t, row.ExpiresAt, f.clock)

	n, err := y.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestDeletedSetProceedsWhilePaused(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))
	require.NoError(t, y.DeleteSet(f.ctx, "world", true))

	n, err := y.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 2, n, "erasure does not wait for a policy")

	require.Equal(t, index.SetDeleted, f.setState(y, "world"))
}

// TestTombstonePrunesTheRefsOnlyItReaches retires a commit and expects the
// store to lose read access to its tree and file, but not to the subtree
// it shares with the commit that stays.
func TestTombstonePrunesTheRefsOnlyItReaches(t *testing.T) {
	f := newFixture(t)

	shared := f.dir("shared", f.file("s.txt", "same"))
	one := f.file("a.txt", "one")
	old := f.tree(shared, one)
	a := f.commit("world", old, false)

	f.advance(time.Hour)
	two := f.file("a.txt", "two")
	next := f.tree(shared, two)
	b := f.commit("world", next, false)

	readable := func(ref *proto.Ref) bool {
		ok, err := f.x.References(f.ctx, ref)
		require.NoError(t, err)
		return ok
	}

	for _, ref := range []*proto.Ref{a, old.Ref(), one.Ref, shared.Ref, b, next.Ref(), two.Ref} {
		require.True(t, readable(ref), "%x before the tombstone", ref.Hash)
	}

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	for _, ref := range []*proto.Ref{a, old.Ref(), one.Ref} {
		require.False(t, readable(ref), "%x only the tombstoned commit reaches", ref.Hash)
	}

	for _, ref := range []*proto.Ref{shared.Ref, b, next.Ref(), two.Ref} {
		require.True(t, readable(ref), "%x the live commit reaches", ref.Hash)
	}
}

func TestTombstonePrunesASetWhoseLiveCommitLostADirectory(t *testing.T) {
	f := newFixture(t)

	one := f.file("a.txt", "one")
	f.commit("world", f.tree(one), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	lost := f.dir("d", f.file("x.txt", "x"))
	f.commit("world", f.tree(f.file("a.txt", "two"), lost), false)
	f.advance(time.Hour)
	f.commit("logs", f.tree(f.file("l.txt", "1")), false)
	f.advance(time.Hour)
	f.commit("logs", f.tree(f.file("l.txt", "2")), false)

	f.store.mu.Lock()
	delete(f.store.objects, string(lost.Ref.Hash))
	f.store.mu.Unlock()

	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	for _, name := range []string{"world", "logs"} {
		require.NoError(t, y.SetPolicy(f.ctx, name, &retention.Policy{KeepLast: 1}))
	}

	retired, err := y.RetireCommits(f.ctx, f.clock)
	require.NoError(t, err)
	require.Len(t, retired, 2)

	readable, err := y.References(f.ctx, one.Ref)
	require.NoError(t, err)
	require.False(t, readable, "only the tombstoned commit reaches it")
}

func TestReIndexReconcilesTombstonesOnAnExistingDatabase(t *testing.T) {
	f := newFixture(t)

	old := f.tree(f.file("a.txt", "one"))
	a := f.commit("world", old, false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	// a database backup taken while both commits were live
	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))
	require.NoError(t, y.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 100}))
	require.Len(t, f.ranges(y, "files"), 2)

	// the live database retires the older commit meanwhile
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// the backup, restored and rebuilt, honours the tombstone it predates
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	require.NotNil(t, f.commitRowIn(y, a).TombstonedAt)

	commits, err := y.CommitInfo(f.ctx, "world", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 1)
	require.Len(t, f.ranges(y, "files"), 1, "the tombstoned version's row is gone")

	readable, err := y.References(f.ctx, a)
	require.NoError(t, err)
	require.False(t, readable)

	readable, err = y.References(f.ctx, old.Ref())
	require.NoError(t, err)
	require.False(t, readable, "the tombstoned commit's tree is unreadable too")
}

func TestRestoreLeasesHoldRetirement(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	later := f.clock.Add(15 * 24 * time.Hour)
	f.store.leases = []*proto.Ref{a}

	n, err := f.x.Retire(f.ctx, later)
	require.NoError(t, err)
	require.Zero(t, n, "a commit a restore reads is not tombstoned")
	require.False(t, f.store.tombstoned(a))

	f.store.leases = nil
	n, err = f.x.Retire(f.ctx, later)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, f.store.tombstoned(a))
}

func TestAPinTakesATrashedCommitOutOfTheTrashForARebuildToo(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	trashed := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "three")), false)

	require.NoError(t, f.x.DeleteCommit(f.ctx, trashed))
	f.advance(time.Hour)
	_, err := f.pin(trashed)
	require.NoError(t, err)
	require.Nil(t, f.commitRow(trashed).DeletedAt)

	rebuilt := f.index()
	require.NoError(t, reindexErr(rebuilt.ReIndex(f.ctx)))

	row := f.commitRowIn(rebuilt, trashed)
	require.Nil(t, row.DeletedAt)
	require.Nil(t, row.ExpiresAt)

	details, err := rebuilt.CommitDetails(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, details, 3)
}

func TestPinsKeepCommitsAlive(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	require.NotNil(t, f.commitRow(a).RetireAt)

	p, err := f.pin(a)
	require.NoError(t, err)

	row := f.commitRow(a)
	require.Equal(t, "pinned", row.RetainedBy)
	require.Nil(t, row.RetireAt)
	require.Nil(t, row.ExpiresAt)

	pins, err := f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.True(t, pins[0].Ref.Equal(p))
	require.True(t, pins[0].Target.Equal(a))
	require.NotZero(t, pins[0].ReceivedAtNs)

	n, err := f.x.Retire(f.ctx, f.clock.Add(30*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)

	f.advance(24 * time.Hour)
	require.NoError(t, f.x.Unpin(f.ctx, p))
	require.True(t, f.store.tombstoned(p))
	require.ErrorIs(t, f.x.Unpin(f.ctx, p), backup.ErrTombstoned)

	pins, err = f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Empty(t, pins)

	row = f.commitRow(a)
	require.Empty(t, row.RetainedBy)
	sameInstant(t, row.RetireAt, f.clock)

	n, err = f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, f.store.tombstoned(a))
}

func TestPartialCheckpointRetiresWhenSuperseded(t *testing.T) {
	f := newFixture(t)

	full := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	partial := f.commit("world", f.tree(f.file("a.txt", "one"), f.file("b.txt", "half")), true)

	latest, err := f.x.LatestCommit(f.ctx, "world")
	require.NoError(t, err)
	require.True(t, latest.Equal(partial), "a checkpoint is the diff base")

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, commits, 1, "but not listed")

	row := f.commitRow(partial)
	require.Equal(t, "latest", row.RetainedBy)
	require.Nil(t, row.RetireAt)

	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "one"), f.file("b.txt", "full")), false)

	row = f.commitRow(partial)
	require.NotNil(t, row.RetireAt)
	sameInstant(t, row.ExpiresAt, f.clock)

	require.Nil(t, f.commitRow(full).RetireAt)

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, f.store.tombstoned(partial))
}

func TestDeleteSetAndRebuild(t *testing.T) {
	f := newFixture(t)

	worldA := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	worldB := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	logs := f.commit("logs", f.tree(f.file("x.log", "l")), false)
	p, err := f.pin(logs)
	require.NoError(t, err)

	require.NoError(t, f.x.DeleteSet(f.ctx, "world", true))

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 2, n, "erasure uses a zero window")
	require.True(t, f.store.tombstoned(worldA))
	require.True(t, f.store.tombstoned(worldB))
	_, erased := f.store.erased[string(worldA.Hash)]
	require.True(t, erased, "an erased set writes erase tombstones")

	require.Equal(t, index.SetDeleted, f.setState(f.x, "world"))
	require.Len(t, f.ranges(f.x, "files"), 1, "only the logs row is left")

	require.ErrorIs(t, f.x.UndeleteSet(f.ctx, "world"), backup.ErrTombstoned)

	// a rebuild from the archives alone
	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	count, err := y.client.CommitRow.Query().Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count, "tombstoned commits are not indexed")
	require.True(t, f.deleted(y, worldA), "the rebuild records the tombstones")
	require.True(t, f.deleted(y, worldB))
	require.False(t, f.deleted(y, logs))

	// the deletion outlives the database it was made in
	obj, err := f.store.Get(f.ctx, worldA)
	require.NoError(t, err)
	require.ErrorIs(t, y.Put(f.ctx, obj), backup.ErrSetClosed)

	count, err = y.client.Pin.Query().Where(pin.DeletedAtIsNil()).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	logsSet, err := y.client.Set.Query().Where(set.Name("logs")).Only(f.ctx)
	require.NoError(t, err)
	require.True(t, logsSet.RetentionPaused, "a rebuilt set waits for its policy")

	pins, err := y.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.True(t, pins[0].Ref.Equal(p))

	require.NoError(t, y.SetPolicy(f.ctx, "logs", &retention.Policy{KeepLast: 1}))
	logsSet, err = y.client.Set.Get(f.ctx, logsSet.ID)
	require.NoError(t, err)
	require.False(t, logsSet.RetentionPaused)
}

func TestReIndexReproducesRanges(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.file("a.txt", "one"), f.dir("world", f.file("level.dat", "w1"))), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two"), f.dir("world", f.file("level.dat", "w1"))), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.dir("world", f.file("level.dat", "w2"), f.dir("region", f.file("r.0", "r")))), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.dir("world", f.file("level.dat", "w2"))), false)

	y := f.index()
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	for _, table := range []string{"files", "trees"} {
		require.Equal(t, f.ranges(f.x, table), f.ranges(y, table), table)
	}

	require.Len(t, f.ranges(f.x, "files"), 5)
	require.Len(t, f.ranges(f.x, "trees"), 4)
}

// TestPresenceFilterNeverLandsOnATombstonedCommit stores a presence
// filter for a commit after its tombstone and expects the row to stay
// without one.
func TestPresenceFilterNeverLandsOnATombstonedCommit(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(PresenceEvery)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	setID, err := findSet(f.ctx, f.x.client, "world")
	require.NoError(t, err)

	filter := presence.New(1)
	filter.Add(a.Hash)
	require.NoError(t, storePresence(f.ctx, f.x, setID, a, filter.Proto()))

	require.Empty(t, f.commitRow(a).Presence, "a tombstoned commit carries no filter")

	stored, err := f.x.client.CommitRow.Query().Where(commitrow.PresenceNotNil()).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stored, "the live head keeps its filter")
}

// TestCommitInfoReproducesTheRef: a listed commit carries its whole hashed
// body, so a restore can name it by ref again.
func TestCommitInfoReproducesTheRef(t *testing.T) {
	f := newFixture(t)

	parent := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)

	obj := proto.NewObject(&proto.Commit{
		Timestamp:     f.clock.Unix(),
		Tree:          f.tree(f.file("a.txt", "two")).Ref(),
		BackupSet:     "world",
		Parent:        parent,
		AgentId:       "node-1",
		ScanStartNs:   f.clock.Add(-time.Minute).UnixNano(),
		PolicyVersion: 3,
		Consistent:    true,
	})
	require.NoError(t, f.x.Put(f.ctx, obj))
	f.presence()

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 1)
	require.NoError(t, err)
	require.Len(t, commits, 1)
	require.True(t, proto.NewObject(commits[0]).Ref().Equal(obj.Ref()), "listed commit %+v hashes differently", commits[0])
	require.True(t, commits[0].GetParent().Equal(parent))
}

func TestACommitKeepsTheKeyItRecords(t *testing.T) {
	f := newFixture(t)

	for i, keyID := range [][]byte{nil, {}, {1, 2, 3, 4, 5, 6, 7, 8}} {
		f.advance(time.Hour)

		obj := proto.NewObject(&proto.Commit{
			Timestamp: f.clock.Unix(),
			Tree:      f.tree(f.file("a.txt", fmt.Sprint(i))).Ref(),
			BackupSet: "world",
			KeyId:     keyID,
		})
		require.NoError(t, f.x.Put(f.ctx, obj))

		commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 1)
		require.NoError(t, err)
		require.Equal(t, keyID, commits[0].KeyId)
		require.True(t, proto.NewObject(commits[0]).Ref().Equal(obj.Ref()), "key id %x", keyID)
	}
}

// TestConcurrentPinsOfOneSet pins several commits of a set at once; every
// pin re-evaluates the set, which must not deadlock on the commit rows.
func TestConcurrentPinsOfOneSet(t *testing.T) {
	f := newFixture(t)

	var commits []*proto.Ref
	for i := range 6 {
		commits = append(commits, f.commit("world", f.tree(f.file("a.txt", fmt.Sprint(i))), false))
		f.advance(time.Minute)
	}

	for range 3 {
		var wg sync.WaitGroup
		results := make([]error, len(commits))
		for i, ref := range commits {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, results[i] = f.pin(ref)
			}()
		}
		wg.Wait()

		for i, err := range results {
			require.NoError(t, err, "pinning commit %d", i)
		}

		pins, err := f.x.Pins(f.ctx)
		require.NoError(t, err)
		for _, p := range pins {
			require.NoError(t, f.x.Unpin(f.ctx, p.Ref))
		}
	}
}

func TestPinsFollowTheirSet(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	p, err := f.pin(a)
	require.NoError(t, err)

	pins, err := f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)

	// a pin whose commit's set the query cannot see is not there; the
	// row goes behind the foreign keys' back, as a policy would hide it
	_, err = f.x.db.ExecContext(f.ctx, "PRAGMA foreign_keys = OFF")
	require.NoError(t, err)
	require.NoError(t, f.x.client.Set.DeleteOneID(f.commitRow(a).SetID).Exec(f.ctx))

	pins, err = f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Empty(t, pins)
	require.ErrorIs(t, f.x.Unpin(f.ctx, p), backup.ErrNotFound)
}

func TestSymlinksAreIndexed(t *testing.T) {
	f := newFixture(t)

	t0 := f.clock
	rootA := f.tree(f.file("a.txt", "one"), f.symlink("current", "worlds/2026"))
	f.commit("world", rootA, false)

	versions, err := f.x.FileInfo(f.ctx, "world", "current", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)
	require.Equal(t, proto.NodeType_NODE_SYMLINK, versions[0].Stat.Type)
	require.Equal(t, "worlds/2026", string(versions[0].Stat.LinkTarget))
	require.Nil(t, versions[0].Ref, "a symlink has no object of its own")

	// a symlink that still points at the same place is one version
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "one"), f.symlink("current", "worlds/2026")), false)

	versions, err = f.x.FileInfo(f.ctx, "world", "current", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 1)

	// repointing it closes the old version and opens a new one
	f.advance(time.Hour)
	t2 := f.clock
	f.commit("world", f.tree(f.file("a.txt", "one"), f.symlink("current", "worlds/2027")), false)

	versions, err = f.x.FileInfo(f.ctx, "world", "current", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, "worlds/2027", string(versions[0].Stat.LinkTarget))
	require.Equal(t, "worlds/2026", string(versions[1].Stat.LinkTarget))

	var links []rangeRow
	for _, row := range f.ranges(f.x, "files") {
		if row.path == "current" {
			links = append(links, row)
		}
	}

	require.Len(t, links, 2)
	require.True(t, links[0].validFrom.Equal(t0))
	closedAt(t, links[0], t2)
	require.Nil(t, links[1].validUntil)
}

func TestASymlinkReplacedByAFileIsANewVersion(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.symlink("current", "worlds/2026")), false)

	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("current", "no longer a link")), false)

	versions, err := f.x.FileInfo(f.ctx, "world", "current", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, proto.NodeType_NODE_FILE, versions[0].Stat.Type)
	require.NotNil(t, versions[0].Ref)
	require.Empty(t, versions[0].Stat.LinkTarget)
	require.Equal(t, proto.NodeType_NODE_SYMLINK, versions[1].Stat.Type)
}

func TestReadDirListsWhatTheSetHeldThen(t *testing.T) {
	f := newFixture(t)

	t0 := f.clock
	f.commit("world", f.tree(
		f.file("a.txt", "one"),
		f.symlink("current", "worlds/2026"),
		f.dir("world", f.file("level.dat", "w1"), f.dir("region", f.file("r.0", "r"))),
	), false)

	f.advance(time.Hour)
	t1 := f.clock
	f.commit("world", f.tree(
		f.file("b.txt", "two"),
		f.dir("world", f.file("level.dat", "w2")),
	), false)

	x, ok := interface{}(f.x).(backup.DirLister)
	require.True(t, ok)

	names := func(entries []*proto.TreeNode) []string {
		out := make([]string, len(entries))
		for i, e := range entries {
			out[i] = string(e.GetStat().GetName())
		}
		return out
	}

	entries, err := x.ReadDir(f.ctx, "world", "", t0)
	require.NoError(t, err)
	require.Equal(t, []string{"a.txt", "current", "world"}, names(entries))
	require.Equal(t, proto.NodeType_NODE_DIRECTORY, entries[2].Stat.Type)
	require.NotNil(t, entries[2].Ref, "a directory entry can be descended into")

	entries, err = x.ReadDir(f.ctx, "world", "", t1)
	require.NoError(t, err)
	require.Equal(t, []string{"b.txt", "world"}, names(entries), "the old root is gone at t1")

	entries, err = x.ReadDir(f.ctx, "world", "world", t0)
	require.NoError(t, err)
	require.Equal(t, []string{"level.dat", "region"}, names(entries))
	require.EqualValues(t, 2, entries[0].Stat.Size)

	entries, err = x.ReadDir(f.ctx, "world", "world", t1)
	require.NoError(t, err)
	require.Equal(t, []string{"level.dat"}, names(entries), "region went away")

	entries, err = x.ReadDir(f.ctx, "world", "nowhere", t1)
	require.NoError(t, err)
	require.Empty(t, entries)

	entries, err = x.ReadDir(f.ctx, "other", "", t1)
	require.NoError(t, err)
	require.Empty(t, entries, "an unknown set lists nothing")
}

func TestCommitDetailsCarryTheSizeAndWhyEachCommitIsKept(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 2}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two"), f.file("b.txt", "three")), false)

	held, err := f.x.CommitDetails(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, held, 2)

	require.Equal(t, "latest,last", held[0].RetainedBy, "the newest commit, and within keep_last")
	require.Equal(t, "last", held[1].RetainedBy)
	require.NotNil(t, held[0].LogicalSize)
	require.EqualValues(t, 8, *held[0].LogicalSize, "both files of the newest commit")
	require.EqualValues(t, 3, *held[1].LogicalSize, "only a.txt was there then")

	// the commit itself is the same one CommitInfo returns
	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, commits, 2)

	sized, sizes, err := f.x.CommitSizes(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, sized, 2)
	require.EqualValues(t, 8, sizes[0].GetLogicalBytes())
	require.EqualValues(t, 2, sizes[0].GetFiles())
	require.EqualValues(t, 1, sizes[1].GetFiles())
	require.Equal(t, commits[0].GetReceivedAtNs(), held[0].Commit.GetReceivedAtNs())

	// a commit retention has retired is not offered, as with CommitInfo
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))

	held, err = f.x.CommitDetails(f.ctx, "world", f.clock, 10)
	require.NoError(t, err)
	require.Len(t, held, 1)
	require.NotEqual(t, a.String(), held[0].Commit.GetTree().String())
}

func TestAPinKeepsTheLabelItWasWrittenWith(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.Put(f.ctx, proto.NewObject(&proto.Pin{
		Target: a, Metadata: map[string]string{"label": "before-migration"},
	})))

	pins, err := f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.True(t, pins[0].GetTarget().Equal(a))
	require.Equal(t, "before-migration", pins[0].GetMetadata()["label"],
		"the store keeps the pin's labels and hands them back")

	// a pin written without any is not a pin with an empty label
	f.advance(time.Hour)
	b := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	require.NoError(t, f.x.Put(f.ctx, proto.NewObject(&proto.Pin{Target: b})))

	pins, err = f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 2)
	require.Empty(t, pins[1].GetMetadata())
}

func TestRetireKeepsACommitAPinInTheStoreHolds(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	// the pin reached the store but not this index, as with an index
	// restored from a copy older than the pin
	pin := proto.NewObject(&proto.Pin{Target: a, ReceivedAtNs: f.clock.UnixNano()})
	require.NoError(t, f.store.Put(f.ctx, pin))

	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)
	require.False(t, f.store.tombstoned(a))

	pins, err := f.x.Pins(f.ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1, "the index learns the pin it missed")

	n, err = f.x.Retire(f.ctx, f.clock.Add(30*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n, "the commit stays held")
}

func TestRetireIgnoresAPinTheStoreUnpinned(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	pin := proto.NewObject(&proto.Pin{Target: a, ReceivedAtNs: f.clock.UnixNano()})
	require.NoError(t, f.store.Put(f.ctx, pin))
	require.NoError(t, f.store.Delete(f.ctx, pin.Ref()))

	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, f.store.tombstoned(a))
}

func TestRetiringCommitsThatShareADirectoryReadsItOnce(t *testing.T) {
	f := newFixture(t)

	var shared []*proto.TreeNode
	for i := range 20 {
		shared = append(shared, f.file(fmt.Sprintf("f%02d", i), "same"))
	}

	sharedDir := f.dir("shared", shared...)

	commits := retireBatch + 30

	var onlyRetired []*proto.Ref
	for c := range commits {
		changing := f.file("changing", fmt.Sprint("v", c))
		f.commit("world", f.tree(sharedDir, changing), false)

		if c == 0 {
			require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
		}

		if c < commits-1 {
			onlyRetired = append(onlyRetired, changing.Ref)
		}

		f.advance(time.Hour)
	}

	clear(f.store.read)

	n, err := f.x.Retire(f.ctx, f.clock.Add(400*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, commits-1, n)

	require.LessOrEqual(t, f.store.read[string(sharedDir.Ref.Hash)], 2, "once among the retired, once among the live")

	for _, ref := range onlyRetired {
		held, err := f.x.client.SetRef.Query().Where(setref.Ref(ref.Hash)).Exist(f.ctx)
		require.NoError(t, err)
		require.False(t, held, "a file only retired commits reached leaves the set")
	}

	held, err := f.x.client.SetRef.Query().Where(setref.Ref(shared[0].Ref.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.True(t, held, "a file the live commit reaches stays")
}

func TestARetirementRetriesThePruneOfASetWhosePruneFailed(t *testing.T) {
	f := newFixture(t)

	gone := f.file("a.txt", "one")
	a := f.commit("world", f.tree(gone), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	failed := errors.New("bucket unavailable")
	f.store.getErr = map[string]error{string(a.Hash): failed}

	n, err := f.x.Retire(f.ctx, f.clock)
	require.ErrorIs(t, err, failed)
	require.Equal(t, 1, n)

	held, err := f.x.client.SetRef.Query().Where(setref.Ref(gone.Ref.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.True(t, held, "the failed prune left it")

	n, err = f.x.Retire(f.ctx, f.clock.Add(time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)

	held, err = f.x.client.SetRef.Query().Where(setref.Ref(gone.Ref.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.False(t, held, "the next run pruned it though it tombstoned nothing")
}

// A pin or a longer window that arrives after Retire found its commits due
// still holds the commit when its tombstone would be written.
func TestATombstoneYieldsToWhatChangedAfterTheCommitFellDue(t *testing.T) {
	f := newFixture(t)

	pinned := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	extended := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "three")), false)

	later := f.clock.Add(400 * 24 * time.Hour)

	due, err := f.x.client.CommitRow.Query().
		Where(commitrow.RefIn(pinned.Hash, extended.Hash)).WithSet().All(f.ctx)
	require.NoError(t, err)
	require.Len(t, due, 2)

	_, err = f.pin(pinned)
	require.NoError(t, err)

	// the pin alone has to hold it, whatever its row says
	require.NoError(t, f.x.client.CommitRow.Update().Where(commitrow.Ref(pinned.Hash)).
		SetExpiresAt(f.clock).Exec(f.ctx))

	require.NoError(t, f.x.client.CommitRow.Update().Where(commitrow.Ref(extended.Hash)).
		SetExpiresAt(later.Add(time.Hour)).Exec(f.ctx))

	written, err := f.x.writeTombstones(f.ctx, due, later, nil)
	require.NoError(t, err)
	require.Empty(t, written)
	require.False(t, f.store.tombstoned(pinned))
	require.False(t, f.store.tombstoned(extended))
}

func TestATombstoneTheStoreRefusedIsWrittenByTheNextRun(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two")), false)

	later := f.clock.Add(15 * 24 * time.Hour)

	f.store.deleteErr = errors.New("bucket unreachable")
	_, err := f.x.Retire(f.ctx, later)
	require.ErrorIs(t, err, f.store.deleteErr)
	require.False(t, f.store.tombstoned(a))
	require.True(t, f.deleted(f.x, a), "the commit is spoken for before its tombstone goes out")
	require.ErrorIs(t, f.x.UndeleteCommit(f.ctx, a), backup.ErrTombstoned)

	f.store.deleteErr = nil
	n, err := f.x.Retire(f.ctx, later)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, f.store.tombstoned(a))
	require.NotNil(t, f.commitRow(a).TombstonedAt)
}

// headKeeper records the commits a store is asked to make heads.
type headKeeper struct {
	backup.ObjectStore
	heads []*proto.Ref
}

func (h *headKeeper) AdvanceHead(commit *proto.Object) error {
	h.heads = append(h.heads, commit.Ref())
	return nil
}

func (h *headKeeper) Unwrap() backup.ObjectStore { return h.ObjectStore }

func TestARebuildPointsEachSetAtItsCommits(t *testing.T) {
	f := newFixture(t)

	first := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	second := f.commit("world", f.tree(f.file("a.txt", "two")), false)

	keeper := &headKeeper{ObjectStore: f.store}
	y := openIndex(t, keeper)
	require.NoError(t, reindexErr(y.ReIndex(f.ctx)))

	require.ElementsMatch(t, []*proto.Ref{first, second}, keeper.heads)
}

func TestACommitPlannedBeforeAnotherOfItsSetLandedWritesNothingStale(t *testing.T) {
	f := newFixture(t)

	f.commit("world", f.tree(f.dir("d", f.file("a.txt", "one"))), false)

	f.advance(time.Hour)
	other := f.tree(f.dir("d", f.file("a.txt", "two")))
	planned := f.tree(f.dir("d", f.file("a.txt", "three")))

	// the other commit lands while the planned one reads its directory;
	// it was received later, so the planned one is the older and fails
	f.store.beforeGet = map[string]func(){string(planned.GetTree().Nodes[0].Ref.Hash): func() {
		f.commit("world", other, false)
	}}

	plannedObj := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: planned.Ref(), BackupSet: "world", AgentId: "node-1"})
	require.Error(t, f.x.Put(f.ctx, plannedObj))
	plannedRef := plannedObj.Ref()

	var versions []rangeRow
	for _, r := range f.ranges(f.x, "files") {
		if r.path == "d/a.txt" {
			versions = append(versions, r)
		}
	}

	require.Len(t, versions, 2, "the planned commit's writes were not made")
	require.Equal(t, versions[1].validFrom, *versions[0].validUntil)
	require.Nil(t, versions[1].validUntil)

	exists, err := f.x.client.CommitRow.Query().Where(commitrow.Ref(plannedRef.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.False(t, exists)
}

// trashNewest commits two versions of a set, the second with b.txt
// changed in d, then deletes the second and lets its trash window pass.
func trashNewest(t *testing.T, f *fixture) *proto.TreeNode {
	t.Helper()

	f.commit("world", f.tree(f.file("a.txt", "one"), f.dir("d", f.file("a.txt", "one"), f.file("b.txt", "one"))), false)
	f.advance(time.Hour)
	changed := f.dir("d", f.file("a.txt", "one"), f.file("b.txt", "two"))
	newest := f.commit("world", f.tree(f.file("a.txt", "one"), changed), false)
	require.NoError(t, f.x.DeleteCommit(f.ctx, newest))
	f.advance(15 * 24 * time.Hour)

	return changed
}

func TestACommitAfterATombstonedNewestOneHoldsOnlyWhatItHolds(t *testing.T) {
	f := newFixture(t)
	trashNewest(t, f)

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	f.advance(time.Hour)
	last := f.commit("world", f.tree(f.file("e.txt", "e")), false)

	entries, err := f.x.ReadDir(f.ctx, "world", "d", f.clock)
	require.NoError(t, err)
	require.Empty(t, entries, "d went with the commit after the tombstoned one")
	require.EqualValues(t, 1, *f.commitRow(last).FileCount)
}

func TestACommitPlannedWhileItsSetsNewestIsTombstonedKeepsWhatTheyShare(t *testing.T) {
	f := newFixture(t)
	changed := trashNewest(t, f)

	var retired int
	var retireErr error

	fresh := f.dir("new", f.file("c.txt", "c"))
	f.store.beforeGet = map[string]func(){string(fresh.Ref.Hash): func() {
		retired, retireErr = f.x.Retire(f.ctx, f.clock)
	}}

	last := f.commit("world", f.tree(f.file("a.txt", "one"), changed, fresh), false)
	require.NoError(t, retireErr)
	require.Equal(t, 1, retired, "the newest commit was tombstoned while the next was planned")

	for dir, want := range map[string]int{"": 3, "d": 2, "new": 1} {
		entries, err := f.x.ReadDir(f.ctx, "world", dir, f.clock)
		require.NoError(t, err)
		require.Len(t, entries, want, dir)
	}

	require.EqualValues(t, 4, *f.commitRow(last).FileCount)

	for _, ref := range []*proto.Ref{changed.Ref, f.file("b.txt", "two").Ref} {
		readable, err := f.x.References(f.ctx, ref)
		require.NoError(t, err)
		require.True(t, readable)
	}
}

func TestRetiringKeepsTheRefsOfACommitThatLandsWhileItWalks(t *testing.T) {
	f := newFixture(t)

	shared := f.file("shared.txt", "shared")
	f.commit("world", f.tree(shared), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 2}))
	f.advance(time.Hour)
	kept := f.tree(f.file("b.txt", "two"))
	f.commit("world", kept, false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("c.txt", "three")), false)

	// the first commit is retired; while its refs are weighed against the
	// live commits, one that holds its file again lands
	f.store.beforeGet = map[string]func(){string(kept.Ref().Hash): func() {
		f.advance(time.Hour)
		f.commit("world", f.tree(shared), false)
	}}

	n, err := f.x.Retire(f.ctx, f.clock)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	held, err := f.x.client.SetRef.Query().Where(setref.Ref(shared.Ref.Hash)).Exist(f.ctx)
	require.NoError(t, err)
	require.True(t, held, "a ref a live commit holds was dropped")
}

func TestReIndexPlacesACommitReceivedWithTheSetsNewestJustAfterIt(t *testing.T) {
	f := newFixture(t)

	received := f.clock.UnixNano()
	put := func(content string) {
		obj := proto.NewObject(&proto.Commit{Timestamp: f.clock.Unix(), Tree: f.tree(f.file("a.txt", content)).Ref(),
			BackupSet: "world", AgentId: "node-1", ReceivedAtNs: received})
		require.NoError(t, f.store.Put(f.ctx, obj))
	}

	put("one")
	put("two")

	y := f.index()
	report, err := y.ReIndex(f.ctx)
	require.NoError(t, err)
	require.Equal(t, backup.ReIndexReport{Tied: 1}, report)

	commits, err := y.CommitInfo(f.ctx, "world", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 2)

	versions, err := y.FileInfo(f.ctx, "world", "a.txt", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, versions, 2, "each commit keeps its own version")
}

func TestEvaluationRetiresEachCommitOnceAndKeepsWhatAWiderPolicyCovers(t *testing.T) {
	f := newFixture(t)

	var refs []*proto.Ref
	for i := range 5 {
		refs = append(refs, f.commit("world", f.tree(f.file("a.txt", fmt.Sprint(i))), false))
		if i == 0 {
			require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 2}))
		}
		f.advance(time.Hour)
	}

	// commit i is retired by the commit two after it
	first := f.clock.Add(-3 * time.Hour)
	for i, ref := range refs[:3] {
		row := f.commitRow(ref)
		require.Empty(t, row.RetainedBy)
		sameInstant(t, row.RetireAt, first.Add(time.Duration(i)*time.Hour))
		require.NotNil(t, row.RetirePolicy)
	}

	require.Equal(t, "last", f.commitRow(refs[3]).RetainedBy)
	require.Equal(t, "latest,last", f.commitRow(refs[4]).RetainedBy)

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 4}))

	require.NotNil(t, f.commitRow(refs[0]).RetireAt, "beyond the wider policy still")
	for _, ref := range refs[1:4] {
		row := f.commitRow(ref)
		require.Equal(t, "last", row.RetainedBy)
		require.Nil(t, row.RetireAt)
		require.Nil(t, row.RetirePolicy)
		require.Nil(t, row.ExpiresAt)
	}
	require.Equal(t, "latest,last", f.commitRow(refs[4]).RetainedBy)
}

func TestOneRetirementPrunesEachSetsDeepTreesOnItsOwn(t *testing.T) {
	f := newFixture(t)

	type set struct {
		gone, kept []*proto.Ref
	}

	sets := map[string]*set{}

	for _, name := range []string{"world", "other"} {
		shared := f.dir("shared", f.dir("deeper", f.file("s.txt", name)))
		one := f.file("a.txt", name+" one")
		inner := f.dir("inner", f.dir("deeper", one))
		old := f.tree(shared, inner)
		a := f.commit(name, old, false)

		f.advance(time.Hour)
		two := f.file("b.txt", name+" two")
		next := f.tree(shared, two)
		b := f.commit(name, next, false)

		sets[name] = &set{gone: []*proto.Ref{a, old.Ref(), inner.Ref, one.Ref}, kept: []*proto.Ref{shared.Ref, b, next.Ref(), two.Ref}}
		require.NoError(t, f.x.SetPolicy(f.ctx, name, &retention.Policy{KeepLast: 1}))
	}

	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, n)

	for name, s := range sets {
		for _, ref := range s.gone {
			ok, err := f.x.References(f.ctx, ref)
			require.NoError(t, err)
			require.False(t, ok, "%s: %x only the tombstoned commit reaches", name, ref.Hash)
		}

		for _, ref := range s.kept {
			ok, err := f.x.References(f.ctx, ref)
			require.NoError(t, err)
			require.True(t, ok, "%s: %x the live commit reaches", name, ref.Hash)
		}
	}
}
