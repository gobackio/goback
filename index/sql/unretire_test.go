package sql

import (
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// retiredHistory commits three versions of a set, the last of which alone
// a policy keeps, retires the other two and returns them with the files
// and trees rows the set had before.
func retiredHistory(t *testing.T, f *fixture) ([]*proto.Ref, []rangeRow, []rangeRow) {
	t.Helper()

	one := f.file("a.txt", "one")
	a := f.commit("world", f.tree(one, f.dir("sub", f.file("b.txt", "x"))), false)
	f.advance(time.Hour)
	b := f.commit("world", f.tree(one, f.dir("sub", f.file("b.txt", "y"))), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "two"), f.dir("sub", f.file("b.txt", "y"))), false)

	files, trees := f.ranges(f.x, "files"), f.ranges(f.x, "trees")

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 1}))
	n, err := f.x.Retire(f.ctx, f.clock.Add(15*24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Less(t, len(f.ranges(f.x, "files")), len(files))

	return []*proto.Ref{a, b}, files, trees
}

func TestUnretireBringsTombstonedCommitsBackWithTheirRows(t *testing.T) {
	f := newFixture(t)
	retired, files, trees := retiredHistory(t, f)

	since := f.commitRow(retired[0]).TombstonedAt.Add(-time.Second)
	listed, err := f.x.TombstonedCommits(f.ctx, "world", since)
	require.NoError(t, err)
	require.Equal(t, retired, listed)

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 100}))

	dry, err := f.x.UnretireCommits(f.ctx, retired, true)
	require.NoError(t, err)
	require.Len(t, dry, 2)
	for _, u := range dry {
		require.True(t, u.Whole())
		require.Equal(t, "world", u.Set)
		require.NotEmpty(t, u.RetainedBy, "the new policy keeps it")
		require.NotNil(t, f.commitRow(u.Commit).TombstonedAt, "a dry run changes nothing")
	}

	done, err := f.x.UnretireCommits(f.ctx, retired, false)
	require.NoError(t, err)
	require.Equal(t, dry, done)

	for _, ref := range retired {
		row := f.commitRow(ref)
		require.Nil(t, row.TombstonedAt)
		require.Nil(t, row.RetireAt)
		require.Nil(t, row.ExpiresAt)
		require.False(t, f.deleted(f.x, ref))

		readable, err := f.x.References(f.ctx, ref)
		require.NoError(t, err)
		require.True(t, readable)
	}

	require.Equal(t, files, f.ranges(f.x, "files"), "the versions rows are what they were before the retirement")
	require.Equal(t, trees, f.ranges(f.x, "trees"))

	commits, err := f.x.CommitInfo(f.ctx, "world", f.clock.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, commits, 3)

	n, err := f.x.Retire(f.ctx, f.clock.Add(30*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)

	rebuilt := f.index()
	require.NoError(t, reindexErr(rebuilt.ReIndex(f.ctx)))
	for _, ref := range retired {
		require.Nil(t, f.commitRowIn(rebuilt, ref).TombstonedAt, "a rebuild honours the revival")
	}
	require.Equal(t, files, f.ranges(rebuilt, "files"))
}

func TestUnretireLeavesACommitMissingAnObjectTombstoned(t *testing.T) {
	f := newFixture(t)
	retired, _, _ := retiredHistory(t, f)

	broken := retired[0]
	commit, err := f.x.ObjectStore.Get(f.ctx, broken)
	require.NoError(t, err)
	f.store.mu.Lock()
	delete(f.store.objects, string(commit.GetCommit().GetTree().Hash))
	f.store.mu.Unlock()

	flushed := f.store.flushed

	done, err := f.x.UnretireCommits(f.ctx, retired, false)
	require.NoError(t, err)
	require.Equal(t, 1, done[0].MissingCount)
	require.Equal(t, commit.GetCommit().GetTree().Hash, done[0].Missing[0].Hash)
	require.Empty(t, done[0].RetainedBy)
	require.True(t, done[1].Whole())

	require.NotNil(t, f.commitRow(broken).TombstonedAt)
	require.True(t, f.deleted(f.x, broken))
	revived, err := f.store.Revived(f.ctx, broken)
	require.NoError(t, err)
	require.False(t, revived, "nothing takes the broken commit back")
	require.Nil(t, f.commitRow(retired[1]).TombstonedAt)
	require.Equal(t, flushed+1, f.store.flushed, "one policy, for the commit brought back")
}

func TestUnretireRefusesALiveCommit(t *testing.T) {
	f := newFixture(t)
	live := f.commit("world", f.tree(f.file("a.txt", "one")), false)

	_, err := f.x.UnretireCommits(f.ctx, []*proto.Ref{live}, true)
	require.ErrorContains(t, err, "not tombstoned")

	_, err = f.x.UnretireCommits(f.ctx, []*proto.Ref{{Hash: make([]byte, proto.HashSize)}}, true)
	require.ErrorIs(t, err, backup.ErrNotFound)
}

// TestUnretireFindsNoCommitOfASetItCannotSee stands a commit row whose set
// the query cannot see, as row-level security on sets hides another
// tenant's, for a commit of a hidden set.
func TestUnretireFindsNoCommitOfASetItCannotSee(t *testing.T) {
	f := newFixture(t)
	ref := &proto.Ref{Hash: make([]byte, proto.HashSize)}

	_, err := f.x.db.ExecContext(f.ctx, "PRAGMA foreign_keys = off")
	require.NoError(t, err)
	err = f.x.client.CommitRow.Create().SetRef(ref.Hash).SetTimestamp(f.clock).SetReceivedAt(f.clock).SetTree(ref.Hash).
		SetSetID(999).SetTombstonedAt(f.clock).Exec(f.ctx)
	require.NoError(t, err)
	_, err = f.x.db.ExecContext(f.ctx, "PRAGMA foreign_keys = on")
	require.NoError(t, err)

	_, err = f.x.UnretireCommits(f.ctx, []*proto.Ref{ref}, true)
	require.ErrorIs(t, err, backup.ErrNotFound)

	retired, err := f.x.RetiredCommits(f.ctx, []*proto.Ref{ref})
	require.NoError(t, err)
	require.Empty(t, retired)
}

// TestUnretireCompletesARevivalThatCrashedBeforeItsRows: the store holds the
// revival but the rows still say tombstoned. Retirement leaves the commit
// alone, a rebuild brings it back, and unretiring it again finishes the job.
func TestUnretireCompletesARevivalThatCrashedBeforeItsRows(t *testing.T) {
	f := newFixture(t)
	retired, files, _ := retiredHistory(t, f)
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 100}))

	_, err := f.store.Revive(f.ctx, retired, false)
	require.NoError(t, err)

	n, err := f.x.Retire(f.ctx, f.clock.Add(30*24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, n)
	require.NotNil(t, f.commitRow(retired[0]).TombstonedAt)

	rebuilt := f.index()
	require.NoError(t, reindexErr(rebuilt.ReIndex(f.ctx)))
	require.Nil(t, f.commitRowIn(rebuilt, retired[0]).TombstonedAt)

	done, err := f.x.UnretireCommits(f.ctx, retired, false)
	require.NoError(t, err)
	for _, u := range done {
		require.True(t, u.Whole())
		require.Nil(t, f.commitRow(u.Commit).TombstonedAt)
	}
	require.Equal(t, files, f.ranges(f.x, "files"))
}

func TestRetireReportsEachCommitWithThePolicyThatRetiredIt(t *testing.T) {
	f := newFixture(t)

	a := f.commit("world", f.tree(f.file("a.txt", "one")), false)
	f.advance(time.Hour)
	b := f.commit("world", f.tree(f.file("a.txt", "two")), false)
	f.advance(time.Hour)
	f.commit("world", f.tree(f.file("a.txt", "three")), false)

	policy := retention.Policy{KeepLast: 1}
	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &policy))
	decided := f.commitRow(a).RetireAt
	require.NotNil(t, decided)

	later := f.clock.Add(15 * 24 * time.Hour)
	retired, err := f.x.RetireCommits(f.ctx, later)
	require.NoError(t, err)
	require.Len(t, retired, 2)

	byRef := map[string]index.Retired{}
	for _, r := range retired {
		byRef[string(r.Ref.Hash)] = r
	}

	for _, ref := range []*proto.Ref{a, b} {
		r := byRef[string(ref.Hash)]
		row := f.commitRow(ref)
		require.Equal(t, "world", r.Set)
		require.Equal(t, f.setID("world"), r.SetID)
		require.True(t, row.Timestamp.Equal(r.Timestamp))
		require.True(t, row.ReceivedAt.Equal(r.ReceivedAt))
		require.True(t, decided.Equal(r.RetiredAt))
		require.True(t, later.Equal(r.TombstonedAt))
		require.Equal(t, &policy, r.Policy)
		require.False(t, r.Deleted)
		require.False(t, r.Partial)
		require.Equal(t, index.RetiredHeld, r.State)
	}

	f.store.mu.Lock()
	delete(f.store.objects, string(a.Hash))
	f.store.mu.Unlock()

	require.NoError(t, f.x.SetPolicy(f.ctx, "world", &retention.Policy{KeepLast: 100}))
	_, err = f.x.UnretireCommits(f.ctx, []*proto.Ref{b}, false)
	require.NoError(t, err)

	states, err := f.x.RetiredCommits(f.ctx, []*proto.Ref{a, b, {Hash: make([]byte, 32)}})
	require.NoError(t, err)
	require.Len(t, states, 2, "an unknown commit is left out")
	require.Equal(t, index.RetiredGone, states[0].State, "the store holds no copy of a")
	require.Equal(t, &policy, states[0].Policy)
	require.Equal(t, index.RetiredLive, states[1].State)
	require.Nil(t, states[1].Policy)
}
