package sql

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// pastBindLimit is more values than SQLite binds in one statement.
const pastBindLimit = 33000

// rows adds n commit rows to the set, received before any the fixture
// commits, tombstoned when asked, and returns their refs.
func (f *fixture) rows(name string, n int, tombstoned bool) []*proto.Ref {
	f.t.Helper()

	setID, err := ensureSet(f.ctx, f.x.client, name, 0)
	require.NoError(f.t, err)

	refs := make([]*proto.Ref, n)
	for i := range refs {
		seed := make([]byte, 16)
		binary.BigEndian.PutUint64(seed, uint64(setID))
		binary.BigEndian.PutUint64(seed[8:], uint64(i))
		hash := sha256.Sum256(seed)
		refs[i] = &proto.Ref{Hash: hash[:]}
	}

	at := f.epoch

	require.NoError(f.t, inBatches(refs, func(batch []*proto.Ref) error {
		builders := make([]*ent.CommitRowCreate, len(batch))
		for i, ref := range batch {
			at = at.Add(time.Microsecond)
			builders[i] = f.x.client.CommitRow.Create().SetRef(ref.Hash).SetTimestamp(at).SetReceivedAt(at).SetTree(ref.Hash).SetSetID(setID)
			if tombstoned {
				builders[i].SetTombstonedAt(f.epoch)
			}
		}

		return f.x.client.CommitRow.CreateBulk(builders...).Exec(f.ctx)
	}))

	return refs
}

func TestASetWithMoreCommitsThanOneStatementBindsKeepsWorking(t *testing.T) {
	f := newFixture(t)

	f.commit("s", f.tree(f.file("a", "1")), false)
	require.NoError(t, f.x.SetPolicy(f.ctx, "s", &retention.KeepAll))

	live := f.rows("s", pastBindLimit, false)

	f.advance(time.Hour)
	f.commit("s", f.tree(f.file("a", "2")), false)

	_, err := f.x.RetireCommits(f.ctx, f.clock)
	require.NoError(t, err)

	_, err = f.x.PinsOf(f.ctx, live...)
	require.NoError(t, err)

	retired, err := f.x.RetiredCommits(f.ctx, live)
	require.NoError(t, err)
	require.Len(t, retired, len(live))

	gone := f.rows("t", pastBindLimit, true)

	tombstoned, err := f.x.TombstonedCommits(f.ctx, "t", time.Time{})
	require.NoError(t, err)
	require.Len(t, tombstoned, len(gone))

	unretired, err := f.x.UnretireCommits(f.ctx, tombstoned, true)
	require.NoError(t, err)
	require.Len(t, unretired, len(gone))
}
