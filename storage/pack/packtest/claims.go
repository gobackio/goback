package packtest

import (
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ClaimingIndex is an archive index that also takes claims on open archives.
type ClaimingIndex interface {
	pack.ArchiveIndex
	pack.ClaimIndex
}

// TestClaimIndex checks open archives: their claims, finalizing,
// abandoning, and what a commit or an abort does with them.
func TestClaimIndex(t *testing.T, idx ClaimingIndex) {
	session := &backup.Session{ID: uuid.New().String(), AgentID: "a", Set: "world", Started: time.Now(), LastSeen: time.Now()}
	other := &backup.Session{ID: uuid.New().String(), AgentID: "b", Set: "world", Started: time.Now(), LastSeen: time.Now()}

	open := RandomArchive(20)
	require.ErrorIs(t, idx.OpenArchive(open.name, session.ID), backup.ErrNoSession, "an open archive needs a live session")

	require.NoError(t, idx.BeginSession(session))
	require.NoError(t, idx.BeginSession(other))

	require.NoError(t, idx.OpenArchive(open.name, session.ID))
	require.NoError(t, idx.AddObjects(open.name, open.index[:10]))
	require.NoError(t, idx.AddObjects(open.name, open.index[10:]))

	ref := &proto.Ref{Hash: open.index[0].Sum[:]}
	elsewhere := &proto.Ref{Hash: RandomIndexFile(1)[0].Sum[:]}

	held, err := idx.Holds([]*proto.Ref{elsewhere, ref}, session.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{string(ref.Hash): true}, held, "the session holds what its open archive has")

	held, err = idx.Holds([]*proto.Ref{ref}, other.ID)
	require.NoError(t, err)
	require.Empty(t, held, "another session does not")

	_, err = idx.LocateObject(ref, pack.Scope{Session: session.ID})
	require.ErrorIs(t, err, pack.ErrRecordNotFound, "an open archive cannot be read from yet")

	total, _, err := idx.CountObjects()
	require.NoError(t, err)
	require.Zero(t, total, "nor counted")

	claims, err := idx.Claims(session.ID)
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, open.name, claims[0].Archive)
	require.Less(t, claims[0].Age, time.Hour)

	claims, err = idx.Claims(other.ID)
	require.NoError(t, err)
	require.Empty(t, claims)

	created := time.Unix(1_790_000_000, 456_000)

	require.ErrorIs(t, idx.FinalizeArchive(open.name, 0, created), pack.ErrClaimLapsed, "past its claim an archive cannot be finalized")
	require.NoError(t, idx.FinalizeArchive(open.name, time.Hour, created))
	require.ErrorIs(t, idx.AddObjects(open.name, RandomIndexFile(1)), pack.ErrClaimLapsed, "a finalized archive takes no more rows")

	info, known, err := idx.LookupArchive(open.name)
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, pack.ArchivePending, info.State)
	require.True(t, created.Equal(info.Created), "finalizing records when the archive was created")

	_, err = idx.LocateObject(ref, pack.Scope{Session: session.ID})
	require.NoError(t, err, "once finalized the session reads its archive")

	claims, err = idx.Claims(session.ID)
	require.NoError(t, err)
	require.Empty(t, claims)

	stray := RandomArchive(5)
	require.NoError(t, idx.OpenArchive(stray.name, session.ID))
	refused := pack.IndexRecord{Sum: RandomIndexFile(1)[0].Sum, Type: uint32(proto.ObjectType_COMMIT)}
	untomb := pack.IndexRecord{Type: uint32(proto.ObjectType_TOMBSTONE)}
	copy(untomb.Sum[:], proto.TombstoneRef(proto.TombstoneRef(&proto.Ref{Hash: refused.Sum[:]})).Hash)
	require.NoError(t, idx.AddObjects(stray.name, append(stray.index, open.index[0], refused, untomb)))

	abandoned, err := idx.Abandon(stray.name, time.Hour)
	require.NoError(t, err)
	require.False(t, abandoned, "a claim within its limit holds")

	abandoned, err = idx.Abandon(stray.name, 0)
	require.NoError(t, err)
	require.True(t, abandoned)

	abandoned, err = idx.Abandon(stray.name, 0)
	require.NoError(t, err)
	require.False(t, abandoned, "only an open archive is abandoned")

	require.ErrorIs(t, idx.AddObjects(stray.name, RandomIndexFile(1)), pack.ErrClaimLapsed)
	require.ErrorIs(t, idx.FinalizeArchive(stray.name, time.Hour, created), pack.ErrClaimLapsed)

	strayRef := &proto.Ref{Hash: stray.index[0].Sum[:]}

	held, err = idx.Holds([]*proto.Ref{strayRef}, session.ID)
	require.NoError(t, err)
	require.Empty(t, held, "a lost archive holds nothing")

	lost, err := idx.Lost(session.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, refs(stray.index), lost, "what the pending archive also has is not lost, nor the refused commit and its un-tombstone")

	lost, err = idx.Lost(other.ID)
	require.NoError(t, err)
	require.Empty(t, lost)

	require.NoError(t, idx.CommitSession(session.ID))

	_, known, err = idx.LookupArchive(stray.name)
	require.NoError(t, err)
	require.False(t, known, "a commit forgets the lost archives")

	info, _, err = idx.LookupArchive(open.name)
	require.NoError(t, err)
	require.Equal(t, pack.ArchiveCommitted, info.State)

	total, _, err = idx.CountObjects()
	require.NoError(t, err)
	require.EqualValues(t, len(open.index), total)

	unfinished := RandomArchive(5)
	require.NoError(t, idx.OpenArchive(unfinished.name, other.ID))
	require.NoError(t, idx.AddObjects(unfinished.name, unfinished.index))

	gone := RandomArchive(5)
	require.NoError(t, idx.OpenArchive(gone.name, other.ID))
	_, err = idx.Abandon(gone.name, 0)
	require.NoError(t, err)

	dropped, err := idx.EndSession(other.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{unfinished.name, gone.name}, dropped, "an abort drops open and lost archives")

	_, known, err = idx.LookupArchive(unfinished.name)
	require.NoError(t, err)
	require.False(t, known)

	require.ErrorIs(t, idx.AddObjects(unfinished.name, RandomIndexFile(1)), backup.ErrNoSession, "the archive's session ended")
	require.ErrorIs(t, idx.FinalizeArchive(unfinished.name, time.Hour, created), backup.ErrNoSession)
}

func refs(index pack.IndexFile) []*proto.Ref {
	out := make([]*proto.Ref, len(index))
	for i, record := range index {
		out[i] = &proto.Ref{Hash: record.Sum[:]}
	}

	return out
}
