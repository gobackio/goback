package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// held is a backup.Batcher over a set of hashes.
type held struct {
	hashes map[string]bool
	asked  []int
}

func (h *held) Missing(_ context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	h.asked = append(h.asked, len(refs))

	var missing []*proto.Ref

	for _, ref := range refs {
		if !h.hashes[string(ref.GetHash())] {
			missing = append(missing, ref)
		}
	}

	return missing, nil
}

func (h *held) PutObjects(_ context.Context, objects []*proto.Object) error {
	for _, obj := range objects {
		h.hashes[string(obj.Ref().GetHash())] = true
	}

	return nil
}

func file(t *testing.T, n int) []byte {
	t.Helper()

	var buf bytes.Buffer

	w, err := NewWriter(&buf)
	require.NoError(t, err)

	for i := range n {
		require.NoError(t, w.Write(proto.NewObject(&proto.Blob{Data: []byte(fmt.Sprint(i))})))
	}

	require.Equal(t, int64(buf.Len()), w.Len())

	return buf.Bytes()
}

func TestIngestStoresAFileOnce(t *testing.T) {
	data := file(t, 5000)
	store := &held{hashes: map[string]bool{}}

	done, err := Ingest(context.Background(), bytes.NewReader(data), int64(len(data)), store)
	require.NoError(t, err)
	require.Equal(t, Ingested{Objects: 5000, Stored: 5000}, done)
	require.Len(t, store.hashes, 5000)

	for _, n := range store.asked {
		require.LessOrEqual(t, n, 4096)
	}

	done, err = Ingest(context.Background(), bytes.NewReader(data), int64(len(data)), store)
	require.NoError(t, err)
	require.Equal(t, Ingested{Objects: 5000, Stored: 0}, done)
}

func TestIngestRefusesAFileThatIsNotWhole(t *testing.T) {
	data := file(t, 10)

	for name, broken := range map[string][]byte{
		"cut short":     data[:len(data)-1],
		"not an upload": append([]byte("goback-upload/0\n"), data[len(magic):]...),
		"empty":         nil,
	} {
		_, err := Ingest(context.Background(), bytes.NewReader(broken), int64(len(broken)), &held{hashes: map[string]bool{}})
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

var errBroken = errors.New("broken connection")

func TestIngestFailsWithWhatBrokeTheRead(t *testing.T) {
	data := file(t, 10)

	for _, at := range []int{3, len(data) - 1} {
		r := io.MultiReader(bytes.NewReader(data[:at]), iotest.ErrReader(errBroken))

		_, err := Ingest(context.Background(), r, int64(len(data)), &held{hashes: map[string]bool{}})
		require.ErrorIs(t, err, errBroken)
		require.NotErrorIs(t, err, ErrInvalid)
	}
}

func TestIngestLeavesTheCommitsAFileHoldsToItsCaller(t *testing.T) {
	part := proto.NewObject(&proto.Blob{Data: []byte("part")})
	first := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: part.Ref(), BackupSet: "world"})
	second := proto.NewObject(&proto.Commit{Timestamp: 2, Tree: part.Ref(), BackupSet: "world"})

	var buf bytes.Buffer

	w, err := NewWriter(&buf)
	require.NoError(t, err)
	require.NoError(t, w.Write(first))
	require.NoError(t, w.Write(part))
	require.NoError(t, w.Write(second))

	h := &held{hashes: map[string]bool{}}
	done, err := Ingest(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), h)
	require.NoError(t, err)
	require.Equal(t, int64(3), done.Objects)
	require.Equal(t, int64(1), done.Stored)
	require.Len(t, done.Commits, 2)
	require.True(t, done.Commits[0].Ref().Equal(first.Ref()))
	require.True(t, done.Commits[1].Ref().Equal(second.Ref()))
}

func TestIngestRefusesAFileWithMoreCommitsThanItHolds(t *testing.T) {
	var buf bytes.Buffer

	w, err := NewWriter(&buf)
	require.NoError(t, err)

	for i := range MaxCommits + 1 {
		require.NoError(t, w.Write(proto.NewObject(&proto.Commit{Timestamp: int64(i + 1), Tree: &proto.Ref{Hash: make([]byte, 32)}, BackupSet: "world"})))
	}

	_, err = Ingest(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), &held{hashes: map[string]bool{}})
	require.ErrorIs(t, err, ErrInvalid)
}

func TestIngestRefusesAFileWhoseCommitsTakeMoreThanItHolds(t *testing.T) {
	var buf bytes.Buffer

	w, err := NewWriter(&buf)
	require.NoError(t, err)

	for i := range 2 {
		large := proto.NewObject(&proto.Commit{Timestamp: int64(i + 1), Tree: &proto.Ref{Hash: make([]byte, 32)}, BackupSet: string(make([]byte, MaxCommitBytes/2+1))})
		require.NoError(t, w.Write(large))
	}

	_, err = Ingest(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), &held{hashes: map[string]bool{}})
	require.ErrorIs(t, err, ErrInvalid)
}
