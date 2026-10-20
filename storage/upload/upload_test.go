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

func TestIngestLeavesTheCommitAFileHoldsToItsCaller(t *testing.T) {
	part := proto.NewObject(&proto.Blob{Data: []byte("part")})
	commit := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: part.Ref(), BackupSet: "world"})

	var buf bytes.Buffer

	w, err := NewWriter(&buf)
	require.NoError(t, err)
	require.NoError(t, w.Write(commit))
	require.NoError(t, w.Write(part))

	h := &held{hashes: map[string]bool{}}
	done, err := Ingest(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), h)
	require.NoError(t, err)
	require.Equal(t, int64(2), done.Objects)
	require.Equal(t, int64(1), done.Stored)
	require.True(t, done.Commit.Ref().Equal(commit.Ref()))

	require.NoError(t, w.Write(commit))

	_, err = Ingest(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), &held{hashes: map[string]bool{}})
	require.ErrorIs(t, err, ErrInvalid, "a file holds one commit at most")
}
