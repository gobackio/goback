package pack

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// rawRecord is a record as an archive stores it, with a header claiming
// size payload bytes followed by payload.
func rawRecord(size uint64, payload []byte) []byte {
	hdr := proto.Bytes(&proto.ObjectHeader{Ref: makeRef(), Type: proto.ObjectType_BLOB, Size: size})

	return append(append(proto.EncodeVarint(nil, uint64(len(hdr))), hdr...), payload...)
}

// walkRaw walks archive bytes holding records as foreach does, and counts
// the records it reached.
func walkRaw(records []byte, load loadPredicate) (int, error) {
	var n int
	err := (&archive{}).foreachReader(bytes.NewReader(append(archiveHeader(), records...)), load, func(*proto.ObjectHeader, []byte, uint32, uint32) error {
		n++
		return nil
	})

	return n, err
}

func TestAWalkRefusesRecordsTheirBytesCannotHold(t *testing.T) {
	whole := append(rawRecord(4, []byte("abcd")), rawRecord(3, []byte("efg"))...)

	for _, load := range []loadPredicate{loadAll, loadNone} {
		n, err := walkRaw(whole, load)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		_, err = walkRaw(whole[:len(whole)-2], load)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, "a payload cut short")

		_, err = walkRaw(append(bytes.Clone(whole), 1, 2, 3), load)
		require.Error(t, err, "bytes too few for a record")

		_, err = walkRaw(rawRecord(1<<30, []byte("abcd")), load)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, "a size past the end")

		_, err = walkRaw(rawRecord(1<<40, []byte("abcd")), load)
		require.Error(t, err, "a size no archive holds")

		_, err = walkRaw(bytes.Repeat([]byte{0xff}, 16), load)
		require.Error(t, err, "a malformed header size")
	}
}

func TestAGetOfARecordWithACorruptHeaderSizeFails(t *testing.T) {
	ctx := context.Background()

	for name, corrupt := range map[string]func([]byte){
		"continued": func(record []byte) { record[0] |= 0x80 },
		"malformed": func(record []byte) { copy(record, bytes.Repeat([]byte{0xff}, 11)) },
	} {
		base := t.TempDir()

		store := newTestStore(t, base)
		objects := makeTestData(t, 3)
		for _, obj := range objects {
			require.NoError(t, store.Put(ctx, obj))
		}
		require.NoError(t, store.Close())

		store = newTestStore(t, base)
		loc, err := store.index.LocateObject(objects[1].Ref(), Scope{})
		require.NoError(t, err)
		require.NoError(t, store.Close())

		file := filepath.Join(base, loc.Archive+ArchiveSuffix)
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		corrupt(data[loc.Record.Offset:])
		require.NoError(t, os.WriteFile(file, data, 0644))

		store = newTestStore(t, base)
		_, err = store.Get(ctx, objects[1].Ref())
		require.Error(t, err, name)
		require.NoError(t, store.Close())
	}
}
