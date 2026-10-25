package pack

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// served is a local archive storage that also hands out URLs into an HTTP
// server over the same directory, which is what a bucket does.
type served struct {
	*localArchiveStorage
	url string
}

func (s served) SignRange(_ context.Context, name string, offset, length int64, ttl time.Duration) (*proto.Location, error) {
	return &proto.Location{
		Url:     s.url + "/" + name,
		Header:  map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)},
		Length:  length,
		Expires: timestamppb.New(time.Now().Add(ttl)),
	}, nil
}

// fetch returns the bytes a location addresses.
func fetch(t *testing.T, location *proto.Location) []byte {
	t.Helper()

	request, err := http.NewRequest(http.MethodGet, location.GetUrl(), nil)
	require.NoError(t, err)

	for name, value := range location.GetHeader() {
		request.Header.Set(name, value)
	}

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()

	require.Equal(t, http.StatusPartialContent, response.StatusCode)

	record, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Len(t, record, int(location.GetLength()))

	return record
}

// follow fetches a location the way a caller would.
func follow(t *testing.T, location *proto.Location) *proto.Object {
	t.Helper()

	object, err := DecodeRecord(fetch(t, location), location.GetAtRestKey())
	require.NoError(t, err)

	return object
}

// locatable returns a pack store whose archives can be fetched over HTTP,
// and a settle that closes it and hands back a reopened one, so the
// archives under test are committed rather than still being written.
func locatable(t *testing.T, extra ...PackOption) (*PackStorage, func() *PackStorage) {
	t.Helper()

	base := t.TempDir()
	server := httptest.NewServer(http.FileServer(http.Dir(base)))
	t.Cleanup(server.Close)

	options := append([]PackOption{
		WithArchiveStorage(served{localArchiveStorage: newLocal(base), url: server.URL}),
		WithArchiveIndex(NewInMemoryIndex()),
	}, extra...)

	store, err := NewPackStorage(options...)
	require.NoError(t, err)
	require.NoError(t, store.Open())

	current := store

	return store, func() *PackStorage {
		require.NoError(t, current.Close())

		reopened, err := NewPackStorage(options...)
		require.NoError(t, err)
		require.NoError(t, reopened.Open())
		t.Cleanup(func() { _ = reopened.Close() })
		current = reopened

		return reopened
	}
}

func TestARunCarriesTheKeyToItsArchiveOnly(t *testing.T) {
	ctx := context.Background()
	key := atRestKey(t)
	store, settle := locatable(t, WithAtRestKey(key))

	first := proto.NewObject(&proto.Blob{Data: []byte("first archive")})
	require.NoError(t, store.Put(ctx, first))
	store = settle()

	second := proto.NewObject(&proto.Blob{Data: []byte("second archive")})
	require.NoError(t, store.Put(ctx, second))
	store = settle()

	located := func(ref *proto.Ref) *proto.Location {
		runs, err := store.LocateRecords(ctx, []*proto.Ref{ref})
		require.NoError(t, err)
		require.Len(t, runs, 1)

		return runs[0].GetLocation()
	}

	one, two := located(first.Ref()), located(second.Ref())
	require.NotEmpty(t, one.GetAtRestKey(), "a record sealed at rest is handed out with its archive's key")

	record := fetch(t, one)
	require.NotContains(t, string(record), "first archive", "the bucket holds the record sealed")
	require.True(t, follow(t, one).Ref().Equal(first.Ref()))
	require.True(t, follow(t, two).Ref().Equal(second.Ref()))

	_, err := DecodeRecord(fetch(t, two), one.GetAtRestKey())
	require.Error(t, err, "one archive's key does not open another")

	_, err = DecodeRecord(record, nil)
	require.ErrorIs(t, err, ErrAtRestKeyMissing)

	// the store's own key is not what a location carries
	var whole bytes.Buffer
	require.NoError(t, insecurecleartextkeyset.Write(key.handle, keyset.NewBinaryWriter(&whole)))
	require.NotEqual(t, whole.Bytes(), one.GetAtRestKey())
}

func TestLocateRecordsRunsNeighbouringRecordsTogether(t *testing.T) {
	ctx := context.Background()
	store, settle := locatable(t, WithAtRestKey(atRestKey(t)))

	var blobs []*proto.Object
	for _, content := range []string{"one", "two", "three"} {
		blob := proto.NewObject(&proto.Blob{Data: []byte(content)})
		require.NoError(t, store.Put(ctx, blob))
		blobs = append(blobs, blob)
	}

	file := proto.NewObject(&proto.File{Inline: []byte("any object")})
	require.NoError(t, store.Put(ctx, file))

	store = settle()

	missing := proto.NewObject(&proto.Blob{Data: []byte("never stored")})
	refs := []*proto.Ref{blobs[0].Ref(), blobs[1].Ref(), blobs[2].Ref(), file.Ref(), missing.Ref(), blobs[1].Ref()}

	runs, err := store.LocateRecords(ctx, refs)
	require.NoError(t, err)
	require.Len(t, runs, 1, "records side by side in one archive are one run")
	require.NotEmpty(t, runs[0].GetLocation().GetAtRestKey())

	body := fetch(t, runs[0].GetLocation())

	var located []int
	for _, record := range runs[0].GetRecords() {
		located = append(located, int(record.GetIndex()))

		object, err := DecodeRecord(body[record.GetOffset():record.GetOffset()+record.GetLength()], runs[0].GetLocation().GetAtRestKey())
		require.NoError(t, err)
		require.True(t, object.Ref().Equal(refs[record.GetIndex()]), "each record is the ref it names")
	}

	require.ElementsMatch(t, []int{0, 1, 2, 5}, located, "metadata and an unknown ref are left to an ordinary read")
}

func TestARunEndsAtAGapOrItsSpan(t *testing.T) {
	at := func(offsets ...uint32) []locatedRecord {
		records := make([]locatedRecord, len(offsets))
		for i, offset := range offsets {
			records[i] = locatedRecord{index: i, rec: &IndexRecord{Offset: offset, Length: 100}}
		}

		return records
	}

	end, span := runOf(at(0, 100, 100, 250), 0, runGap)
	require.Equal(t, 4, end, "a repeat and a small gap stay in the run")
	require.EqualValues(t, 350, span)

	end, _ = runOf(at(0, 100, 200+runGap+1), 0, runGap)
	require.Equal(t, 2, end, "a wide gap ends it")

	whole := make([]locatedRecord, runSpan/(1<<20)+1)
	for i := range whole {
		whole[i] = locatedRecord{index: i, rec: &IndexRecord{Offset: uint32(i << 20), Length: 1 << 20}}
	}

	end, span = runOf(whole, 0, runGap)
	require.Equal(t, len(whole)-1, end, "so does its span")
	require.EqualValues(t, runSpan, span)
}

func TestARunCoversNoRecordThatWasNotAskedFor(t *testing.T) {
	ctx := context.Background()
	store, settle := locatable(t, WithAtRestKey(atRestKey(t)))

	var objects []*proto.Object
	for _, object := range []*proto.Object{
		proto.NewObject(&proto.Blob{Data: []byte("mine")}),
		proto.NewObject(&proto.File{Inline: []byte("my file")}),
		proto.NewObject(&proto.Blob{Data: []byte("mine too")}),
		proto.NewObject(&proto.Blob{Data: []byte("theirs")}),
		proto.NewObject(&proto.Blob{Data: []byte("mine as well")}),
	} {
		require.NoError(t, store.Put(ctx, object))
		objects = append(objects, object)
	}

	store = settle()

	refs := []*proto.Ref{objects[0].Ref(), objects[1].Ref(), objects[2].Ref(), objects[4].Ref()}
	runs, err := store.LocateRecords(ctx, refs)
	require.NoError(t, err)
	require.Len(t, runs, 2, "a record between two asked for splits them")

	for _, run := range runs {
		var located []*proto.Ref

		body := fetch(t, run.GetLocation())
		for _, record := range run.GetRecords() {
			object, err := DecodeRecord(body[record.GetOffset():record.GetOffset()+record.GetLength()], run.GetLocation().GetAtRestKey())
			require.NoError(t, err)
			located = append(located, object.Ref())
		}

		switch len(located) {
		case 2:
			require.True(t, located[0].Equal(objects[0].Ref()) && located[1].Equal(objects[2].Ref()), "a file asked for joins the blobs around it")
		case 1:
			require.True(t, located[0].Equal(objects[4].Ref()))
			require.EqualValues(t, run.GetRecords()[0].GetLength(), run.GetLocation().GetLength(), "the range is the one record")
		default:
			t.Fatalf("a run of %d records", len(located))
		}
	}
}

func TestARecordWhoseHeaderOverrunsItIsRefused(t *testing.T) {
	for _, size := range []uint64{math.MaxUint64, math.MaxUint64 - 5, 17} {
		record := append(protowire.AppendVarint(nil, size), make([]byte, 16)...)

		_, err := DecodeRecord(record, nil)
		require.Error(t, err, "a header of %d bytes", size)
	}
}

func TestReadingMoreThanAStreamHoldsFails(t *testing.T) {
	for _, n := range []uint64{math.MaxUint64, 1 << 40, exactAhead + 1} {
		_, err := readExactly(bytes.NewReader(make([]byte, 100)), n)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, "%d bytes", n)
	}

	read, err := readExactly(bytes.NewReader(make([]byte, exactAhead+10)), exactAhead+10)
	require.NoError(t, err)
	require.Len(t, read, exactAhead+10)
}
