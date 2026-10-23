package storage_test

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

// fakeGCS answers the Cloud Storage JSON API's single-request upload and
// object metadata read, which is all a small write and its HEAD send.
type fakeGCS struct {
	mtx     sync.Mutex
	objects map[string]fakeObject
	last    time.Time
}

type fakeObject struct {
	data    []byte
	updated time.Time
}

func (f *fakeGCS) resource(bucket, name string, obj fakeObject) map[string]string {
	sum := md5.Sum(obj.data)
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.Checksum(obj.data, crc32.MakeTable(crc32.Castagnoli)))

	return map[string]string{
		"kind":        "storage#object",
		"bucket":      bucket,
		"name":        name,
		"size":        strconv.Itoa(len(obj.data)),
		"md5Hash":     base64.StdEncoding.EncodeToString(sum[:]),
		"crc32c":      base64.StdEncoding.EncodeToString(crc),
		"generation":  strconv.FormatInt(obj.updated.UnixMicro(), 10),
		"timeCreated": obj.updated.Format(time.RFC3339Nano),
		"updated":     obj.updated.Format(time.RFC3339Nano),
	}
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/"):
		bucket := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/upload/storage/v1/b/"), "/o")
		f.upload(w, r, bucket)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/b/"):
		bucket, name, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/storage/v1/b/"), "/o/")

		f.mtx.Lock()
		obj, ok := f.objects[name]
		f.mtx.Unlock()

		if !ok {
			http.Error(w, `{"error":{"code":404,"message":"not found"}}`, http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(f.resource(bucket, name, obj))
	default:
		http.Error(w, "not faked", http.StatusNotImplemented)
	}
}

func (f *fakeGCS) upload(w http.ResponseWriter, r *http.Request, bucket string) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || r.URL.Query().Get("uploadType") != "multipart" {
		http.Error(w, "only single-request uploads are faked", http.StatusNotImplemented)
		return
	}

	parts := multipart.NewReader(r.Body, params["boundary"])

	var meta struct {
		Name string `json:"name"`
	}

	part, err := parts.NextPart()
	if err == nil {
		err = json.NewDecoder(part).Decode(&meta)
	}

	var data []byte
	if err == nil {
		part, err = parts.NextPart()
	}

	if err == nil {
		data, err = io.ReadAll(part)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mtx.Lock()
	updated := time.Now().UTC().Truncate(time.Millisecond)
	if !updated.After(f.last) {
		updated = f.last.Add(time.Millisecond)
	}
	f.last = updated

	obj := fakeObject{data: data, updated: updated}
	f.objects[meta.Name] = obj
	f.mtx.Unlock()

	_ = json.NewEncoder(w).Encode(f.resource(bucket, meta.Name, obj))
}

func openFakeGCS(t *testing.T) *blob.Bucket {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := httptest.NewUnstartedServer(&fakeGCS{objects: map[string]fakeObject{}})
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)

	client, err := gcs.NewClient(context.Background(),
		option.WithEndpoint(server.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)

	bucket, err := gcsblob.OpenBucket(context.Background(), nil, "bucket", &gcsblob.Options{Client: client})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	return bucket
}

func TestAnUploadToCloudStorageTellsWhatItStoredWithoutAskingAgain(t *testing.T) {
	bucket := openFakeGCS(t)

	var stored pack.File

	requests, _ := countedOn(t, "gcs", bucket, func(store *storage.BucketStore) {
		file, err := store.Create("a.idx")
		require.NoError(t, err)

		_, err = file.Write([]byte("hello"))
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stored = file
	})

	require.Zero(t, requests["head"], "the upload's answer says what the bucket stored")

	info, sum, ok := stored.(pack.StoredFile).Stored()
	require.True(t, ok)

	want := md5.Sum([]byte("hello"))
	require.Equal(t, want[:], sum)
	require.Equal(t, int64(5), info.Size())

	opened, err := storage.NewBucketStore(bucket).Open("a.idx")
	require.NoError(t, err)

	attrs, err := opened.Stat()
	require.NoError(t, err)
	require.True(t, attrs.ModTime().Equal(info.ModTime()), "as an Open of the file says")
	require.False(t, info.ModTime().IsZero())
}

func TestAnUploadElsewhereLeavesWhatItStoredToBeAsked(t *testing.T) {
	_, _ = counted(t, "mem-stored", func(store *storage.BucketStore) {
		file, err := store.Create("a.idx")
		require.NoError(t, err)
		require.NoError(t, file.Close())

		_, _, ok := file.(pack.StoredFile).Stored()
		require.False(t, ok)
	})
}
