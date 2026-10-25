package storage_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/testing/tests3"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	"google.golang.org/api/option"
)

// archive is the whole of one stored file, of which a location addresses
// a range.
var archive = []byte("0123456789abcdefghijklmnopqrstuvwxyz")

// signing is a bucket store over a real object store, holding one archive.
func signing(t *testing.T, name string) *storage.BucketStore {
	t.Helper()
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, tests3.Start(t, "goback"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	store := storage.NewBucketStore(bucket)

	file, err := store.Create(name)
	require.NoError(t, err)
	_, err = file.Write(archive)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	return store
}

// fetch follows a location, optionally overriding the headers it says to
// send, and returns the status and body.
func fetch(t *testing.T, url string, header map[string]string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	for name, value := range header {
		req.Header.Set(name, value)
	}

	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	return response.StatusCode, body
}

func TestASignedRangeReadsThoseBytesFromTheObjectStore(t *testing.T) {
	store := signing(t, "one.pack")

	location, err := store.SignRange(context.Background(), "one.pack", 10, 6, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 6, location.GetLength())
	require.WithinDuration(t, time.Now().Add(time.Minute), location.GetExpires().AsTime(), time.Minute)

	code, body := fetch(t, location.GetUrl(), location.GetHeader())
	require.Equal(t, http.StatusPartialContent, code)
	require.Equal(t, archive[10:16], body)
}

func TestASignedRangeCannotBeTurnedOnTheRestOfTheArchive(t *testing.T) {
	store := signing(t, "one.pack")

	location, err := store.SignRange(context.Background(), "one.pack", 10, 6, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "bytes=10-15", location.GetHeader()["Range"], "the range the caller must repeat")

	// the store refuses a request the signature does not cover; which of
	// the 4xx it answers with is its own business
	code, body := fetch(t, location.GetUrl(), nil)
	require.GreaterOrEqual(t, code, 400, "dropping the range reached %q", body)
	require.NotContains(t, string(body), string(archive), "the whole archive came back")

	code, body = fetch(t, location.GetUrl(), map[string]string{"Range": "bytes=0-35"})
	require.GreaterOrEqual(t, code, 400, "a widened range reached %q", body)
	require.NotContains(t, string(body), string(archive), "the whole archive came back")
}

func TestAnEmptyRangeIsNotSigned(t *testing.T) {
	store := signing(t, "one.pack")

	_, err := store.SignRange(context.Background(), "one.pack", 0, 0, time.Minute)
	require.ErrorIs(t, err, pack.ErrNoSignedURL)
}

func TestAStoreWithNoBytesToAddressSaysSo(t *testing.T) {
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, "mem://")
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	_, err = storage.NewBucketStore(bucket).SignRange(ctx, "one.pack", 0, 6, time.Minute)
	require.ErrorIs(t, err, pack.ErrNoSignedURL, "an in-memory bucket hands out no URLs")
}

// signedHeaders is the set of headers the V4 signature of a location's URL
// covers, lowercased.
func signedHeaders(t *testing.T, location *proto.Location) []string {
	t.Helper()

	u, err := url.Parse(location.GetUrl())
	require.NoError(t, err)

	names := u.Query().Get("X-Goog-SignedHeaders")
	if names == "" {
		names = u.Query().Get("X-Amz-SignedHeaders")
	}

	return strings.Split(names, ";")
}

func TestACloudStorageRangeSignsTheRange(t *testing.T) {
	ctx := context.Background()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	client, err := gcs.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	bucket, err := gcsblob.OpenBucket(ctx, nil, "bucket", &gcsblob.Options{
		Client:         client,
		GoogleAccessID: "signer@example.iam.gserviceaccount.com",
		PrivateKey:     pemKey,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	location, err := storage.SignArchiveRange(ctx, bucket, storage.SignedRange{
		Name: "one.pack", Offset: 10, Length: 6, TTL: time.Minute,
		Header: map[string]string{"x-goog-custom-audit-goback": "t1"},
	})
	require.NoError(t, err)
	require.Subset(t, signedHeaders(t, location), []string{"range", "x-goog-custom-audit-goback"})
}

func TestAnS3RangeSignsTheRange(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "signer")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")

	bucket, err := blob.OpenBucket(context.Background(), "s3://goback?endpoint=http://127.0.0.1:1&region=us-east-1&use_path_style=true&disable_https=true")
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	location, err := storage.NewBucketStore(bucket).SignRange(context.Background(), "one.pack", 10, 6, time.Minute)
	require.NoError(t, err)
	require.Contains(t, signedHeaders(t, location), "range")
}
