package goback

import (
	"context"
	"fmt"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
)

func parse(t *testing.T, raw string) *url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	return u
}

func TestADrivePathIsAPath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("only Windows paths start with a volume")
	}

	for _, raw := range []string{`e:\backups\store`, "e:/backups/store"} {
		u, err := parseLocation(raw)
		require.NoError(t, err)
		require.Empty(t, u.Scheme, raw)
		require.Equal(t, "e:/backups/store", u.Path, raw)
	}
}

func TestRemoteAddressKeepsThePortOnlyOnce(t *testing.T) {
	for _, test := range []struct{ raw, want string }{
		{"goback://store.example", "store.example:443"},
		{"goback+insecure://localhost", "localhost:6060"},
		{"goback://store.example:8443", "store.example:8443"},
		{"goback://127.0.0.1:8443", "127.0.0.1:8443"},
		{"goback://[::1]:8443", "[::1]:8443"},
		{"goback://gbk_abc-123_x@store.example:8443", "store.example:8443"},
	} {
		u, err := url.Parse(test.raw)
		require.NoError(t, err)
		require.Equal(t, test.want, remoteAddress(u), test.raw)
	}
}

func TestOnlyThePlaintextSchemeSkipsTLS(t *testing.T) {
	require.Contains(t, drivers, insecureScheme, "the scheme reaches a store server")
}

func TestTheUrlCarriesTheApiKey(t *testing.T) {
	key, err := remoteKey(parse(t, "goback://gbk_abc-123_x@store.example:8443"))
	require.NoError(t, err)
	require.Equal(t, "gbk_abc-123_x", key)
}

func TestAKeyFileKeepsTheSecretOutOfTheUrl(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(path, []byte("gbk_from_the_file\n"), 0o600))

	key, err := remoteKey(parse(t, "goback://store.example?key-file="+url.QueryEscape(path)))
	require.NoError(t, err)
	require.Equal(t, "gbk_from_the_file", key, "trimmed of the trailing newline an editor leaves")
}

func TestAKeyWithAPasswordAfterItIsRefused(t *testing.T) {
	_, err := remoteKey(parse(t, "goback://gbk_abc:secret@store.example"))
	require.ErrorContains(t, err, "no password")
}

func TestAStoreServerWithNoKeyAnywhereSaysWhereToPutOne(t *testing.T) {
	_, err := remoteKey(parse(t, "goback://store.example"))
	require.ErrorContains(t, err, "in front of the host")
	require.ErrorContains(t, err, "key-file")
}

func TestAMisspelledParameterIsNotIgnored(t *testing.T) {
	require.NoError(t, checkRemoteParams(parse(t, "goback://store.example?ca=root.crt&key-file=k")))

	err := checkRemoteParams(parse(t, "goback://store.example?cacert=root.crt"))
	require.ErrorContains(t, err, "cacert")
	require.ErrorContains(t, err, "ca=", "the error names what the url does carry")
	require.ErrorContains(t, err, "key-file=")
}

func TestADirectoryStoreSyncsTheFileItsBucketKeepsUnderAKey(t *testing.T) {
	dir := t.TempDir()

	bucket, err := fileblob.OpenBucket(dir, &fileblob.Options{NoTempDir: true})
	require.NoError(t, err)
	defer bucket.Close()

	key := storage.ArchiveKey("0123.goback")
	require.NoError(t, bucket.WriteAll(context.Background(), key, []byte("archive"), nil))

	require.NoError(t, syncUnder(dir)(key))
}

func TestADirectoryStoreCreatesAFileOnceHoweverManyTryAtOnce(t *testing.T) {
	dir := t.TempDir()

	bucket, err := fileblob.OpenBucket(dir, &fileblob.Options{NoTempDir: true})
	require.NoError(t, err)
	defer bucket.Close()

	store := storage.NewBucketStore(bucket, storage.WithCreateNew(createUnder(dir)))

	for round := range 20 {
		name := fmt.Sprintf("%d.end", round)

		var (
			won   atomic.Int32
			tries sync.WaitGroup
		)

		for i := range 16 {
			tries.Go(func() {
				err := store.CreateNew(name, fmt.Appendf(nil, "writer %d", i))
				if err == nil {
					won.Add(1)
				} else {
					require.ErrorIs(t, err, pack.ErrFileExists)
				}
			})
		}

		tries.Wait()
		require.EqualValues(t, 1, won.Load(), "round %d", round)
	}

	names, err := store.List(".end")
	require.NoError(t, err)
	require.Len(t, names, 20, "nothing written aside is listed")
}

func TestALocationWithoutASchemeIsThePathAsWritten(t *testing.T) {
	for _, raw := range []string{"/media/me/Backup #2/goback", "/srv/q?x/store", "/srv/50%off", "//srv/store", "store #1"} {
		u, err := parseLocation(raw)
		require.NoError(t, err, raw)
		require.Empty(t, u.Scheme, raw)
		require.Empty(t, u.Host, raw)
		require.Equal(t, raw, u.Path)
	}

	u, err := parseLocation("goback://gbk_key@backup.example:9000")
	require.NoError(t, err)
	require.Equal(t, "goback", u.Scheme)
	require.Equal(t, "backup.example:9000", u.Host)

	_, err = parseLocation("sqlite:///var/lib/goback#2")
	require.Error(t, err, "a url's fragment would be dropped from the path")
}

func TestARedactedLocationLeavesTheKeyOut(t *testing.T) {
	for raw, want := range map[string]string{
		"goback://gbk_live_xyz@backup.example":           "goback://xxxxx@backup.example",
		"gobak://gbk_live_xyz@backup.example:abc/x?y=1":  "gobak://xxxxx@backup.example:abc/x?y=1",
		"postgres://goback:s3cret@db.example/goback":     "postgres://xxxxx@db.example/goback",
		"gs://bucket/prefix":                             "gs://bucket/prefix",
		"/media/me/user@host/store":                      "/media/me/user@host/store",
		"goback://backup.example?key-file=/etc/key@home": "goback://backup.example?key-file=/etc/key@home",
	} {
		require.Equal(t, want, Redacted(raw), raw)
	}

	_, err := Open(context.Background(), Settings{Storage: "goback://gbk_live_xyz@backup.example:abc"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "gbk_live_xyz")
}

type unopenable struct {
	backup.ObjectStore
	closed bool
}

func (s *unopenable) Open() error  { return errors.New("listing archives: 503") }
func (s *unopenable) Close() error { s.closed = true; return nil }

type closingIndex struct {
	backup.Index
	closed bool
}

func (x *closingIndex) Close() error { x.closed = true; return nil }

func TestAStoreThatFailsToOpenIsClosedWithItsIndex(t *testing.T) {
	store, index := &unopenable{}, &closingIndex{}

	drivers["unopenable"] = func(context.Context, *url.URL, Settings) (backup.ObjectStore, backup.Index, error) {
		return store, index, nil
	}
	t.Cleanup(func() { delete(drivers, "unopenable") })

	_, err := Open(context.Background(), Settings{Storage: "unopenable://store"})
	require.ErrorContains(t, err, "503")
	require.True(t, store.closed)
	require.True(t, index.closed)
}
