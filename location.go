package goback

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// driver opens the store a location of its scheme names, and its index.
type driver func(ctx context.Context, u *url.URL, s Settings) (backup.ObjectStore, backup.Index, error)

var drivers = map[string]driver{
	"":                openDirectory,
	"gcs":             openBucket("gs"),
	"gs":              openBucket("gs"),
	"s3":              openBucket("s3"),
	"goback":          openRemote,
	"goback+insecure": openRemote,
}

// parseLocation reads a storage or index location. A path that starts
// with a volume, such as e:/backups, is a path, not a URL with scheme e.
func parseLocation(raw string) (*url.URL, error) {
	if filepath.VolumeName(raw) != "" {
		return &url.URL{Path: filepath.ToSlash(raw)}, nil
	}

	return url.Parse(raw)
}

// localDir is the absolute directory a location without a scheme names,
// created when missing.
func localDir(u *url.URL) (string, error) {
	abs, err := filepath.Abs(u.Host + u.Path)
	if err != nil {
		return "", err
	}

	return abs, os.MkdirAll(abs, 0o700)
}

func openDirectory(_ context.Context, u *url.URL, s Settings) (backup.ObjectStore, backup.Index, error) {
	dir, err := localDir(u)
	if err != nil {
		return nil, nil, err
	}

	file, err := fileblob.OpenBucket(dir, &fileblob.Options{NoTempDir: true})
	if err != nil {
		return nil, nil, err
	}

	options, err := s.packOptions()
	if err != nil {
		return nil, nil, err
	}

	return s.withIndex(func(x *sql.Index) (*pack.PackStorage, error) {
		return pack.NewPackStorage(append(options,
			pack.WithArchiveStorage(storage.NewBucketStore(file, storage.WithSync(syncUnder(dir)))),
			pack.WithArchiveIndex(x),
			pack.WithOwned(x),
			pack.WithMaxParallel(1),
			pack.WithMaxSize(1024*1024*1024),
			pack.WithSessionLease(10*time.Minute),
			pack.WithCompaction(pack.CompactionConfig{MinimumCandidates: 100}),
		)...)
	})
}

// syncUnder syncs the file a directory bucket keeps under key, and the
// directory holding it, which Windows makes durable without being asked.
func syncUnder(dir string) func(key string) error {
	return func(key string) error {
		path := filepath.Join(dir, filepath.FromSlash(key))

		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return err
		}

		err = file.Sync()
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}

		if err != nil || runtime.GOOS == "windows" {
			return err
		}

		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		defer parent.Close()

		return parent.Sync()
	}
}

// openBucket opens a Cloud Storage or S3-compatible bucket. Every query
// parameter (prefix, endpoint, region, ...) goes to the bucket; the index
// is Settings.Index and the metadata cache sits under Settings.CacheDir.
func openBucket(scheme string) driver {
	return func(ctx context.Context, u *url.URL, s Settings) (backup.ObjectStore, backup.Index, error) {
		query := u.Query()
		if query.Has("index") {
			return nil, nil, errors.New("a bucket url takes no index; the index location is the store's index")
		}

		if query.Has("cache") {
			return nil, nil, errors.New("a bucket url takes no cache; the cache directory names where caches go")
		}

		bucket, err := blob.OpenBucket(ctx, (&url.URL{Scheme: scheme, Host: u.Host, RawQuery: query.Encode()}).String())
		if err != nil {
			return nil, nil, err
		}

		options, err := s.packOptions()
		if err != nil {
			return nil, nil, err
		}

		limit := s.MetadataCacheSize
		switch {
		case limit == 0:
			limit = DefaultMetadataCacheSize
		case limit < 0:
			limit = 0
		}

		return s.withIndex(func(x *sql.Index) (*pack.PackStorage, error) {
			return storage.NewBucketObjectStore(bucket, x, s.storeCache("metadata"), limit, append(options, pack.WithOwned(x))...)
		})
	}
}

// withIndex opens the Settings.Index location as the archive index of the
// pack store build makes. The store closes it.
func (s Settings) withIndex(build func(*sql.Index) (*pack.PackStorage, error)) (backup.ObjectStore, backup.Index, error) {
	if s.Index == "" {
		return nil, nil, errors.New("no index location: where this store's index lives, a directory for SQLite or a postgres:// url")
	}

	u, err := parseLocation(s.Index)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid index location %s: %w", s.Index, err)
	}

	location := u.String()

	switch u.Scheme {
	case "", "file", "sqlite":
		if location, err = localDir(u); err != nil {
			return nil, nil, err
		}
	}

	s.logger().Info("opening the index", "location", location)

	// a local index keeps every commit until a policy is set
	var opts []sql.Option
	if s.Behind != nil {
		opts = append(opts, sql.WithBehind(s.Behind))
	}

	x := sql.New(location, nil, opts...)
	x.DefaultPolicy = &retention.KeepAll

	packs, err := build(x)
	if err != nil {
		return nil, nil, err
	}

	x.ObjectStore = packs

	if err := x.Open(); err != nil {
		return nil, nil, fmt.Errorf("opening the index: %w", err)
	}

	return packs, x, nil
}

// packOptions is the pack options for Settings.AtRestKey and
// Settings.Behind: a store in front of another is served by nothing else.
func (s Settings) packOptions() ([]pack.PackOption, error) {
	var options []pack.PackOption
	if s.Behind != nil {
		options = append(options, pack.WithLocalSessions())
	}

	if s.AtRestKey == "" {
		return options, nil
	}

	key, err := pack.LoadAtRestKey(s.AtRestKey)
	if err != nil {
		return nil, fmt.Errorf("loading the at-rest key: %w", err)
	}

	return append(options, pack.WithAtRestKey(key)), nil
}

// insecureScheme names a store server reached without TLS, which puts the
// api key and every object on the wire in the clear. It is for a store on
// the same machine; anywhere else, goback:// is the scheme.
const insecureScheme = "goback+insecure"

// remoteParams are the query parameters a goback:// URL may carry.
var remoteParams = map[string]string{
	"ca":       "a PEM certificate authority the store server must present",
	"key-file": "a file holding the api key, for when the url itself would show it",
}

// remoteAddress is the address a goback:// URL dials: without a port,
// goback:// dials 443 and goback+insecure:// the store server's own port.
func remoteAddress(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == insecureScheme {
			port = "6060"
		}
	}

	return net.JoinHostPort(u.Hostname(), port)
}

// checkRemoteParams rejects a query parameter a goback:// URL does not
// carry, so a misspelled one is a mistake rather than a silent default.
func checkRemoteParams(u *url.URL) error {
	for name := range u.Query() {
		if _, ok := remoteParams[name]; !ok {
			known := make([]string, 0, len(remoteParams))
			for param, what := range remoteParams {
				known = append(known, param+"= ("+what+")")
			}

			sort.Strings(known)

			return fmt.Errorf("%q is not something a %s:// url carries; it takes %s", name, u.Scheme, strings.Join(known, " and "))
		}
	}

	return nil
}

// remoteKey is the api key the URL carries in front of the host, else the
// contents of the file its key-file parameter names.
func remoteKey(u *url.URL) (string, error) {
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			return "", errors.New("a goback:// url carries the api key alone, with no password after it")
		}

		if key := u.User.Username(); key != "" {
			return key, nil
		}
	}

	path := u.Query().Get("key-file")
	if path == "" {
		return "", fmt.Errorf("a %s:// store needs an api key, in front of the host or in the file its key-file parameter names", u.Scheme)
	}

	key, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading the api key: %w", err)
	}

	return strings.TrimSpace(string(key)), nil
}

// openRemote dials a store server, which is its own index.
func openRemote(_ context.Context, u *url.URL, s Settings) (backup.ObjectStore, backup.Index, error) {
	if err := checkRemoteParams(u); err != nil {
		return nil, nil, err
	}

	addr := remoteAddress(u)

	key, err := remoteKey(u)
	if err != nil {
		return nil, nil, err
	}

	ca := u.Query().Get("ca")
	creds := auth.Credentials{Secret: key, AgentID: s.AgentID}

	var client *storage.Client

	if u.Scheme == insecureScheme {
		if ca != "" {
			return nil, nil, fmt.Errorf("%s:// presents no certificate to check against %s", u.Scheme, ca)
		}

		s.logger().Warn("talking to the store server without TLS; the api key and everything uploaded is readable on the way", "address", addr)
		creds.Plaintext = true

		client, err = storage.NewPlaintextClient(addr, creds)
	} else {
		tlsConfig, tlsErr := storage.ClientTLS(ca)
		if tlsErr != nil {
			return nil, nil, tlsErr
		}

		client, err = storage.NewClient(addr, creds, tlsConfig)
	}

	if err != nil {
		return nil, nil, err
	}

	return client, client, nil
}
