package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"

	"github.com/urfave/cli"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// Opener is implemented by stores that must be opened before use.
type Opener interface {
	Open() error
}

// Closer is implemented by stores and indexes that must be closed.
type Closer interface {
	Close() error
}

// CloseStore compacts a store that can and closes it.
func CloseStore(store backup.ObjectStore) {
	if c, ok := store.(interface {
		Compact(context.Context) (*pack.CompactReport, error)
	}); ok {
		if _, err := c.Compact(context.Background()); err != nil {
			log.Printf("Compaction failed: %v", err)
		}
	}

	if cl, ok := store.(Closer); ok {
		if err := cl.Close(); err != nil {
			log.Printf("Closing the store failed: %v", err)
		}
	}
}

// CloseAll closes the store, then the index. A local store keeps its
// archives in the same index, and its last compaction still writes there.
func CloseAll(store backup.ObjectStore, index backup.Index) {
	CloseStore(store)

	if err := index.Close(); err != nil {
		log.Printf("Closing the index failed: %v", err)
	}
}

// StoreSettings name a store and say how to open it, as the global flags
// do. The zero value of a size is invalid; Settings fills in the flags'
// defaults.
type StoreSettings struct {
	Storage    string
	Index      string
	AgentID    string
	StoreKey   string
	Passphrase string
	AtRestKey  string
	CacheDir   string

	MetadataCacheSize string
	BlobCacheSize     string
}

// Settings reads the StoreSettings from the global flags; the agent id
// defaults to the hostname.
func Settings(c *cli.Context) StoreSettings {
	return StoreSettings{
		Storage:           c.GlobalString("storage"),
		Index:             c.GlobalString("index"),
		AgentID:           AgentID(c),
		StoreKey:          c.GlobalString("store-key"),
		Passphrase:        c.GlobalString("passphrase"),
		AtRestKey:         c.GlobalString("at-rest-key"),
		CacheDir:          c.GlobalString("cache-dir"),
		MetadataCacheSize: c.GlobalString("metadata-cache-size"),
		BlobCacheSize:     c.GlobalString("blob-cache-size"),
	}
}

// StoreKey loads the key file named by the global --store-key flag, or
// exits. Without one, a --passphrase opens the escrowed copy the store
// keeps. It returns nil when the store is written in the clear.
func StoreKey(c *cli.Context, store backup.ObjectStore) *storekey.Key {
	key, err := Settings(c).Key(context.Background(), store)
	if err != nil {
		Fatal(err)
	}

	return key
}

// Key loads the StoreKey file. Without one, the Passphrase opens the
// escrowed copy the store keeps. It returns nil when the store is written
// in the clear.
func (s StoreSettings) Key(ctx context.Context, store backup.ObjectStore) (*storekey.Key, error) {
	if s.StoreKey != "" {
		key, err := storekey.Load(s.StoreKey)
		if err != nil {
			return nil, fmt.Errorf("Could not load store key: %w", err)
		}

		return key, nil
	}

	if s.Passphrase == "" {
		return nil, nil
	}

	key, err := escrowedKey(ctx, Unwrap(store), s.Passphrase)
	if err != nil {
		return nil, fmt.Errorf("Could not open the escrowed store key: %w", err)
	}

	return key, nil
}

func escrowedKey(ctx context.Context, store any, passphrase string) (*storekey.Key, error) {
	escrow, ok := store.(interface {
		EscrowedKeys(context.Context) ([]backup.EscrowedKey, error)
	})
	if !ok {
		return nil, fmt.Errorf("store %T keeps no escrowed key", store)
	}

	kept, err := escrow.EscrowedKeys(ctx)
	if err != nil {
		return nil, err
	}

	if len(kept) == 0 {
		return nil, errors.New("the store keeps no escrowed key; upload one with goback key escrow --upload")
	}

	for _, k := range kept {
		key, err := storekey.Recover(k.Escrowed, passphrase)
		if err == nil {
			return key, nil
		}
	}

	return nil, fmt.Errorf("the passphrase opens none of the %d escrowed copies the store keeps", len(kept))
}

// Unwrap peels caching and wrapping stores off until the innermost store.
func Unwrap(store backup.ObjectStore) backup.ObjectStore {
	for inner := wrapped.Unwrap(store); inner != nil; inner = wrapped.Unwrap(store) {
		store = inner
	}

	return store
}

func createFolders(loc string) (string, error) {
	abs, err := filepath.Abs(loc)
	if err != nil {
		return "", err
	}

	err = os.MkdirAll(abs, os.FileMode(0700))
	if err != nil {
		if os.IsExist(err) {
			return abs, err
		}
	}

	return abs, nil
}

// parseLocation reads a --storage or --index location. A path that starts
// with a volume, such as e:/backups, is a path, not a URL with scheme e.
func parseLocation(raw string) (*url.URL, error) {
	if filepath.VolumeName(raw) != "" {
		return &url.URL{Path: filepath.ToSlash(raw)}, nil
	}

	return url.Parse(raw)
}

func makeLocation(u *url.URL) (string, error) {
	return createFolders(u.Host + u.Path)
}

func initPack(u *url.URL, s StoreSettings) (backup.ObjectStore, error) {
	archiveLocation, err := makeLocation(u)
	if err != nil {
		return nil, err
	}

	file, err := fileblob.OpenBucket(archiveLocation, &fileblob.Options{NoTempDir: true})
	if err != nil {
		return nil, err
	}

	options, err := s.atRest()
	if err != nil {
		return nil, err
	}

	return s.withIndex(func(x *sql.Index) (*pack.PackStorage, error) {
		return pack.NewPackStorage(append(options,
			pack.WithArchiveStorage(storage.NewBucketStore(file)),
			pack.WithArchiveIndex(x),
			pack.WithOwned(x),
			pack.WithMaxParallel(1),
			pack.WithMaxSize(1024*1024*1024),
			pack.WithSessionLease(10*time.Minute),
			pack.WithCompaction(pack.CompactionConfig{MinimumCandidates: 100}),
		)...)
	})
}

// storeIndexes holds the index each pack store keeps its archives in,
// which is the index OpenIndex hands out for that store.
var storeIndexes = map[backup.ObjectStore]*sql.Index{}

// withIndex opens the --index location as the archive index of the pack
// store build makes, and records it for OpenIndex. The store closes it.
func (s StoreSettings) withIndex(build func(*sql.Index) (*pack.PackStorage, error)) (backup.ObjectStore, error) {
	if s.Index == "" {
		return nil, errors.New("--index is required: where this store's index lives, a directory for SQLite or a postgres:// url")
	}

	u, err := parseLocation(s.Index)
	if err != nil {
		return nil, fmt.Errorf("invalid index location %s: %w", s.Index, err)
	}

	x, err := openSQL(u)
	if err != nil {
		return nil, err
	}

	packs, err := build(x)
	if err != nil {
		return nil, err
	}

	x.ObjectStore = packs

	err = x.Open()
	if err != nil {
		return nil, fmt.Errorf("opening the index: %w", err)
	}

	storeIndexes[packs] = x

	return packs, nil
}

// atRest is the pack options for the global --at-rest-key flag, none
// without it.
func (s StoreSettings) atRest() ([]pack.PackOption, error) {
	path := s.AtRestKey
	if path == "" {
		return nil, nil
	}

	key, err := pack.LoadAtRestKey(path)
	if err != nil {
		return nil, fmt.Errorf("loading the at-rest key: %w", err)
	}

	return []pack.PackOption{pack.WithAtRestKey(key)}, nil
}

// initGCS opens a Cloud Storage bucket, initS3 an S3-compatible one. Every
// query parameter (prefix, endpoint, region, ...) goes to the bucket; the
// archive index is the --index location and the metadata cache sits under
// --cache-dir.
func initGCS(u *url.URL, s StoreSettings) (backup.ObjectStore, error) {
	return initBucket("gs", u, s)
}

func initS3(u *url.URL, s StoreSettings) (backup.ObjectStore, error) {
	return initBucket("s3", u, s)
}

func initBucket(scheme string, u *url.URL, s StoreSettings) (backup.ObjectStore, error) {
	query := u.Query()
	if query.Has("index") {
		return nil, errors.New("a bucket url takes no index; the --index location is the store's index")
	}

	if query.Has("cache") {
		return nil, errors.New("a bucket url takes no cache; --cache-dir names where caches go")
	}

	bucket, err := blob.OpenBucket(context.Background(), (&url.URL{Scheme: scheme, Host: u.Host, RawQuery: query.Encode()}).String())
	if err != nil {
		return nil, err
	}

	options, err := s.atRest()
	if err != nil {
		return nil, err
	}

	return s.withIndex(func(x *sql.Index) (*pack.PackStorage, error) {
		limit, err := s.MetadataCacheLimit()
		if err != nil {
			return nil, unwrapped{err}
		}

		return storage.NewBucketObjectStore(bucket, x, s.StoreCache("metadata"), limit, append(options, pack.WithOwned(x))...)
	})
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

func initRemote(u *url.URL, s StoreSettings) (backup.ObjectStore, error) {
	if err := checkRemoteParams(u); err != nil {
		return nil, err
	}

	addr := remoteAddress(u)

	key, err := remoteKey(u)
	if err != nil {
		return nil, err
	}

	ca := u.Query().Get("ca")
	creds := auth.Credentials{Secret: key, AgentID: s.AgentID}

	if u.Scheme == insecureScheme {
		if ca != "" {
			return nil, fmt.Errorf("%s:// presents no certificate to check against %s", u.Scheme, ca)
		}

		log.Printf("Talking to %s without TLS; the api key and everything uploaded is readable on the way", addr)
		creds.Plaintext = true

		return storage.NewPlaintextClient(addr, creds)
	}

	tlsConfig, err := storage.ClientTLS(ca)
	if err != nil {
		return nil, err
	}

	return storage.NewClient(addr, creds, tlsConfig)
}

// initSQL opens the index at the --index location: a directory (with or
// without a sqlite:// or file:// scheme) holds an SQLite database and
// postgres:// names a database server.
func initSQL(u *url.URL, store backup.ObjectStore) (backup.Index, error) {
	x, err := openSQL(u)
	if err != nil {
		return nil, err
	}

	x.ObjectStore = store

	return x, nil
}

func openSQL(u *url.URL) (*sql.Index, error) {
	location := u.String()

	switch u.Scheme {
	case "", "file", "sqlite":
		loc, err := makeLocation(u)
		if err != nil {
			return nil, err
		}

		location = loc
	}

	log.Printf("Opening %s index at %s", indexDialect(u.Scheme), location)

	// a local index keeps every commit until a policy is set
	x := sql.New(location, nil)
	x.DefaultPolicy = ptr(retention.KeepAll)

	return x, nil
}

func indexDialect(scheme string) string {
	switch scheme {
	case "postgres", "postgresql":
		return "postgres"
	default:
		return "sqlite"
	}
}

var storageDrivers = map[string]func(*url.URL, StoreSettings) (backup.ObjectStore, error){
	"":                initPack,
	"gcs":             initGCS,
	"gs":              initGCS,
	"s3":              initS3,
	"goback":          initRemote,
	"goback+insecure": initRemote,
}

var indexDrivers = map[string]func(*url.URL, backup.ObjectStore) (backup.Index, error){
	"":           initSQL,
	"file":       initSQL,
	"sqlite":     initSQL,
	"postgres":   initSQL,
	"postgresql": initSQL,
}

// RestoreSession opens a session naming the object a restore reads, when
// the store has sessions, so retirement and garbage collection keep it
// while the restore runs. The returned func ends the session.
func RestoreSession(ctx context.Context, store backup.ObjectStore, set string, ref *proto.Ref) (context.Context, func(), error) {
	sessions, ok := store.(backup.SessionStore)
	if !ok {
		return ctx, func() {}, nil
	}

	sctx, err := sessions.BeginSession(ctx, &backup.Session{Set: set, Restore: ref})
	if err != nil {
		return nil, nil, err
	}

	return sctx, func() {
		if err := sessions.EndSession(sctx); err != nil {
			log.Printf("Failed ending restore session: %v", err)
		}
	}, nil
}

// RechunkFlag turns --rechunk on a restore command.
var RechunkFlag = cli.BoolFlag{
	Name:  "rechunk",
	Usage: "cut an existing destination with the backup's chunker, so parts that only moved are not downloaded",
}

// SalvageFlag turns --salvage on a restore command.
var SalvageFlag = cli.BoolFlag{
	Name:  "salvage",
	Usage: "write what can be read when parts are missing, leaving holes, and report them",
}

// Salvage applies --salvage to the restorer and logs every hole it leaves.
func Salvage(c *cli.Context, restorer *backup.Restorer) {
	if !c.Bool("salvage") {
		return
	}

	restorer.Salvage = true
	restorer.OnHole = func(hole backup.Hole) {
		log.Printf("missing %d bytes at offset %d of %s (%x)", hole.Length, hole.Offset, hole.Path, hole.Ref.GetHash())
	}
}

// unwrapped marks a driver error OpenStore reports as it is, without
// naming the driver.
type unwrapped struct{ error }

// GetObjectStore opens the store the global --storage location names, or
// exits.
func GetObjectStore(c *cli.Context) backup.ObjectStore {
	store, err := Settings(c).OpenStore()
	if err != nil {
		Fatal(err)
	}

	return store
}

// OpenStore opens the store the Storage location names.
func (s StoreSettings) OpenStore() (backup.ObjectStore, error) {
	if s.Storage == "" {
		return nil, errors.New("--storage is required: where the objects live")
	}

	u, err := parseLocation(s.Storage)
	if err != nil {
		return nil, fmt.Errorf("Invalid storage location %s: %w", s.Storage, err)
	}

	driver, ok := storageDrivers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("No driver for storage location %s", u.String())
	}

	store, err := driver(u, s)
	if plain := (unwrapped{}); errors.As(err, &plain) {
		return nil, plain.error
	}

	if err != nil {
		return nil, fmt.Errorf("Could not initialise storage driver %s: %w", u.Scheme, err)
	}

	if op, ok := store.(Opener); ok {
		if err := op.Open(); err != nil {
			return nil, fmt.Errorf("Could not open object store %s: %w", u.Scheme, err)
		}
	}

	return store, nil
}

// OpenIndex opens the index the --index location names, or the store
// itself when it is one, or exits.
func OpenIndex(c *cli.Context, store backup.ObjectStore) backup.Index {
	idx, err := Settings(c).OpenIndex(store)
	if err != nil {
		Fatal(err)
	}

	return idx
}

// OpenIndex opens the index of store: the one OpenStore opened with it,
// the store itself when it is one, else the one the Index location names.
func (s StoreSettings) OpenIndex(store backup.ObjectStore) (backup.Index, error) {
	if x, ok := storeIndexes[store]; ok {
		return x, nil
	}

	if idx, ok := store.(backup.Index); ok {
		log.Println("Store implements index")
		return idx, nil
	}

	if s.Index == "" {
		return nil, errors.New("--index is required: where the store's index lives")
	}

	u, err := parseLocation(s.Index)
	if err != nil {
		return nil, fmt.Errorf("Invalid index location %s: %w", s.Index, err)
	}

	log.Printf("Loading %s index driver", u.Scheme)

	driver, ok := indexDrivers[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("No driver for storage location %s", u.String())
	}

	idx, err := driver(u, store)
	if err != nil {
		return nil, fmt.Errorf("Could not initialise index driver %s: %w", u.Scheme, err)
	}

	if err := idx.Open(); err != nil {
		return nil, fmt.Errorf("Could not open object index %s: %w", u.Scheme, err)
	}

	return idx, nil
}

func ptr[T any](v T) *T {
	return &v
}

// Reindex rebuilds the indexes of the tables a maintenance operation
// churned, when idx keeps any, and exits on failure.
func Reindex(c *cli.Context, idx backup.Index) {
	r, ok := idx.(maintenance.Reindexer)
	if !ok {
		return
	}

	if _, err := r.ReindexChurned(Context(c)); err != nil {
		Fatalf("Reindexing failed: %v", err)
	}
}
