package common

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

// StoreSettings are the global store flags as given, which the agent's
// config file also sets.
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

// Parse turns the flag values into goback.Settings. A cache size of 0
// means no bound.
func (s StoreSettings) Parse() (goback.Settings, error) {
	metadata, err := cacheSize("--metadata-cache-size", s.MetadataCacheSize)
	if err != nil {
		return goback.Settings{}, err
	}

	blobs, err := cacheSize("--blob-cache-size", s.BlobCacheSize)
	if err != nil {
		return goback.Settings{}, err
	}

	return goback.Settings{
		Storage:           s.Storage,
		Index:             s.Index,
		AgentID:           s.AgentID,
		StoreKey:          s.StoreKey,
		Passphrase:        s.Passphrase,
		AtRestKey:         s.AtRestKey,
		CacheDir:          s.CacheDir,
		MetadataCacheSize: metadata,
		BlobCacheSize:     blobs,
	}, nil
}

func cacheSize(flag, value string) (int64, error) {
	size, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", flag, err)
	}

	if size == 0 {
		return -1, nil
	}

	return int64(size), nil
}

// Open opens the store s names.
func (s StoreSettings) Open(ctx context.Context) (*goback.Repo, error) {
	if s.Storage == "" {
		return nil, errors.New("--storage is required: where the objects live")
	}

	settings, err := s.Parse()
	if err != nil {
		return nil, err
	}

	return goback.Open(ctx, settings)
}

// OpenRepo opens the store the global flags name, or exits. Close it with
// CloseRepo.
func OpenRepo(c *cli.Context) *goback.Repo {
	repo, err := Settings(c).Open(Context(c))
	if err != nil {
		Fatal(err)
	}

	return repo
}

// CloseRepo compacts the store when it can, then closes it and its index.
func CloseRepo(repo *goback.Repo) {
	if _, err := repo.Compact(context.Background()); err != nil {
		log.Printf("Compaction failed: %v", err)
	}

	if err := repo.Close(); err != nil {
		log.Printf("Closing the store failed: %v", err)
	}
}

// StoreKey is the store key the global flags name, or exits. It is nil for
// a store written in the clear.
func StoreKey(c *cli.Context, repo *goback.Repo) *storekey.Key {
	key, err := repo.Key(Context(c))
	if err != nil {
		Fatal(err)
	}

	return key
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
