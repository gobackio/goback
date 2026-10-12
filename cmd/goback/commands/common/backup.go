package common

import (
	"context"
	"errors"
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/urfave/cli"
)

// Settings are the store settings the global flags give.
func Settings(c *cli.Context) (goback.Settings, error) {
	settings, err := agent.Store{
		Storage:           c.GlobalString("storage"),
		Index:             c.GlobalString("index"),
		AgentID:           c.GlobalString("agent-id"),
		StoreKey:          c.GlobalString("store-key"),
		AtRestKey:         c.GlobalString("at-rest-key"),
		CacheDir:          c.GlobalString("cache-dir"),
		MetadataCacheSize: c.GlobalString("metadata-cache-size"),
		BlobCacheSize:     c.GlobalString("blob-cache-size"),
	}.Settings()

	settings.Passphrase = c.GlobalString("passphrase")

	return settings, err
}

// OpenRepo opens the store the global flags name, or exits. Close it with
// CloseRepo.
func OpenRepo(c *cli.Context) *goback.Repo {
	settings, err := Settings(c)
	if err != nil {
		Fatal(err)
	}

	if settings.Storage == "" {
		Fatal(errors.New("--storage is required: where the objects live"))
	}

	repo, err := goback.Open(Context(c), settings)
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
