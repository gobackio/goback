package common

import (
	"context"
	"errors"
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"

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

// Restorer is a restorer of repo set up from the restore flags shared by
// commit restore and file restore, or exits.
func Restorer(c *cli.Context, repo *goback.Repo) *backup.Restorer {
	overwrite, err := backup.ParseOverwriteMode(c.String("overwrite"))
	if err != nil {
		Fatal(err)
	}

	key := StoreKey(c, repo)

	restorer := &backup.Restorer{
		Store:     repo.Store,
		Key:       key,
		Cache:     BlobCache(c, repo),
		Workers:   c.Int("workers"),
		Overwrite: overwrite,
		Verify:    c.Bool("verify"),
		DryRun:    c.Bool("dry-run"),
		Rechunk:   c.Bool("rechunk"),
		Salvage:   c.Bool("salvage"),
	}

	if restorer.Salvage {
		restorer.OnHole = func(hole backup.Hole) {
			log.Printf("missing %d bytes at offset %d of %s (%x)", hole.Length, hole.Offset, hole.Path, hole.Ref.GetHash())
		}
	}

	if seeds := c.StringSlice("seed"); len(seeds) > 0 {
		restorer.Seeds = backup.NewSeedMap(key)
		for _, seed := range seeds {
			if err := restorer.Seeds.Add(seed); err != nil {
				Fatalf("indexing seed %s: %v", seed, err)
			}
		}

		log.Printf("Indexed %d chunks from %d seeds", restorer.Seeds.Len(), len(seeds))
	}

	return restorer
}
