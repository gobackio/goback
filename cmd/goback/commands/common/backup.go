package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

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

	passphrase, perr := Passphrase(c.GlobalString("passphrase"), c.GlobalString("passphrase-file"))
	settings.Passphrase = passphrase

	return settings, errors.Join(err, perr)
}

// Passphrase is value, else the contents of the file at path without the
// space around them, else empty.
func Passphrase(value, path string) (string, error) {
	if value != "" || path == "" {
		return value, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading the passphrase file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

// OpenRepo opens the store the global flags name, or exits. Close it with
// CloseRepo.
func OpenRepo(c *cli.Context) *goback.Repo {
	return openRepo(c, false)
}

// CreateRepo is OpenRepo for a command that backs up, which makes a
// directory store and a local index that do not exist yet.
func CreateRepo(c *cli.Context) *goback.Repo {
	return openRepo(c, true)
}

func openRepo(c *cli.Context, create bool) *goback.Repo {
	settings, err := Settings(c)
	if err != nil {
		Fatal(err)
	}

	settings.Create = create

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
	Usage: "split the existing destination with the backup's chunker, so parts that only moved are not downloaded",
}

// SalvageFlag turns --salvage on a restore command.
var SalvageFlag = cli.BoolFlag{
	Name:  "salvage",
	Usage: "write the readable parts when parts are missing; leave holes for the rest and report them",
}

// OwnerFlag sets --owner on a restore command.
var OwnerFlag = cli.StringFlag{
	Name:  "owner",
	Usage: "who owns what is restored, on Unix as root: none leaves it to you; recorded gives each entry the user and group it was backed up with, by name; USER[:GROUP] gives everything to that account",
	Value: "none",
}

// SeedModeFlag sets --seed-mode on a restore command.
var SeedModeFlag = cli.StringFlag{
	Name:  "seed-mode",
	Usage: "how a --seed directory is read: guess lists it and reads, as each file is restored, the files most likely to hold its parts, by path, name, then extension and size; full reads every file in it up front",
	Value: backup.DefaultSeedMode.String(),
}

// Restorer is a restorer of repo set up from the restore flags shared by
// commit restore and file restore, or exits.
func Restorer(c *cli.Context, repo *goback.Repo) *backup.Restorer {
	overwrite, err := backup.ParseOverwriteMode(c.String("overwrite"))
	if err != nil {
		Fatal(err)
	}

	owner, err := backup.ParseOwner(c.String("owner"))
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
		Owner:     owner,
	}

	if restorer.Salvage {
		restorer.OnHole = func(hole backup.Hole) {
			log.Printf("missing %d bytes at offset %d of %s (%x)", hole.Length, hole.Offset, hole.Path, hole.Ref.GetHash())
		}
	}

	if seeds := c.StringSlice("seed"); len(seeds) > 0 {
		mode := backup.DefaultSeedMode
		if name := c.String("seed-mode"); name != "" {
			if mode, err = backup.ParseSeedMode(name); err != nil {
				Fatal(err)
			}
		}

		restorer.Seeds = backup.NewSeedMap(key)
		for _, seed := range seeds {
			if err := restorer.Seeds.AddSeed(seed, mode); err != nil {
				Fatalf("indexing seed %s: %v", seed, err)
			}
		}

		if n := restorer.Seeds.Len(); n > 0 {
			log.Printf("Indexed %d chunks from %d seeds", n, len(seeds))
		}
	}

	return restorer
}
