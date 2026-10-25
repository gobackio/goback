package file

import (
	"fmt"
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

func (f *file) restore() error {
	node, outcome, err := f.restorer.RestorePath(f.ctx, f.index, f.set, f.src, f.when, f.dst)
	if err != nil {
		return err
	}

	switch outcome {
	case backup.OutcomeLost:
		return fmt.Errorf("%s is not restored: the store lost it", f.dst)
	case backup.OutcomeCloudOnly:
		return fmt.Errorf("%s is not restored: its content was only in the cloud when it was backed up", f.dst)
	}

	info := node.Stat
	log.Printf("name: %s, size: %d, mod: %s", info.Name, info.Size, info.ModTime())

	stats := f.restorer.Stats()

	common.Result(struct {
		File    views.NodeView      `json:"file"`
		Outcome string              `json:"outcome"`
		Path    string              `json:"path"`
		Stats   backup.RestoreStats `json:"stats"`
	}{common.View.Node(node), outcome.String(), f.dst, stats}, func() {
		log.Printf("%s: %s", outcome, f.dst)
		log.Printf("bytes from destination %d, seeds %d, cache %d, store %d", stats.BytesFromDestination, stats.BytesFromSeeds, stats.BytesFromCache, stats.BytesFromStore)

		if stats.SecurityLeftOut > 0 {
			log.Print("the security descriptor was left out, which a restore on Windows as an elevated administrator sets")
		}

		if stats.StreamsLeftOut > 0 {
			log.Printf("%d alternate data streams left out: the destination cannot hold them", stats.StreamsLeftOut)
		}

		if stats.OwnersLeftOut > 0 {
			log.Print("it kept the restoring user as owner: its recorded user or group has no account here")
		}
	})

	return nil
}

func restoreAction(c *cli.Context) {
	src := c.Args().Get(0)
	dst := c.Args().Get(1)
	age := c.Args().Get(2)
	if age == "" {
		age = "0"
	}

	d, err := time.ParseDuration(age)
	if err != nil {
		common.Fatalf("Failed parsing <age> parameter: %v", err)
	}

	when := time.Now().Add(-d)

	repo := common.OpenRepo(c)
	store := repo.Store
	idx := repo.Index

	restorer := common.Restorer(c, repo)
	key := restorer.Key

	f := &file{
		ctx:      common.Context(c),
		key:      key,
		reader:   backup.NewBackupReader(store).WithKey(key),
		restorer: restorer,
		store:    store,
		src:      src,
		dst:      dst,
		when:     when,
		index:    idx,
		set:      c.GlobalString("set"),
	}

	if err := f.restore(); err != nil {
		common.Fatal(err)
	}

	common.SweepBlobCache(restorer.Cache)

	if err := idx.Close(); err != nil {
		common.Fatal(err)
	}
}

var restoreCmd = cli.Command{
	Name:      "restore",
	Usage:     "Restore a file in place; parts the destination already has are not downloaded",
	ArgsUsage: "<path> <destination> [age]",
	Action:    restoreAction,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "overwrite",
			Usage: "how to treat an existing file: always hashes every part and rewrites what differs; if-changed skips it when size and mtime match",
			Value: "always",
		},
		cli.BoolFlag{
			Name:  "verify",
			Usage: "hash the written file again before it replaces the destination",
		},
		cli.BoolFlag{
			Name:  "dry-run",
			Usage: "report what would change without writing",
		},
		cli.StringSliceFlag{
			Name:  "seed",
			Usage: "file or directory to copy matching chunks from instead of downloading them; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.IntFlag{
			Name:  "workers",
			Usage: "how many files and parts to fetch at once",
			Value: backup.DefaultRestoreWorkers(),
		},
		common.SalvageFlag,
		common.RechunkFlag,
		common.SeedModeFlag,
		common.OwnerFlag,
	},
}
