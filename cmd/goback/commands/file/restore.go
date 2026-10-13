package file

import (
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
	Usage:     "Restore a file in place, downloading only the parts the destination does not already hold",
	ArgsUsage: "<path> <destination> [age]",
	Action:    restoreAction,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "overwrite",
			Usage: "always: hash every part of an existing file and rewrite what differs; if-changed: trust a size and mtime match",
			Value: "always",
		},
		cli.BoolFlag{
			Name:  "verify",
			Usage: "rehash the written file before it replaces the destination",
		},
		cli.BoolFlag{
			Name:  "dry-run",
			Usage: "report what would change without writing",
		},
		cli.StringSliceFlag{
			Name:  "seed",
			Usage: "file or directory whose chunks may be used instead of downloading; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.IntFlag{
			Name:  "workers",
			Usage: "files and parts fetched at once",
			Value: backup.DefaultRestoreWorkers(),
		},
		common.SalvageFlag,
		common.RechunkFlag,
	},
}
