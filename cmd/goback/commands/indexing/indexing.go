// Package indexing indexes objects the store already holds.
package indexing

import (
	"log"
	"os"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/index"

	"github.com/urfave/cli"
)

// Command is the index command.
var Command = cli.Command{
	Name:  "index",
	Usage: "Index objects the store already holds, without a rebuild",
	Subcommands: []cli.Command{
		{
			Name:        "commits",
			Usage:       "Index the commits a file lists, one hex ref per line",
			Description: "Indexes them by set in receipt order as a rebuild does, those that name no set under a placeholder set; refuses the lot if one is not a commit, is tombstoned or already indexed",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "refs-file", Usage: "file of commit refs, one hex ref per line"},
				cli.BoolFlag{Name: "dry-run", Usage: "write nothing, report what would be done"},
			},
			Action: commitsAction,
		},
	},
}

func commitsAction(c *cli.Context) {
	name := c.String("refs-file")
	if name == "" {
		common.Fatalf("--refs-file is required")
	}

	file, err := os.Open(name)
	if err != nil {
		common.Fatal(err)
	}

	refs, err := index.ReadRefs(file)
	file.Close()
	if err != nil {
		common.Fatalf("%s: %v", name, err)
	}

	repo := common.OpenRepo(c)
	defer common.CloseRepo(repo)

	x := repo.Index
	defer x.Close()

	indexer, ok := x.(index.CommitIndexer)
	if !ok {
		common.Fatalf("Index %T cannot index single commits", x)
	}

	dryRun := c.Bool("dry-run")

	sets, err := indexer.IndexCommits(common.Context(c), refs, dryRun)
	found := make([]views.IndexedSetView, len(sets))
	for i, s := range sets {
		found[i] = common.View.IndexedSet(s)
	}

	common.Result(found, func() { logSets(sets, dryRun) })

	if err != nil {
		common.Fatal(err)
	}
}

// logSets prints what IndexCommits did, or on a dry run would do, per set.
func logSets(sets []index.IndexedSet, dryRun bool) {
	for _, s := range sets {
		log.Printf("Set %q (id %d, new: %t, placeholder: %t): %d commits received %s to %s, chained: %t",
			s.Set, s.SetID, s.Created, s.Placeholder, s.Commits, s.Oldest.Format("2006-01-02 15:04:05.000000"), s.Newest.Format("2006-01-02 15:04:05.000000"), s.Chained)

		if !dryRun {
			log.Printf("  indexed %d, %d of them a microsecond after a tie, %d received before the set's newest left out", s.Indexed, s.Tied, s.Behind)
		}
	}
}
