package gc

import (
	"log"
	"strings"
	"time"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"

	"github.com/urfave/cli"
)

// Command is the gc command.
var Command = cli.Command{
	Name:   "gc",
	Usage:  "Mark every object reachable from a live commit or pin and rewrite archives whose dead share justifies it",
	Action: gcAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "readers",
			Usage: "parallel object reads during the mark",
			Value: 32,
		},
		cli.Float64Flag{
			Name:  "dead-ratio",
			Usage: "rewrite an archive once this share of its bytes is dead",
			Value: 0.3,
		},
		cli.DurationFlag{
			Name:  "erasure-bound",
			Usage: "rewrite an archive that has held dead objects this long regardless of ratio",
			Value: 14 * 24 * time.Hour,
		},
		cli.DurationFlag{
			Name:  "min-age",
			Usage: "never drop an object younger than this",
			Value: 24 * time.Hour,
		},
		cli.BoolFlag{
			Name:  "no-sweep",
			Usage: "mark only",
		},
		cli.StringFlag{
			Name:  "temp-dir",
			Usage: "directory for the live-set runs; defaults to the system temp dir",
		},
	},
}

func gcAction(c *cli.Context) {
	ctx := common.Context(c)
	repo := common.OpenRepo(c)
	store := repo.Store

	collector, ok := wrapped.Innermost(store).(pack.Collector)
	if !ok {
		common.Fatalf("storage %s cannot be garbage collected", c.GlobalString("storage"))
	}

	opts := pack.CollectOptions{
		Readers:      c.Int("readers"),
		DeadRatio:    c.Float64("dead-ratio"),
		ErasureBound: c.Duration("erasure-bound"),
		MinAge:       c.Duration("min-age"),
		NoSweep:      c.Bool("no-sweep"),
		TempDir:      c.String("temp-dir"),
	}

	report, err := maintenance.Collect(ctx, collector, repo.Index, opts)
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.View.Report(report), func() { Log(report) })

	common.CloseRepo(repo)
}

// Log prints one line per phase of a collection report.
func Log(report *pack.CollectReport) {
	for _, line := range strings.Split(report.Summary(), "\n") {
		log.Print(line)
	}
}
