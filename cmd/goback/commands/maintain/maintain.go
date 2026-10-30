// Package maintain runs a store's housekeeping: whatever is due, or one
// operation.
package maintain

import (
	"log"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/cmd/goback/commands/gc"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

// Command is the maintain command.
var Command = cli.Command{
	Name:        "maintain",
	Usage:       "Run the housekeeping that is due, for example from cron",
	Description: "Without a subcommand, maintain sweeps, retires, compacts, splits and builds presence filters. It collects garbage once the last collection is older than --gc-interval. It also rebuilds the database indexes of heavily changed tables in the index. A subcommand runs one of these operations. A store server runs this housekeeping on its own schedule.",
	Flags: []cli.Flag{
		cli.DurationFlag{
			Name:  "gc-interval",
			Usage: "collect garbage when the last collection is at least this old; 0 turns collection off",
			Value: maintenance.DefaultSchedule.Collect,
		},
	},
	Action: action(due),
	Subcommands: []cli.Command{
		{
			Name:  "sweep",
			Usage: "Finalize idle archives and end sessions whose lease has expired",
			Action: action(func(m *members) {
				m.store().Sweep(time.Now())
				common.Result(common.Done{Action: "swept"}, func() {})
			}),
		},
		{
			Name:  "compact",
			Usage: "Rewrite small archives into full-sized ones",
			Action: action(func(m *members) {
				runner := m.runner(maintenance.DefaultSchedule)
				if runner.Store == nil {
					common.Fatalf("storage %s has no archives to maintain", goback.Redacted(m.c.GlobalString("storage")))
				}

				report, err := runner.Compact(common.Context(m.c))
				if err != nil {
					common.Fatal(err)
				}
				common.Result(common.View.Compact(report), func() { log.Print(report.Summary()) })
			}),
		},
		{
			Name:  "split",
			Usage: "Rewrite old archives holding file data beside metadata into archives holding one or the other, oldest first",
			Flags: []cli.Flag{
				cli.DurationFlag{Name: "age", Usage: "split only archives at least this old", Value: 7 * 24 * time.Hour},
				cli.StringFlag{Name: "budget", Usage: "split at most this much", Value: "30GB"},
			},
			Action: action(func(m *members) {
				splitter, ok := wrapped.Innermost(m.objects).(maintenance.Splitter)
				if !ok {
					common.Fatalf("storage %s has no archives to split", goback.Redacted(m.c.GlobalString("storage")))
				}

				budget, err := humanize.ParseBytes(m.c.String("budget"))
				if err != nil {
					common.Fatalf("Reading --budget: %v", err)
				}

				report, err := splitter.Split(common.Context(m.c), pack.SplitOptions{Age: m.c.Duration("age"), Budget: budget})
				if err != nil {
					common.Fatal(err)
				}
				common.Result(common.View.Compact(&report.CompactReport), func() { log.Print(report.Summary()) })
			}),
		},
		{
			Name:  "retire",
			Usage: "Tombstone the retired commits whose trash window has ended",
			Action: action(func(m *members) {
				runner := m.runner(maintenance.DefaultSchedule)
				if runner.Retirer == nil {
					common.Fatalf("Index %T keeps no retention state", m.index)
				}

				n, err := runner.Retire(common.Context(m.c))
				if err != nil {
					common.Fatalf("Retirement failed after %d commits: %v", n, err)
				}
				common.Result(counted{"retired", n}, func() { log.Printf("Retired %d commits", n) })
			}),
		},
		{
			Name:  "presence",
			Usage: "Build a presence filter for each set whose newest commit lacks one",
			Action: action(func(m *members) {
				presence, ok := m.index.(maintenance.Presence)
				if !ok {
					common.Fatalf("Index %T keeps no presence filters", m.index)
				}

				n, err := presence.BuildPendingPresence(common.Context(m.c))
				if err != nil {
					common.Fatal(err)
				}

				common.Result(counted{"built_presence", n}, func() { log.Printf("Built %d presence filters", n) })
			}),
		},
		{
			Name:  "check",
			Usage: "Report the commits in the store that are missing from the index and not tombstoned",
			Action: action(func(m *members) {
				checker, ok := m.index.(index.OrphanFinder)
				if !ok {
					common.Fatalf("Index %T keeps no commit rows", m.index)
				}

				orphans, err := checker.OrphanCommits(common.Context(m.c))
				if err != nil {
					common.Fatal(err)
				}

				found := make([]views.OrphanCommitView, len(orphans))
				var size int64
				for i, orphan := range orphans {
					found[i] = common.View.OrphanCommit(orphan)
					size += orphan.Bytes
				}

				common.Result(found, func() {
					for _, orphan := range found {
						log.Printf("Commit %s has no row: %d copies, %s", orphan.Ref, orphan.Copies, humanize.Bytes(uint64(orphan.Bytes)))
					}

					log.Printf("%d commits without a row (%s); a collection reports what only they reach as unattributed", len(orphans), humanize.Bytes(uint64(size)))
				})
			}),
		},
		{
			Name:  "drop-orphans",
			Usage: "Tombstone the commits in the store that are missing from the index, as check lists them; without --apply, only list them",
			Flags: []cli.Flag{
				cli.BoolFlag{Name: "apply", Usage: "write the tombstones"},
				cli.DurationFlag{Name: "age", Usage: "leave out commits received less than this long ago, which may still be on their way into the index", Value: 24 * time.Hour},
			},
			Action: action(func(m *members) {
				dropper, ok := m.index.(index.OrphanDropper)
				if !ok {
					common.Fatalf("Index %T keeps no commit rows", m.index)
				}

				apply := m.c.Bool("apply")

				orphans, err := dropper.DropOrphans(common.Context(m.c), time.Now().Add(-m.c.Duration("age")), !apply)
				if err != nil {
					common.Fatalf("Dropping orphans failed after %d commits: %v", len(orphans), err)
				}

				found := make([]views.OrphanCommitView, len(orphans))
				for i, orphan := range orphans {
					found[i] = common.View.OrphanCommit(orphan)
				}

				common.Result(found, func() {
					for _, orphan := range found {
						log.Printf("Commit %s has no row: %d copies, %s", orphan.Ref, orphan.Copies, humanize.Bytes(uint64(orphan.Bytes)))
					}

					if apply {
						log.Printf("Tombstoned %d commits", len(orphans))
					} else {
						log.Printf("Would tombstone %d commits; --apply writes the tombstones", len(orphans))
					}
				})
			}),
		},
		{
			Name:  "sizes",
			Usage: "Record the logical size of commits that lack one; measure the sets whose commits changed",
			Action: action(func(m *members) {
				sizer, ok := m.index.(sizeFiller)
				if !ok {
					common.Fatalf("Index %T keeps no commit sizes", m.index)
				}

				n, err := sizer.FillMissingSizes(common.Context(m.c))
				if err != nil {
					common.Fatal(err)
				}

				measured, err := sizer.MeasureSets(common.Context(m.c))
				if err != nil {
					common.Fatal(err)
				}

				common.Result([]counted{{"filled_sizes", n}, {"measured_sets", len(measured)}}, func() {
					log.Printf("Filled %d commit sizes, measured %d sets", n, len(measured))
				})
			}),
		},
	},
}

func due(m *members) {
	schedule := maintenance.DefaultSchedule
	schedule.Collect = m.c.Duration("gc-interval")

	ran, err := m.runner(schedule).Due(common.Context(m.c))

	view := common.View.Maintenance(ran)
	if ran.Compacted != nil {
		report := common.View.Compact(ran.Compacted)
		view.Compacted = &report
	}

	if ran.Collected != nil {
		report := common.View.Report(ran.Collected)
		view.Collected = &report
	}

	common.Result(view, func() {
		log.Printf("Swept: %t, retired %d commits, built %d presence filters, measured %d sets", ran.Swept, ran.Retired, ran.Presence, ran.Measured)

		if ran.Compacted != nil {
			log.Print(ran.Compacted.Summary())
		}

		if ran.Collected != nil {
			gc.Log(ran.Collected)
		} else {
			log.Print("No collection due")
		}
	})

	if err != nil {
		common.Fatal(err)
	}
}

// counted is what a maintenance operation did, and to how many things.
type counted struct {
	Action string `json:"action"`
	Count  int    `json:"count"`
}

// sizeFiller is an index that works out the size of each commit it
// recorded without one, and measures its sets.
type sizeFiller interface {
	index.SizeFiller
	maintenance.Measurer
}

type members struct {
	c       *cli.Context
	objects backup.ObjectStore
	index   backup.Index
}

func (m *members) runner(schedule maintenance.Schedule) *maintenance.Runner {
	return maintenance.NewRunner(m.objects, m.index, schedule)
}

func (m *members) store() maintenance.Store {
	store, ok := wrapped.Innermost(m.objects).(maintenance.Store)
	if !ok {
		common.Fatalf("storage %s has no archives to maintain", goback.Redacted(m.c.GlobalString("storage")))
	}

	return store
}

func action(run func(*members)) func(*cli.Context) {
	return func(c *cli.Context) {
		repo := common.OpenRepo(c)

		run(&members{c: c, objects: repo.Store, index: repo.Index})

		if err := repo.Close(); err != nil {
			log.Printf("Closing the store failed: %v", err)
		}
	}
}
