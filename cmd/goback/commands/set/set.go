// Package set manages backup sets.
package set

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/index"

	"github.com/urfave/cli"
)

// Command is the set command.
var Command = cli.Command{
	Name:        "set",
	Description: "Delete and undelete sets, and show or change their retention",
	Subcommands: []cli.Command{
		{
			Name:      "delete",
			Usage:     "Close a set and retire its commits; --erase skips the trash window",
			ArgsUsage: "<name>",
			Flags:     []cli.Flag{cli.BoolFlag{Name: "erase", Usage: "tombstone the commits at once instead of after the trash window"}},
			Action:    deleteAction,
		},
		{
			Name:      "undelete",
			Usage:     "Reopen a set before its commits are tombstoned",
			ArgsUsage: "<name>",
			Action:    undeleteAction,
		},
		{
			Name:      "retention",
			Usage:     "Show or change the retention of a set, or the store's defaults without a name; a keep flag replaces the policy, --inherit drops it",
			ArgsUsage: "[<name>]",
			Flags: []cli.Flag{
				cli.IntFlag{Name: "keep-last", Usage: "keep the newest this many commits"},
				cli.StringSliceFlag{Name: "keep", Usage: "a bracket as <period>=<count> calendar periods, newest first, repeated; the last may drop its count to keep forever (--keep hourly=336 --keep daily=60 --keep weekly=12 --keep monthly)"},
				cli.DurationFlag{Name: "keep-within", Usage: "keep every commit younger than this"},
				cli.BoolFlag{Name: "inherit", Usage: "drop the policy: a set inherits the store's, the store uses the built-in"},
				cli.IntFlag{Name: "trash-days", Value: -1, Usage: "store only: days a deleted commit waits for its tombstone"},
			},
			Action: retentionAction,
		},
	},
}

var keepFlags = []string{"keep-last", "keep", "keep-within"}

func retentionAction(c *cli.Context) {
	if c.NArg() > 1 {
		common.Fatal("usage: set retention [<name>] [--keep-... | --inherit] [--trash-days N]")
	}

	repo := common.OpenRepo(c)
	idx := repo.Index
	defer idx.Close()

	x, ok := idx.(admin.Index)
	if !ok {
		common.Fatalf("Index %T keeps no retention settings", idx)
	}

	ctx := common.Context(c)
	name := c.Args().First()

	var policy *retention.Policy
	for _, flag := range keepFlags {
		if c.IsSet(flag) {
			policy = &retention.Policy{
				KeepLast:   c.Int("keep-last"),
				KeepWithin: c.Duration("keep-within"),
				Brackets:   brackets(c.StringSlice("keep")),
			}
			break
		}
	}

	if policy != nil || c.Bool("inherit") {
		var err error
		if name == "" {
			err = x.SetDefaultPolicy(ctx, policy)
		} else {
			err = x.SetPolicy(ctx, name, policy)
		}
		if err != nil {
			common.Fatal(err)
		}
	}

	if days := c.Int("trash-days"); days >= 0 {
		if name != "" {
			common.Fatal("the trash window belongs to the store; leave the set name out")
		}

		if err := x.SetWindows(ctx, index.Windows{TrashDays: days}); err != nil {
			common.Fatal(err)
		}
	}

	showRetention(ctx, x, name)
}

func showRetention(ctx context.Context, x admin.Index, name string) {
	w, err := x.Windows(ctx)
	if err != nil {
		common.Fatal(err)
	}

	if name == "" {
		policy, stored, err := x.GetDefaultPolicy(ctx)
		if err != nil {
			common.Fatal(err)
		}

		source := "built-in"
		if stored {
			source = "stored"
		}

		common.Result(struct {
			Policy    views.PolicyView `json:"policy"`
			Source    string           `json:"source"`
			TrashDays int              `json:"trash_days"`
		}{viewPolicy(policy), source, w.TrashDays}, func() {
			fmt.Printf("store default: %s (%s)\ntrash %d days\n", describe(policy), source, w.TrashDays)
		})

		return
	}

	ret, err := x.GetPolicy(ctx, name)
	if err != nil {
		common.Fatal(err)
	}

	source := "inherited"
	if ret.Policy != nil {
		source = "own"
	}

	common.Result(struct {
		Set    string           `json:"set"`
		Policy views.PolicyView `json:"policy"`
		Source string           `json:"source"`
		Paused bool             `json:"paused"`
	}{name, viewPolicy(ret.Effective), source, ret.Paused}, func() {
		fmt.Printf("set %s: %s (%s)\n", name, describe(ret.Effective), source)
		if ret.Paused {
			fmt.Println("retirement is paused since the rebuild; set a policy to resume it")
		}
	})
}

func viewPolicy(p retention.Policy) views.PolicyView {
	out := common.View.Policy(p)
	out.Flags = describe(p)

	return out
}

// brackets reads the --keep flags as the stretches of a set's past,
// newest first. A bracket without a count keeps its period forever.
func brackets(flags []string) []retention.Bracket {
	var read []retention.Bracket

	for _, flag := range flags {
		period, count, hasCount := strings.Cut(flag, "=")

		bracket := retention.Bracket{Period: retention.Period(period)}
		if hasCount {
			var err error

			bracket.Count, err = strconv.Atoi(count)
			if err != nil || bracket.Count < 1 {
				common.Fatalf("--keep %s: %q is not a number of periods", flag, count)
			}
		}

		read = append(read, bracket)
	}

	return read
}

// describe prints a policy as its keep flags.
func describe(p retention.Policy) string {
	var parts []string
	if p.KeepLast > 0 {
		parts = append(parts, fmt.Sprintf("keep-last=%d", p.KeepLast))
	}

	for _, b := range p.Brackets {
		if b.Count == 0 {
			parts = append(parts, fmt.Sprintf("keep=%s", b.Period))

			continue
		}

		parts = append(parts, fmt.Sprintf("keep=%s=%d", b.Period, b.Count))
	}

	if p.KeepWithin > 0 {
		within := p.KeepWithin.String()
		if p.KeepWithin == time.Duration(1<<63-1) {
			within = "forever"
		}

		parts = append(parts, "keep-within="+within)
	}

	if len(parts) == 0 {
		return "keep nothing"
	}

	return strings.Join(parts, " ")
}

func deleteAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: set delete [--erase] <name>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	err := common.GetRetention(index).DeleteSet(common.Context(c), c.Args().First(), c.Bool("erase"))
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.Done{Action: "closed", Name: c.Args().First()}, func() { log.Printf("Set %s closed", c.Args().First()) })
	index.Close()
}

func undeleteAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: set undelete <name>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	err := common.GetRetention(index).UndeleteSet(common.Context(c), c.Args().First())
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.Done{Action: "reopened", Name: c.Args().First()}, func() { log.Printf("Set %s reopened", c.Args().First()) })
	index.Close()
}
