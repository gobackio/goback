// Package set manages backup sets.
package set

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/urfave/cli"
)

// Command is the set command.
var Command = cli.Command{
	Name:  "set",
	Usage: "List, delete or undelete sets, or show and change a set's retention",
	Subcommands: []cli.Command{
		{
			Name:   "list",
			Usage:  "List the sets with what their agents expect of them and whether they are late",
			Action: listAction,
		},
		{
			Name:      "delete",
			Usage:     "Close a set and retire its commits; --erase skips the trash window",
			ArgsUsage: "<name>",
			Flags:     []cli.Flag{cli.BoolFlag{Name: "erase", Usage: "tombstone the commits now instead of after the trash window"}},
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
			Usage:     "Show or change a set's retention policy, or the store's default when no name is given; a keep flag replaces the policy, --inherit removes it",
			ArgsUsage: "[<name>]",
			Flags: []cli.Flag{
				cli.IntFlag{Name: "keep-last", Usage: "keep this many of the newest commits"},
				cli.StringSliceFlag{Name: "keep", Usage: "keep the last commit of each of the latest <count> calendar periods, given as <period>=<count>; repeat it for each bracket, newest first; the last bracket may omit its count to keep forever (--keep hourly=336 --keep daily=60 --keep weekly=12 --keep monthly)"},
				cli.DurationFlag{Name: "keep-within", Usage: "keep every commit younger than this"},
				cli.BoolFlag{Name: "inherit", Usage: "remove the policy, so a set follows the store's policy and the store follows the built-in one"},
				cli.IntFlag{Name: "trash-days", Value: -1, Usage: "store only: how many days a deleted commit waits before it is tombstoned"},
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
			fmt.Printf("store default: %s (%s)\ntrash %d days\n", policy.String(), source, w.TrashDays)
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
		fmt.Printf("set %s: %s (%s)\n", name, ret.Effective.String(), source)
		if ret.Paused {
			fmt.Println("retirement is paused since the rebuild; set a policy to resume it")
		}
	})
}

func viewPolicy(p retention.Policy) views.PolicyView {
	out := common.View.Policy(p)
	out.Flags = p.String()

	return out
}

// brackets reads the --keep flags as the stretches of a set's past,
// newest first.
func brackets(flags []string) []retention.Bracket {
	read := make([]retention.Bracket, len(flags))

	for i, flag := range flags {
		b, err := retention.ParseBracket(flag)
		if err != nil {
			common.Fatalf("--keep %s: %v", flag, err)
		}

		read[i] = b
	}

	return read
}

func listAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	idx := repo.Index
	defer idx.Close()

	lister, ok := idx.(maintenance.SetLister)
	if !ok {
		common.Fatalf("Index %T keeps no sets", idx)
	}

	sets, err := lister.ListSets(common.Context(c))
	if err != nil {
		common.Fatal(err)
	}

	now := time.Now()
	out := make([]views.SetView, len(sets))

	for i, s := range sets {
		out[i] = common.View.Set(s)
		out[i].Late = s.Late(now)
	}

	common.Result(out, func() {
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "SET\tSTATE\tEXPECTS\tNEWEST\tLATE")

		for i, s := range sets {
			newest := ""
			if !s.NewestAt.IsZero() {
				newest = s.NewestAt.Local().Format(time.DateTime)
			}

			late := ""
			if out[i].Late {
				late = "late"
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", s.Name, s.State, expects(s.Schedule), newest, late)
		}

		_ = w.Flush()
	})
}

// expects says what an agent reported it expects of a set, empty for
// nothing.
func expects(r *schedule.Reported) string {
	switch {
	case r == nil:
		return ""
	case r.Kind == schedule.Receiver:
		return fmt.Sprintf("receiver every %s", r.CommitEvery)
	case r.Jitter > 0:
		return fmt.Sprintf("%s %s +%s", r.Kind, r.LongestGap, r.Jitter)
	}

	return fmt.Sprintf("%s %s", r.Kind, r.LongestGap)
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
