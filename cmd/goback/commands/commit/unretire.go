package commit

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

func unretireAction(c *cli.Context) {
	set := c.String("set")
	if (set == "") == (c.NArg() == 0) {
		common.Fatal("usage: commit unretire [--dry-run] (--set <set> [--since <time>] | <ref>...)")
	}

	repo := common.OpenRepo(c)
	idx := repo.Index
	ctx := common.Context(c)

	x, ok := idx.(index.Unretirer)
	if !ok {
		common.Fatalf("Index %T cannot unretire commits", idx)
	}

	var refs []*proto.Ref
	if set != "" {
		since, err := parseSince(c.String("since"))
		if err != nil {
			common.Fatal(err)
		}

		refs, err = x.TombstonedCommits(ctx, set, since)
		if err != nil {
			common.Fatal(err)
		}
	} else {
		for _, arg := range c.Args() {
			refs = append(refs, common.ParseRef(arg))
		}
	}

	var done []index.Unretired
	if len(refs) > 0 {
		var err error
		done, err = x.UnretireCommits(ctx, refs, c.Bool("dry-run"))
		if err != nil {
			common.Fatal(err)
		}
	}

	out := make([]views.UnretiredView, len(done))
	for i, u := range done {
		out[i] = common.View.Unretired(u)
	}

	common.Result(out, func() {
		if len(out) == 0 {
			log.Printf("No tombstoned commits to unretire")
		}

		for _, u := range out {
			log.Print(unretiredLine(u, c.Bool("dry-run")))
		}
	})

	common.CloseRepo(repo)
}

func unretiredLine(u views.UnretiredView, dryRun bool) string {
	if u.Busy {
		return fmt.Sprintf("Commit %s of %s left as it is to another process maintaining the store", u.Commit, u.Set)
	}

	if u.MissingCount > 0 {
		return fmt.Sprintf("Commit %s of %s stays tombstoned: %d objects it reaches are gone, among them %s",
			u.Commit, u.Set, u.MissingCount, strings.Join(u.Missing, ", "))
	}

	verb := "is back"
	if dryRun {
		verb = "would come back"
	}

	if u.RetainedBy == "" {
		return fmt.Sprintf("Commit %s of %s %s, and its set's policy retires it again", u.Commit, u.Set, verb)
	}

	return fmt.Sprintf("Commit %s of %s %s, kept by %s", u.Commit, u.Set, verb, u.RetainedBy)
}

// parseSince reads an RFC 3339 time or a date, the zero time for none.
func parseSince(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}

	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--since %q: want an RFC 3339 time or a date", s)
	}

	return t, nil
}

var unretireCmd = cli.Command{
	Name:      "unretire",
	Usage:     "Bring back tombstoned commits whose objects the store still holds",
	ArgsUsage: "[<ref>...]",
	Action:    unretireAction,
	Flags: []cli.Flag{
		cli.StringFlag{Name: "set", Usage: "unretire this set's tombstoned commits instead of the given refs"},
		cli.StringFlag{Name: "since", Usage: "with --set, unretire only commits tombstoned at or after this RFC 3339 time or date"},
		cli.BoolFlag{Name: "dry-run", Usage: "report what would come back and what is missing without changing anything"},
	},
}
