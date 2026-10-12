package commit

import (
	"log"
	"time"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func (c *commit) list() {
	commits, err := c.index.CommitInfo(c.ctx, c.set, time.Now(), 10)
	if err != nil {
		common.Fatal(errors.Wrap(err, "Failed reading commit info"))
	}

	out := make([]views.CommitView, len(commits))
	for i, commit := range commits {
		out[i] = common.View.Commit(commit)
	}

	common.Result(out, func() {
		for i, commit := range commits {
			note := ""
			if commit.Consistent {
				note = " consistent"
			}

			log.Printf("%s %s%s", time.Unix(commit.Timestamp, 0), out[i].Ref, note)
		}
	})
}

func (c *commit) listTrash() {
	trashed, err := common.GetRetention(c.index).TrashedCommits(c.ctx, c.set, time.Time{}, 0)
	if err != nil {
		common.Fatal(errors.Wrap(err, "Failed reading deleted commits"))
	}

	out := make([]views.TrashedCommitView, len(trashed))
	for i, commit := range trashed {
		out[i] = common.View.TrashedCommit(commit)
	}

	common.Result(out, func() {
		for _, commit := range out {
			log.Printf("%s %s deleted %s, expires %s", commit.Time, commit.Ref, commit.Deleted, commit.Expires)
		}
	})
}

func listAction(c *cli.Context) {
	store := common.GetObjectStore(c)
	index := common.OpenIndex(c, store)

	s := &commit{
		ctx:   common.Context(c),
		index: index,
		set:   c.GlobalString("set"),
	}

	if c.Bool("deleted") {
		s.listTrash()
	} else {
		s.list()
	}

	common.CloseAll(store, index)
}

var listCmd = cli.Command{
	Name:   "list",
	Usage:  "List commits",
	Action: listAction,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "deleted",
			Usage: "list the deleted commits that can still be undeleted",
		},
	},
}
