package commit

import (
	"log"

	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/urfave/cli"
)

func deleteAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: commit delete <ref>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	err := common.GetRetention(index).DeleteCommit(common.Context(c), common.ParseRef(c.Args().First()))
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.Done{Action: "retired", Ref: c.Args().First()}, func() {
		log.Printf("Commit %s retired; undelete it within the trash window", c.Args().First())
	})
	index.Close()
}

func undeleteAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: commit undelete <ref>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	err := common.GetRetention(index).UndeleteCommit(common.Context(c), common.ParseRef(c.Args().First()))
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.Done{Action: "undeleted", Ref: c.Args().First()}, func() {
		log.Printf("Commit %s is live again", c.Args().First())
	})
	index.Close()
}

var deleteCmd = cli.Command{
	Name:      "delete",
	Usage:     "Retire a commit; it can be undeleted until its trash window ends",
	ArgsUsage: "<ref>",
	Action:    deleteAction,
}

var undeleteCmd = cli.Command{
	Name:      "undelete",
	Usage:     "Bring back a retired commit before its trash window ends",
	ArgsUsage: "<ref>",
	Action:    undeleteAction,
}
