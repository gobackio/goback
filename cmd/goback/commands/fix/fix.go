package fix

import (
	"log"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

// Command is the fix command.
var Command = cli.Command{
	Name:   "fix",
	Usage:  "Rebuild the index from the objects the store holds, as on a new machine or after the index is lost",
	Action: fixAction,
}

func fixAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	index := repo.Index

	report, err := index.ReIndex(common.Context(c))
	if err != nil {
		common.Fatal(err)
	}

	common.CloseRepo(repo)

	common.Result(reindexed{Done: common.Done{Action: "reindexed"}, ReIndexView: common.View.ReIndex(report)}, func() {
		if report.Tied > 0 || report.Behind > 0 || report.Unnamed > 0 {
			log.Printf("%d commits shared their set's newest receipt time and were indexed a microsecond after it, %d were received before it and left out, %d name no set and went to a placeholder set",
				report.Tied, report.Behind, report.Unnamed)
		}
	})
}

// reindexed is what fix prints.
type reindexed struct {
	common.Done
	views.ReIndexView
}
