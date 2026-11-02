package fix

import (
	"log"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

// Command is the fix command.
var Command = cli.Command{
	Name:   "fix",
	Usage:  "Rebuild the index from the objects in the store, for example on a new machine or after losing the index, then check the store holds what the versions reference",
	Action: fixAction,
}

func fixAction(c *cli.Context) {
	ctx := common.Context(c)
	repo := common.OpenRepo(c)
	index := repo.Index

	report, err := index.ReIndex(ctx)
	if err != nil {
		common.Fatal(err)
	}

	var damage *views.DamageView

	if finder, ok := index.(backup.DamageFinder); ok {
		found, err := finder.FindDamage(ctx)
		if err != nil {
			common.Fatal(err)
		}

		view := common.View.Damage(found)
		damage = &view
	}

	common.CloseRepo(repo)

	common.Result(reindexed{Done: common.Done{Action: "reindexed"}, ReIndexView: common.View.ReIndex(report), Damage: damage}, func() {
		if report.Tied > 0 || report.Behind > 0 || report.Unnamed > 0 {
			log.Printf("%d commits shared their set's newest receipt time and were indexed a microsecond after it, %d were received before it and left out, %d name no set and went to a placeholder set",
				report.Tied, report.Behind, report.Unnamed)
		}

		if damage != nil && (damage.Paths > 0 || damage.Lost > 0 || damage.Rescan > 0) {
			log.Printf("the store lost objects the versions reference: %d paths will be read again by the next backups, %d versions are unrecoverable, %d sets will be read in full",
				damage.Paths, damage.Lost, damage.Rescan)
		}
	})
}

// reindexed is what fix prints.
type reindexed struct {
	common.Done
	views.ReIndexView
	Damage *views.DamageView `json:"damage,omitempty"`
}
