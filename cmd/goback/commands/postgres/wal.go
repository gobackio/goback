package postgres

import (
	"log"
	"time"

	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/urfave/cli"
)

var baseSetFlag = cli.StringFlag{
	Name:  "base-set",
	Usage: "set the cluster's base backups go to",
}

var walCmd = cli.Command{
	Name:  "wal",
	Usage: "commit the spooled WAL files to the set named by --set, then delete them from the spool",
	Flags: []cli.Flag{
		spoolFlag,
		baseSetFlag,
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; WAL older than the oldest base backup inside it is let go",
			Value: 14 * 24 * time.Hour,
		},
	},
	Action: walAction,
}

func walAction(c *cli.Context) error {
	if c.String("spool") == "" || c.String("base-set") == "" || c.GlobalString("set") == "" {
		return cli.NewExitError("--set, --spool and --base-set are required", 2)
	}

	env := common.Env(c)

	defer func() {
		common.CloseAll(env.Store, env.Index)
	}()

	if dir := common.StoreCache(c, "backup"); dir != "" {
		var err error
		if env.TreeCache, err = job.OpenTreeCache(dir); err != nil {
			return err
		}
	}

	result, err := job.PostgresWAL(common.Context(c), env, job.PostgresWALSettings{
		Set:     c.GlobalString("set"),
		Spool:   c.String("spool"),
		BaseSet: c.String("base-set"),
		Window:  c.Duration("window"),
	})
	if err != nil {
		return err
	}

	if env.TreeCache != nil {
		if _, err := env.TreeCache.Sweep(); err != nil {
			log.Printf("sweeping the tree cache: %v", err)
		}
	}

	if result == nil {
		common.Result(struct {
			Files int64 `json:"files"`
		}{}, func() { log.Print("The spool is empty") })

		return nil
	}

	common.Result(common.View.Walk(result), func() { log.Printf("Commit %x: %d spooled files", result.Ref.Hash, result.Files) })

	return nil
}
