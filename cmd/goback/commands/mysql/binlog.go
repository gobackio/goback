package mysql

import (
	"log"
	"time"

	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

var binlogCmd = cli.Command{
	Name:  "binlog",
	Usage: "rotate the binary log when it holds anything new, and commit the closed binlogs to the set named by --set",
	Description: "Run it every minute. It reads the binlog files directly, so it runs where they are, as a user\n" +
		"that can read them. After RESET MASTER the binlogs go on in a new chain of the same set.",
	Flags: withClient(
		cli.StringFlag{
			Name:  "dump-set",
			Usage: "set the server's dumps go to",
		},
		cli.StringFlag{
			Name:   "binlog-dir",
			Usage:  "directory the server writes its binlogs to; the directory of @@log_bin_basename, else @@datadir, when empty",
			EnvVar: "GOBACK_MYSQL_BINLOG_DIR",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; binlogs older than the oldest dump inside it are let go",
			Value: 14 * 24 * time.Hour,
		},
	),
	Action: binlogAction,
}

func binlogAction(c *cli.Context) error {
	if c.String("dump-set") == "" || c.GlobalString("set") == "" {
		return cli.NewExitError("--set and --dump-set are required", 2)
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

	result, err := job.MySQLBinlog(common.Context(c), env, job.MySQLBinlogSettings{
		Set:       c.GlobalString("set"),
		Client:    jobClient(c),
		BinlogDir: c.String("binlog-dir"),
		DumpSet:   c.String("dump-set"),
		Window:    c.Duration("window"),
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
		}{}, func() { log.Print("No new closed binlog") })

		return nil
	}

	meta := result.Commit.Metadata

	common.Result(struct {
		views.WalkView
		Chain      string `json:"chain"`
		LastBinlog string `json:"last_binlog"`
	}{common.View.Walk(result), meta[mysql.MetaChain], meta[mysql.MetaLastBinlog]}, func() {
		log.Printf("Commit %x: binlogs of chain %s up to %s", result.Ref.Hash, meta[mysql.MetaChain], meta[mysql.MetaLastBinlog])
	})

	return nil
}
