package mysql

import (
	"encoding/hex"
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

var receiveCmd = cli.Command{
	Name:  "receive",
	Usage: "receive the server's binlogs as a replica and commit them to the set named by --set until SIGTERM",
	Description: "receive connects as MySQL's option files say: the [client] group of --defaults-extra-file and\n" +
		"~/.my.cnf, for host, port, socket, user, password and ssl-mode. The user needs REPLICATION SLAVE and\n" +
		"REPLICATION CLIENT. It starts at the newest dump's binlog, so take a dump first. After RESET MASTER\n" +
		"the binlogs go on in a new chain of the same set.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "spool",
			Usage:  "directory binlogs wait in for their commit",
			EnvVar: "GOBACK_MYSQL_SPOOL",
		},
		cli.StringFlag{
			Name:  "dump-set",
			Usage: "set the server's dumps go to",
		},
		cli.StringFlag{
			Name:   "defaults-extra-file",
			Usage:  "MySQL option file read before ~/.my.cnf, for the connection and its password",
			EnvVar: "GOBACK_MYSQL_DEFAULTS_EXTRA_FILE",
		},
		cli.UintFlag{
			Name:  "server-id",
			Usage: "replica server id to connect with; one derived from the set's name by default",
		},
		cli.DurationFlag{
			Name:  "commit-every",
			Usage: "how often new binlog is committed; at least 10s",
			Value: mysql.DefaultCommitEvery,
		},
		cli.StringFlag{
			Name:  "commit-bytes",
			Usage: "how much new binlog makes the receiver commit early",
			Value: "256MiB",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; binlogs older than the oldest dump inside it are let go",
			Value: 14 * 24 * time.Hour,
		},
		cli.DurationFlag{
			Name:  "shutdown-grace",
			Usage: "how long the last commit may take after SIGTERM",
			Value: 30 * time.Second,
		},
	},
	Action: receiveAction,
}

func receiveAction(c *cli.Context) error {
	set := c.GlobalString("set")

	if c.String("spool") == "" || c.String("dump-set") == "" || set == "" {
		return cli.NewExitError("--set, --spool and --dump-set are required", 2)
	}

	commitBytes, err := humanize.ParseBytes(c.String("commit-bytes"))
	if err != nil {
		return cli.NewExitError("--commit-bytes: "+err.Error(), 2)
	}

	if c.Uint("server-id") > 1<<32-1 {
		return cli.NewExitError("--server-id must fit in 32 bits", 2)
	}

	ctx, stop := common.Graceful(c, c.Duration("shutdown-grace"))

	env := common.Env(c)

	defer func() {
		common.CloseAll(env.Store, env.Index)
	}()

	if dir := common.StoreCache(c, "backup"); dir != "" {
		if env.TreeCache, err = job.OpenTreeCache(dir); err != nil {
			return err
		}
	}

	return job.MySQLReceive(ctx, env, job.MySQLReceiveSettings{
		Set:               set,
		DumpSet:           c.String("dump-set"),
		Spool:             c.String("spool"),
		DefaultsExtraFile: c.String("defaults-extra-file"),
		ServerID:          uint32(c.Uint("server-id")),
		CommitEvery:       c.Duration("commit-every"),
		CommitBytes:       int64(commitBytes),
		Window:            c.Duration("window"),
	}, stop, func(result *backup.WalkResult) {
		if env.TreeCache != nil {
			if _, err := env.TreeCache.Sweep(); err != nil {
				log.Printf("sweeping the tree cache: %v", err)
			}
		}

		log.Printf("Commit %s: %d files, %d bytes uploaded", hex.EncodeToString(result.Ref.GetHash()), result.Files, result.Uploaded)
	}, nil)
}
