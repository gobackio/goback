package mysql

import (
	"encoding/hex"
	"log"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

var receiveCmd = cli.Command{
	Name:  "receive",
	Usage: "Receive the server's binlogs as a replica and commit them to the set named by --set until SIGTERM",
	Description: "receive reads host, port, socket, user, password and ssl-mode from the [client] group of\n" +
		"--defaults-extra-file and ~/.my.cnf. The user needs REPLICATION SLAVE and REPLICATION CLIENT. receive\n" +
		"starts at the newest dump's binlog, so take a dump first. After RESET MASTER, the binlogs continue in\n" +
		"a new chain in the same set.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "spool",
			Usage:  "directory that holds binlogs until they are committed",
			EnvVar: "GOBACK_MYSQL_SPOOL",
		},
		cli.StringFlag{
			Name:  "dump-set",
			Usage: "set that holds the server's dumps",
		},
		cli.StringFlag{
			Name:   "defaults-extra-file",
			Usage:  "MySQL option file with the connection settings and password; read before ~/.my.cnf",
			EnvVar: "GOBACK_MYSQL_DEFAULTS_EXTRA_FILE",
		},
		cli.UintFlag{
			Name:  "server-id",
			Usage: "server id to connect with as a replica; derived from the set's name by default",
		},
		cli.DurationFlag{
			Name:  "commit-every",
			Usage: "how often to commit new binlog; at least 10s",
			Value: mysql.DefaultCommitEvery,
		},
		cli.StringFlag{
			Name:  "commit-bytes",
			Usage: "commit early once this much new binlog has arrived",
			Value: "256MiB",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches at least; binlogs older than the newest dump before it are retired",
			Value: 14 * 24 * time.Hour,
		},
		cli.BoolFlag{
			Name:  "until-caught-up",
			Usage: "commit the binlog the server has written when receive starts, then exit",
		},
		cli.DurationFlag{
			Name:  "shutdown-grace",
			Usage: "how long the last commit may take after SIGTERM",
			Value: 30 * time.Second,
		},
		common.InitialRetentionFlag,
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

	initial := common.InitialRetention(c)

	ctx, stop := common.Graceful(c, c.Duration("shutdown-grace"))

	repo := common.OpenRepo(c)
	env := common.Env(c, repo, goback.Caches{Trees: true})

	defer func() {
		_ = env.Close()
		common.CloseRepo(repo)
	}()

	return job.MySQLReceive(ctx, env, job.MySQLReceiveSettings{
		Set:               set,
		DumpSet:           c.String("dump-set"),
		Spool:             c.String("spool"),
		DefaultsExtraFile: c.String("defaults-extra-file"),
		ServerID:          uint32(c.Uint("server-id")),
		CommitEvery:       c.Duration("commit-every"),
		CommitBytes:       int64(commitBytes),
		Window:            c.Duration("window"),
		InitialRetention:  initial,
		UntilCaughtUp:     c.Bool("until-caught-up"),
	}, stop, func(result *backup.WalkResult) {
		if env.TreeCache != nil {
			if _, err := env.TreeCache.Sweep(); err != nil {
				log.Printf("sweeping the tree cache: %v", err)
			}
		}

		log.Printf("Commit %s: %d files, %d bytes uploaded", hex.EncodeToString(result.Ref.GetHash()), result.Files, result.Uploaded)
	}, nil)
}
