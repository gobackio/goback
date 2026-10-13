package postgres

import (
	"context"
	"encoding/hex"
	"log"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

var receiveCmd = cli.Command{
	Name:  "receive",
	Usage: "Stream the cluster's WAL through a replication slot and commit it to the set named by --set until SIGTERM",
	Description: "receive connects with --dbname, or with PGHOST, PGUSER and the other PG* variables. The role\n" +
		"needs the REPLICATION attribute. pg_hba.conf needs a replication line for the role. It also needs a\n" +
		"line for a normal connection to the database in --dbname, which is postgres by default. The slot\n" +
		"holds the WAL that the receiver has not committed yet. Set max_slot_wal_keep_size to limit it. Run\n" +
		"receive --drop-slot when you retire the receiver.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "spool",
			Usage:  "directory that holds WAL until it is committed",
			EnvVar: "GOBACK_POSTGRES_SPOOL",
		},
		cli.StringFlag{
			Name:  "base-set",
			Usage: "set that holds the cluster's base backups",
		},
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string for the receiver",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:  "slot",
			Usage: "physical replication slot to use; goback_ followed by the set's name by default",
		},
		cli.DurationFlag{
			Name:  "commit-every",
			Usage: "how often to commit new WAL; at least 10s",
			Value: postgres.DefaultCommitEvery,
		},
		cli.StringFlag{
			Name:  "commit-bytes",
			Usage: "commit early once this much new WAL has arrived",
			Value: "256MiB",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; WAL older than the oldest base backup in this window is retired",
			Value: 14 * 24 * time.Hour,
		},
		cli.BoolFlag{
			Name:  "allow-uncapped-slot",
			Usage: "stream even when max_slot_wal_keep_size does not limit the slot, so the slot can keep WAL until the database's disk is full",
		},
		cli.DurationFlag{
			Name:  "shutdown-grace",
			Usage: "how long the last commit may take after SIGTERM",
			Value: 30 * time.Second,
		},
		cli.BoolFlag{
			Name:  "until-caught-up",
			Usage: "commit the WAL the server has flushed when receive starts, then exit",
		},
		cli.BoolFlag{
			Name:  "drop-slot",
			Usage: "drop the replication slot and exit",
		},
		common.InitialRetentionFlag,
	},
	Action: receiveAction,
}

func receiveAction(c *cli.Context) error {
	set := c.GlobalString("set")

	slot := c.String("slot")
	if slot == "" && set != "" {
		slot = postgres.SlotName(set)
	}

	if c.Bool("drop-slot") {
		if slot == "" {
			return cli.NewExitError("--slot or --set is required", 2)
		}

		if err := postgres.DropSlot(context.Background(), c.String("dbname"), slot); err != nil {
			return err
		}

		log.Printf("Dropped the replication slot %s", slot)

		return nil
	}

	if c.String("spool") == "" || c.String("base-set") == "" || set == "" {
		return cli.NewExitError("--set, --spool and --base-set are required", 2)
	}

	commitBytes, err := humanize.ParseBytes(c.String("commit-bytes"))
	if err != nil {
		return cli.NewExitError("--commit-bytes: "+err.Error(), 2)
	}

	initial := common.InitialRetention(c)

	ctx, stop := common.Graceful(c, c.Duration("shutdown-grace"))

	repo := common.OpenRepo(c)
	env := common.Env(c, repo, goback.Caches{Trees: true})

	defer func() {
		_ = env.Close()
		common.CloseRepo(repo)
	}()

	var status *postgres.ReceiverStatus

	err = job.PostgresReceive(ctx, env, job.PostgresReceiveSettings{
		Set:               set,
		BaseSet:           c.String("base-set"),
		Spool:             c.String("spool"),
		DBName:            c.String("dbname"),
		Slot:              slot,
		AllowUncappedSlot: c.Bool("allow-uncapped-slot"),
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
	}, func(s postgres.ReceiverStatus) {
		status = &s
	})
	if err != nil || !c.Bool("until-caught-up") || status == nil {
		return err
	}

	common.Result(status, func() {
		log.Printf("Caught up at %s; the slot keeps %s of WAL", status.Received, humanize.IBytes(uint64(status.RetainedBytes)))
	})

	return nil
}
