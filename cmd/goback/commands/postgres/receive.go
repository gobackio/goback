package postgres

import (
	"context"
	"encoding/hex"
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

var receiveCmd = cli.Command{
	Name:  "receive",
	Usage: "stream the cluster's WAL through a replication slot and commit it to the set named by --set until SIGTERM",
	Description: "receive connects as a replica, as libpq does: through --dbname, or through PGHOST, PGUSER and the\n" +
		"other PG* variables. The role needs the REPLICATION attribute, and pg_hba.conf a replication line for it\n" +
		"and a line for a normal connection to the database --dbname names, postgres by default. The slot holds\n" +
		"WAL the receiver hasn't committed; set max_slot_wal_keep_size to bound it. Run --drop-slot when retiring\n" +
		"the receiver.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "spool",
			Usage:  "directory WAL waits in for its commit",
			EnvVar: "GOBACK_POSTGRES_SPOOL",
		},
		baseSetFlag,
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string the receiver connects with",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:  "slot",
			Usage: "the physical replication slot; goback_ and the set's name by default",
		},
		cli.DurationFlag{
			Name:  "commit-every",
			Usage: "how often new WAL is committed; at least 10s",
			Value: postgres.DefaultCommitEvery,
		},
		cli.StringFlag{
			Name:  "commit-bytes",
			Usage: "how much new WAL makes the receiver commit early",
			Value: "256MiB",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; WAL older than the oldest base backup inside it is let go",
			Value: 14 * 24 * time.Hour,
		},
		cli.BoolFlag{
			Name:  "allow-uncapped-slot",
			Usage: "stream even when max_slot_wal_keep_size lets the slot keep WAL until the database's disk is full",
		},
		cli.DurationFlag{
			Name:  "shutdown-grace",
			Usage: "how long the last commit may take after SIGTERM",
			Value: 30 * time.Second,
		},
		cli.BoolFlag{
			Name:  "drop-slot",
			Usage: "drop the replication slot and exit",
		},
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

	return job.PostgresReceive(ctx, env, job.PostgresReceiveSettings{
		Set:               set,
		BaseSet:           c.String("base-set"),
		Spool:             c.String("spool"),
		DBName:            c.String("dbname"),
		Slot:              slot,
		AllowUncappedSlot: c.Bool("allow-uncapped-slot"),
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
