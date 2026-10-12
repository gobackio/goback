package postgres

import (
	"log"

	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

var baseCmd = cli.Command{
	Name:  "base",
	Usage: "take a base backup and commit it to the set named by --set",
	Description: "base connects as libpq does: through --dbname, or through PGHOST, PGUSER and the other PG*\n" +
		"variables. The user needs the REPLICATION attribute. From Postgres 15 on, goback takes the backup over\n" +
		"a replication connection of its own; before that, or with --pg-basebackup, pg_basebackup takes it.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string the backup connects with",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:   "pg-basebackup",
			Usage:  "take the backup with this pg_basebackup, whose major version must match the server's",
			EnvVar: "GOBACK_POSTGRES_BASEBACKUP",
		},
	},
	Action: baseAction,
}

func baseAction(c *cli.Context) error {
	if c.GlobalString("set") == "" {
		return cli.NewExitError("--set is required", 2)
	}

	// everything that can exit the process comes before pg_basebackup starts:
	// an orphaned pg_basebackup is reparented to PID 1, which in the Postgres
	// container is the postmaster, and its death restarts the cluster
	repo := common.OpenRepo(c)
	env := common.Env(c, repo)

	defer func() {
		common.CloseRepo(repo)
	}()

	result, err := job.PostgresBase(common.Context(c), env, job.PostgresBaseSettings{
		Set:          c.GlobalString("set"),
		DBName:       c.String("dbname"),
		PGBasebackup: c.String("pg-basebackup"),
	})
	if err != nil {
		return err
	}

	common.Result(struct {
		views.WalkView
		WALStart string `json:"wal_start"`
	}{common.View.Walk(result), result.Commit.Metadata[postgres.MetaStartWALFile]}, func() {
		log.Printf("Commit %x: base backup of %d bytes, WAL from %s", result.Ref.Hash, result.Bytes, result.Commit.Metadata[postgres.MetaStartWALFile])
	})

	return nil
}
