package postgres

import (
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

var baseCmd = cli.Command{
	Name:  "base",
	Usage: "Take a base backup and commit it to the set named by --set",
	Description: "base connects with --dbname, or with PGHOST, PGUSER and the other PG* variables. The user needs\n" +
		"the REPLICATION attribute. On Postgres 15 and later, goback takes the backup over its own replication\n" +
		"connection. On older versions, or with --pg-basebackup, goback runs pg_basebackup to take it.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string for the backup",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:   "pg-basebackup",
			Usage:  "pg_basebackup program to take the backup with; its major version must match the server's",
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
	env := common.Env(c, repo, goback.Caches{})

	defer func() {
		_ = env.Close()
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
