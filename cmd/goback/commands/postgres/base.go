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
	Usage: "take a base backup with pg_basebackup and commit it to the set named by --set",
	Description: "pg_basebackup connects as libpq does: through --dbname, or through PGHOST, PGUSER and the\n" +
		"other PG* variables. The user needs the REPLICATION attribute.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string pg_basebackup connects with",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:   "pg-basebackup",
			Usage:  "the pg_basebackup to run; its major version must match the server's",
			EnvVar: "GOBACK_POSTGRES_BASEBACKUP",
			Value:  "pg_basebackup",
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
	env := common.Env(c)

	defer func() {
		common.CloseAll(env.Store, env.Index)
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
