// Package postgres holds the commands that back up and restore Postgres
// clusters.
package postgres

import (
	"github.com/urfave/cli"
)

var spoolFlag = cli.StringFlag{
	Name:   "spool",
	Usage:  "directory archive_command copies WAL files into and the WAL backup commits from",
	EnvVar: "GOBACK_POSTGRES_SPOOL",
}

// Command is the postgres command.
var Command = cli.Command{
	Name:        "postgres",
	Description: "Back up and restore Postgres 9.6 through 17 clusters: base backups and the WAL archive, each in a set of its own",
	Subcommands: []cli.Command{
		archiveCmd,
		baseCmd,
		receiveCmd,
		restoreCmd,
		walCmd,
		walGetCmd,
	},
}
