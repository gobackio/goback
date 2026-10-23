// Package postgres holds the commands that back up and restore Postgres
// clusters.
package postgres

import (
	"github.com/urfave/cli"
)

// Command is the postgres command.
var Command = cli.Command{
	Name:  "postgres",
	Usage: "Back up and restore Postgres clusters (beta)",
	Description: "Base backups work with Postgres 9.6 and later. Streaming WAL through a replication slot works with\n" +
		"Postgres 14 and later. Base backups and WAL go into separate sets.",
	Subcommands: []cli.Command{
		baseCmd,
		checkCmd,
		receiveCmd,
		restoreCmd,
		walGetCmd,
	},
}
