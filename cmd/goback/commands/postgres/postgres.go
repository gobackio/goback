// Package postgres holds the commands that back up and restore Postgres
// clusters.
package postgres

import (
	"github.com/urfave/cli"
)

// Command is the postgres command.
var Command = cli.Command{
	Name:        "postgres",
	Description: "Back up and restore Postgres clusters: base backups from 9.6 on, and from 14 on the WAL streamed through a replication slot, each in a set of its own",
	Subcommands: []cli.Command{
		baseCmd,
		receiveCmd,
		restoreCmd,
		walGetCmd,
	},
}
