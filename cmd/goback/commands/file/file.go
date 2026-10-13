package file

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"

	"github.com/urfave/cli"
)

type file struct {
	ctx      context.Context
	reader   *backup.BackupReader
	restorer *backup.Restorer
	store    backup.ObjectStore
	key      *storekey.Key
	index    backup.Index
	src      string
	dst      string
	when     time.Time
	set      string
}

// Command is the file command.
var Command = cli.Command{
	Name:  "file",
	Usage: "List, show and restore single files and their versions",
	Subcommands: []cli.Command{
		restoreCmd,
		showCmd,
		lsCmd,
	},
}
