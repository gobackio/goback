package commit

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

type commit struct {
	ctx      context.Context
	restorer *backup.Restorer
	store    backup.ObjectStore
	index    backup.Index
	when     time.Time
	ref      *proto.Ref
	base     string
	from     string
	set      string
	delete   bool

	progressInterval time.Duration
}

// Command is the commit command.
var Command = cli.Command{
	Name:        "commit",
	Description: "Back up, list, restore, delete and undelete commits",
	Subcommands: []cli.Command{
		newCmd,
		listCmd,
		restoreCmd,
		deleteCmd,
		undeleteCmd,
		unretireCmd,
	},
}
