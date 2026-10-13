package object

import (
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

// Command is the object command.
var Command = cli.Command{
	Name:  "object",
	Usage: "Read stored objects by ref",
	Subcommands: []cli.Command{
		listCmd,
		getCmd,
		countCmd,
	},
}

type object struct {
	store      backup.ObjectStore
	objectType proto.ObjectType
	ref        *proto.Ref
	out        string
}
