package mysql

import (
	"slices"

	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/urfave/cli"
)

var checkCmd = cli.Command{
	Name:  "check",
	Usage: "List what the server lacks for dumps and, with --set, for receiving binlogs into that set",
	Description: "check connects as dump and receive do, and exits with status 1 when something is missing.",
	Flags: withClient(
		cli.UintFlag{
			Name:  "server-id",
			Usage: "server id receive would connect with as a replica; derived from the set's name by default",
		},
		cli.BoolFlag{
			Name:  "no-binlog-position",
			Usage: "check for dumps that skip recording the binary log position",
		},
	),
	Action: checkAction,
}

func checkAction(c *cli.Context) error {
	if c.Uint("server-id") > 1<<32-1 {
		return cli.NewExitError("--server-id must fit in 32 bits", 2)
	}

	ctx, file := common.Context(c), c.String("defaults-extra-file")

	problems := mysql.CheckDump(ctx, file, !c.Bool("no-binlog-position"))

	if set := c.GlobalString("set"); set != "" {
		id := uint32(c.Uint("server-id"))
		if id == 0 {
			id = mysql.ReplicaServerID(set)
		}

		for _, p := range mysql.Check(ctx, file, id) {
			if !slices.Contains(problems, p) {
				problems = append(problems, p)
			}
		}
	}

	return common.Problems(problems)
}
