package postgres

import (
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/urfave/cli"
)

var checkCmd = cli.Command{
	Name:  "check",
	Usage: "List what the cluster lacks for base backups and, with --set or --slot, for streaming WAL",
	Description: "check connects as base and receive do, and exits with status 1 when something is missing. With\n" +
		"--set, it checks the slot receive would use for that WAL set.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:   "dbname",
			Usage:  "connection string to check",
			EnvVar: "GOBACK_POSTGRES_DBNAME",
		},
		cli.StringFlag{
			Name:  "slot",
			Usage: "physical replication slot receive would use; goback_ followed by the set's name by default",
		},
		cli.BoolFlag{
			Name:  "allow-uncapped-slot",
			Usage: "accept a slot that max_slot_wal_keep_size does not limit",
		},
	},
	Action: checkAction,
}

func checkAction(c *cli.Context) error {
	slot := c.String("slot")
	if set := c.GlobalString("set"); slot == "" && set != "" {
		slot = postgres.SlotName(set)
	}

	return common.Problems(postgres.Check(common.Context(c), c.String("dbname"), slot, c.Bool("allow-uncapped-slot")))
}
