// Package mysql holds the commands that back up and restore MySQL servers.
package mysql

import (
	"github.com/gobackio/goback/backup/mysql"

	"github.com/urfave/cli"
)

var clientFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "defaults-extra-file",
		Usage:  "MySQL option file the client programs read before their own, for the connection and its password; ~/.my.cnf alone when empty",
		EnvVar: "GOBACK_MYSQL_DEFAULTS_EXTRA_FILE",
	},
	cli.StringFlag{
		Name:   "mysql",
		Usage:  "the mysql client to run",
		EnvVar: "GOBACK_MYSQL",
		Value:  "mysql",
	},
	cli.StringFlag{
		Name:   "mysqldump",
		Usage:  "the mysqldump to run",
		EnvVar: "GOBACK_MYSQLDUMP",
		Value:  "mysqldump",
	},
	cli.StringFlag{
		Name:   "mysqlbinlog",
		Usage:  "the mysqlbinlog a restore replays with; at least the source server's version",
		EnvVar: "GOBACK_MYSQLBINLOG",
		Value:  "mysqlbinlog",
	},
}

func client(c *cli.Context) *mysql.Exec {
	return &mysql.Exec{
		Mysql:        c.String("mysql"),
		Mysqldump:    c.String("mysqldump"),
		Mysqlbinlog:  c.String("mysqlbinlog"),
		DefaultsFile: c.String("defaults-extra-file"),
	}
}

func withClient(flags ...cli.Flag) []cli.Flag {
	return append(flags, clientFlags...)
}

// Command is the mysql command.
var Command = cli.Command{
	Name:        "mysql",
	Description: "Back up and restore MySQL 5.5 through 8.4 servers: dumps and the binary log, each in a set of its own",
	Subcommands: []cli.Command{
		binlogCmd,
		dumpCmd,
		restoreCmd,
	},
}
