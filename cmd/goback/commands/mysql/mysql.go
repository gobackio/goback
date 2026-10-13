// Package mysql holds the commands that back up and restore MySQL servers.
package mysql

import (
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/mysql"

	"github.com/urfave/cli"
)

var clientFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "defaults-extra-file",
		Usage:  "MySQL option file with the connection settings and password; the client programs read it before their own option files; without it, they read only ~/.my.cnf",
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
		Usage:  "the mysqldump program to run",
		EnvVar: "GOBACK_MYSQLDUMP",
		Value:  "mysqldump",
	},
	cli.StringFlag{
		Name:   "mysqlbinlog",
		Usage:  "the mysqlbinlog program that restore replays binlogs with; its version must be at least the source server's",
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

func jobClient(c *cli.Context) job.MySQLClient {
	return job.MySQLClient{
		DefaultsExtraFile: c.String("defaults-extra-file"),
		Mysql:             c.String("mysql"),
		Mysqldump:         c.String("mysqldump"),
	}
}

func withClient(flags ...cli.Flag) []cli.Flag {
	return append(flags, clientFlags...)
}

// Command is the mysql command.
var Command = cli.Command{
	Name:        "mysql",
	Usage:       "Back up and restore MySQL servers",
	Description: "goback works with MySQL 5.5 through 8.4. Dumps and the binary log go into separate sets.",
	Subcommands: []cli.Command{
		receiveCmd,
		dumpCmd,
		restoreCmd,
	},
}
