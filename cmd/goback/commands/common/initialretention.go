package common

import (
	"github.com/gobackio/goback/backup/retention"

	"github.com/urfave/cli"
)

// InitialRetentionFlag is the --initial-retention flag of the commands
// whose run may create its set.
var InitialRetentionFlag = cli.StringFlag{
	Name:  "initial-retention",
	Usage: `retention policy the set starts with if this run creates it, as goback set retention shows one, such as "keep-last=1 keep=daily=60 keep=weekly=4"; a set that exists keeps its own`,
}

// InitialRetention parses --initial-retention, exiting on a policy that is
// not valid; nil when the flag is not given.
func InitialRetention(c *cli.Context) *retention.Policy {
	raw := c.String(InitialRetentionFlag.Name)
	if raw == "" {
		return nil
	}

	p, err := retention.ParsePolicy(raw)
	if err != nil {
		Fatalf("--%s: %v", InitialRetentionFlag.Name, err)
	}

	return &p
}
