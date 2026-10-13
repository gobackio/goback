package common

import (
	"strings"

	"github.com/urfave/cli"
)

// MetaFlag is the repeatable --meta key=value flag of the commands that
// write a commit or a pin.
var MetaFlag = cli.StringSliceFlag{
	Name:  "meta",
	Usage: "key=value label to store with the commit or pin; repeatable",
}

// Metadata parses the --meta values; a bare key gets an empty value.
func Metadata(c *cli.Context) map[string]string {
	pairs := c.StringSlice(MetaFlag.Name)
	if len(pairs) == 0 {
		return nil
	}

	out := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, _ := strings.Cut(pair, "=")
		out[key] = value
	}

	return out
}
