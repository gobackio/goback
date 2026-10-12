// Package pin manages pins: objects that keep a commit, tree or file
// alive regardless of retention.
package pin

import (
	"log"
	"time"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

// Command is the pin command.
var Command = cli.Command{
	Name:        "pin",
	Description: "Keep commits, trees or files regardless of retention",
	Subcommands: []cli.Command{
		{
			Name:      "add",
			Usage:     "Pin a ref",
			ArgsUsage: "<ref>",
			Action:    addAction,
			Flags:     []cli.Flag{common.MetaFlag},
		},
		{
			Name:   "list",
			Usage:  "List the live pins",
			Action: listAction,
		},
		{
			Name:      "remove",
			Usage:     "Tombstone a pin",
			ArgsUsage: "<pin-ref>",
			Action:    removeAction,
		},
	},
}

func addAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: pin add <ref>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	pin := proto.NewObject(&proto.Pin{Target: common.ParseRef(c.Args().First()), Metadata: common.Metadata(c)})

	err := index.Put(common.Context(c), pin)
	if err != nil {
		common.Fatal(err)
	}

	common.Result(struct {
		Pin    string `json:"pin"`
		Target string `json:"target"`
	}{views.Hex(pin.Ref()), c.Args().First()}, func() { log.Printf("Pinned %s as %x", c.Args().First(), pin.Ref().Hash) })
	index.Close()
}

func listAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	index := repo.Index

	pins, err := common.GetRetention(index).Pins(common.Context(c))
	if err != nil {
		common.Fatal(err)
	}

	out := make([]views.PinView, len(pins))
	for i, pin := range pins {
		out[i] = common.View.Pin(pin)
	}

	common.Result(out, func() {
		for _, pin := range pins {
			log.Printf("%x -> %x (%s)", pin.Ref.Hash, pin.Target.Hash, time.Unix(0, pin.ReceivedAtNs).Format(time.RFC3339))
		}
	})

	index.Close()
}

func removeAction(c *cli.Context) {
	if c.NArg() != 1 {
		common.Fatal("usage: pin remove <pin-ref>")
	}

	repo := common.OpenRepo(c)
	index := repo.Index

	err := common.GetRetention(index).Unpin(common.Context(c), common.ParseRef(c.Args().First()))
	if err != nil {
		common.Fatal(err)
	}

	common.Result(common.Done{Action: "removed", Ref: c.Args().First()}, func() { log.Printf("Pin %s removed", c.Args().First()) })
	index.Close()
}
