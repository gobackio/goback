package object

import (
	"context"
	"encoding/hex"
	"log"
	"os"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

func (o *object) get() {
	obj, err := o.store.Get(context.Background(), o.ref)
	if err != nil {
		common.Fatal(err)
	}

	err = os.WriteFile(o.out, obj.Bytes(), 0666)
	if err != nil {
		common.Fatal(err)
	}

	common.Result(struct {
		Ref   string `json:"ref"`
		Type  string `json:"type"`
		Parts int    `json:"parts,omitempty"`
		Path  string `json:"path"`
		Bytes int    `json:"bytes"`
	}{views.Hex(o.ref), obj.Type().String(), len(obj.GetFile().GetParts()), o.out, len(obj.Bytes())}, func() {
		log.Printf("Found object %s", obj.Type())

		if obj.Type() == proto.ObjectType_FILE {
			log.Printf("%d parts", len(obj.GetFile().Parts))
		}
	})
}

func getAction(c *cli.Context) {
	hash, err := hex.DecodeString(c.Args().Get(0))
	out := c.Args().Get(1)

	if err != nil {
		common.Fatal(err)
	}

	repo := common.OpenRepo(c)
	store := repo.Store

	o := &object{
		store: store,
		ref:   &proto.Ref{Hash: hash},
		out:   out,
	}

	o.get()
}

var getCmd = cli.Command{
	Name:      "get",
	Usage:     "Write an object to a file",
	ArgsUsage: "<ref> <file>",
	Action:    getAction,
}
