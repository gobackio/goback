package object

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

func (o *object) list() {
	out, err := os.Create("objects.csv")
	if err != nil {
		common.Fatal(err)
	}

	writer := csv.NewWriter(out)
	err = writer.Write([]string{"ref", "timestamp", "set"})
	if err != nil {
		common.Fatal(err)
	}

	var count int

	err = o.store.Walk(context.Background(), true, o.objectType, func(obj *proto.Object) error {
		count++

		return writer.Write(
			[]string{
				fmt.Sprintf("%x", obj.Ref().Hash),
				fmt.Sprint(obj.GetCommit().GetTimestamp()),
				obj.GetCommit().GetBackupSet(),
			})
	})

	if err != nil {
		common.Fatal(err)
	}

	writer.Flush()
	err = writer.Error()
	if err != nil {
		common.Fatal(err)
	}

	err = out.Close()
	if err != nil {
		common.Fatal(err)
	}

	common.Result(struct {
		Path    string `json:"path"`
		Objects int    `json:"objects"`
	}{out.Name(), count}, func() {})
}

func listAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	store := repo.Store
	objectType := proto.ObjectType(proto.ObjectType_value[strings.ToUpper(c.String("type"))])

	o := &object{
		store:      store,
		objectType: objectType,
	}

	o.list()
}

var listCmd = cli.Command{
	Name:   "list",
	Usage:  "List all objects",
	Action: listAction,
}
