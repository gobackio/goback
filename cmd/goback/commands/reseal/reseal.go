package reseal

import (
	"context"
	"errors"
	"log"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/urfave/cli"
)

// Command is the reseal command.
var Command = cli.Command{
	Name:   "reseal",
	Usage:  "Seal the store's blobs again under refs the server can check, replacing the commits above them; rebuild the index into an empty database afterwards",
	Action: resealAction,
}

type replaced struct {
	Old string `json:"old"`
	New string `json:"new"`
}

type failed struct {
	Ref   string `json:"ref"`
	Error string `json:"error"`
}

func resealAction(c *cli.Context) {
	ctx := common.Context(c)

	repo := common.OpenRepo(c)
	defer common.CloseRepo(repo)

	report, err := maintenance.Reseal(ctx, repo.Store, func(ctx context.Context, _ []byte) (*storekey.Key, error) {
		return repo.Key(ctx)
	})
	if errors.Is(err, errors.ErrUnsupported) {
		common.Fatalf("storage %s cannot be resealed", goback.Redacted(c.GlobalString("storage")))
	}

	if err != nil {
		common.Fatal(err)
	}

	commits := make([]replaced, len(report.Resealed))
	for i, pair := range report.Resealed {
		commits[i] = replaced{views.Hex(pair.Old), views.Hex(pair.New)}
	}

	failures := make([]failed, len(report.Failed))
	for i, failure := range report.Failed {
		failures[i] = failed{views.Hex(failure.Ref), failure.Err.Error()}
	}

	common.Result(struct {
		Busy     bool       `json:"busy,omitempty"`
		Resealed []replaced `json:"resealed"`
		Current  int        `json:"current"`
		Blobs    int        `json:"blobs"`
		Pins     int        `json:"pins"`
		Failed   []failed   `json:"failed"`
	}{report.Busy, commits, report.Current, report.Blobs, report.Pins, failures}, func() {
		log.Print(report.Summary())

		for _, failure := range failures {
			log.Printf("left %s as it was: %s", failure.Ref, failure.Error)
		}

		if len(commits) > 0 {
			log.Print("Rebuild the index into an empty database with goback fix")
		}
	})
}
