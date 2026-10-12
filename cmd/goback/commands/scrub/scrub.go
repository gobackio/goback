package scrub

import (
	"context"
	"log"
	"os"
	"sort"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/storage/pack"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli"
)

// Scrubber is implemented by stores that can rehash everything they hold.
type Scrubber interface {
	Scrub(ctx context.Context) (*pack.ScrubReport, error)
}

// Command is the scrub command.
var Command = cli.Command{
	Name:   "scrub",
	Usage:  "Rehash every stored object and report corruption; exits non-zero if any object fails",
	Action: scrubAction,
}

func scrubAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	store := repo.Store

	scrubber, ok := goback.Innermost(store).(Scrubber)
	if !ok {
		common.Fatalf("storage %s cannot be scrubbed", c.GlobalString("storage"))
	}

	report, err := scrubber.Scrub(context.Background())
	if err != nil {
		common.Fatal(err)
	}

	type corrupt struct {
		Archive string `json:"archive"`
		Ref     string `json:"ref"`
		Error   string `json:"error"`
	}

	corrupted := make([]corrupt, len(report.Corrupt))
	for i, f := range report.Corrupt {
		corrupted[i] = corrupt{f.Archive, views.Hex(f.Ref), f.Err.Error()}
	}

	common.Result(struct {
		Archives uint64            `json:"archives"`
		Objects  uint64            `json:"objects"`
		Bytes    uint64            `json:"bytes"`
		Corrupt  []corrupt         `json:"corrupt"`
		Sealed   map[string]uint64 `json:"sealed"`
	}{report.Archives, report.Objects, report.Bytes, corrupted, report.Sealed}, func() {
		log.Printf("Scrubbed %d objects (%s) in %d archives, %d corrupt", report.Objects, humanize.Bytes(report.Bytes), report.Archives, len(report.Corrupt))

		for _, id := range sortedKeys(report.Sealed) {
			if id == "" {
				log.Printf("%d objects stored in the clear", report.Sealed[id])
				continue
			}

			log.Printf("%d objects sealed under key %s", report.Sealed[id], id)
		}
	})

	common.CloseRepo(repo)

	if len(report.Corrupt) > 0 {
		os.Exit(1)
	}
}

func sortedKeys(counts map[string]uint64) []string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
