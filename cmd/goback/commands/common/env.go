package common

import (
	"log/slog"
	"os"

	"github.com/gobackio/goback/backup/job"

	"github.com/urfave/cli"
)

// Env opens the store, its index and its key the global flags name, or
// exits. It opens no cache, and the programs runs start write to stderr
// as they are. The caller closes the store and the index with CloseAll.
func Env(c *cli.Context) *job.Env {
	store := GetObjectStore(c)
	index := OpenIndex(c, store)

	return &job.Env{
		Store:   store,
		Index:   index,
		Key:     StoreKey(c, store),
		AgentID: AgentID(c),
		Logger:  slog.Default(),
		Stderr:  os.Stderr,
	}
}
