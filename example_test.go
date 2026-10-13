package goback_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
)

// A program backs a directory up into a store in a local directory and
// restores the commit it made into another.
func ExampleOpen() {
	dir, err := os.MkdirTemp("", "goback-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		log.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(home, "notes.md"), []byte("hello\n"), 0o600); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	repo, err := goback.Open(ctx, goback.Settings{
		Storage: filepath.Join(dir, "store"),
		Index:   filepath.Join(dir, "index"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	env, err := repo.Env(ctx, goback.Caches{})
	if err != nil {
		log.Fatal(err)
	}
	defer env.Close()

	if _, err := job.Path(ctx, env, job.PathSettings{Set: "home", Dir: home, ProgressInterval: -1}); err != nil {
		log.Fatal(err)
	}

	commit, err := backup.PickCommit(ctx, repo.Index, "home", nil, time.Now())
	if err != nil {
		log.Fatal(err)
	}

	restorer := &backup.Restorer{Store: repo.Store, Key: env.Key}

	restored := filepath.Join(dir, "restored")
	if err := restorer.RestoreTree(ctx, commit, restored, backup.TreeOptions{}); err != nil {
		log.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(restored, "notes.md"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(string(data))
	// Output: hello
}
