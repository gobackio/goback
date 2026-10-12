package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/proto"
)

// TreeOptions say what RestoreTree restores and tell its caller how it
// goes. Every callback is optional.
type TreeOptions struct {
	// From restores only the subtree at this slash-separated path of the
	// commit.
	From string
	// Delete removes everything under the directory the tree does not
	// hold, except the partial files a later restore resumes from.
	Delete bool
	// Listed is handed the files and bytes the tree holds once it is
	// listed, before anything is written.
	Listed func(files, bytes int64)
	// Written is handed the size of each part as it lands in a file,
	// possibly from several goroutines at once.
	Written func(n int64)
	// Done is called once for every file, possibly from several goroutines
	// at once. An error it returns stops the restore; returning nil for a
	// file that failed passes over it. Without Done, a failed file stops
	// the restore.
	Done func(file RestoredFile) error
	// Removing is called for each path Delete removes, before it is
	// removed, or instead of removing it in a dry run.
	Removing func(path string)
}

// RestoredFile is one file RestoreTree is through with.
type RestoredFile struct {
	Path string
	Ref  *proto.Ref
	Size int64
	// Written is how many of its bytes landed in the file; the rest came
	// from the file already there.
	Written int64
	Outcome Outcome
	Err     error
}

// RestoreTree restores the commit's tree into dir in place: directories,
// symlinks, and files as RestoreFiles does. Each directory stays writable
// for its owner until what it holds is written and then takes the recorded
// mode and time, deepest first. A restore session holds the commit while
// it runs when the store keeps sessions. It refuses a tree naming a path
// outside dir.
func (r *Restorer) RestoreTree(ctx context.Context, commit *proto.Commit, dir string, opts TreeOptions) error {
	reader, err := NewBackupReader(r.Store).WithKey(r.Key).ForCommit(ctx, commit)
	if err != nil {
		return err
	}

	ctx, end, err := HoldRestore(ctx, r.Store, commit.GetBackupSet(), proto.NewObject(commit).Ref())
	if err != nil {
		return err
	}
	defer end()

	tree, parent := commit.GetTree(), []byte(nil)
	if opts.From != "" {
		tree, parent, err = reader.GetTree(ctx, tree, strings.Split(opts.From, "/"))
		if err != nil {
			return err
		}
	}

	entries, files, size, err := listTree(ctx, reader, tree, parent, dir)
	if err != nil {
		return err
	}

	if opts.Listed != nil {
		opts.Listed(files, size)
	}

	if !r.DryRun {
		if err := os.MkdirAll(dir, 0o775); err != nil {
			return err
		}
	}

	if err := r.restoreEntries(ctx, entries, opts); err != nil {
		return err
	}

	if opts.Delete {
		if err := removeUnrestored(dir, entries, r.DryRun, opts.Removing); err != nil {
			return err
		}
	}

	if r.DryRun {
		return nil
	}

	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if !e.info.IsDir() {
			continue
		}

		if err := os.Chmod(e.path, e.info.Mode().Perm()); err != nil {
			return err
		}

		if err := os.Chtimes(e.path, time.Now(), e.info.ModTime()); err != nil {
			return err
		}
	}

	return nil
}

type treeEntry struct {
	path string
	info os.FileInfo
	stat *proto.FileInfo
	ref  *proto.Ref
}

// listTree is every entry of the tree, parents before their children,
// with the files and bytes it holds.
func listTree(ctx context.Context, reader *BackupReader, tree *proto.Ref, parent []byte, dir string) (entries []treeEntry, files, size int64, err error) {
	err = reader.WalkTree(ctx, tree, parent, func(rel string, info os.FileInfo, ref *proto.Ref) error {
		if !filepath.IsLocal(rel) {
			return fmt.Errorf("the tree holds %q, outside the directory restored into", rel)
		}

		stat, _ := info.Sys().(*proto.FileInfo)
		if stat == nil {
			return fmt.Errorf("no stat for %s", rel)
		}

		entries = append(entries, treeEntry{path: filepath.Join(dir, rel), info: info, stat: stat, ref: ref})

		if !info.IsDir() && stat.GetType() != proto.NodeType_NODE_SYMLINK {
			files++
			size += stat.GetSize()
		}

		return nil
	})

	return entries, files, size, err
}

// restoreEntries makes every directory and link and then hands the files
// to RestoreFiles, which takes several at a time.
func (r *Restorer) restoreEntries(ctx context.Context, entries []treeEntry, opts TreeOptions) error {
	var (
		files   []FileToRestore
		written []*atomic.Int64
	)

	for _, e := range entries {
		switch {
		case e.info.IsDir():
			if r.DryRun {
				continue
			}

			if err := restoreDir(e.path, e.info.Mode()); err != nil {
				return err
			}
		case e.stat.GetType() == proto.NodeType_NODE_SYMLINK:
			if r.DryRun {
				continue
			}

			if err := restoreSymlink(e.path, string(e.stat.GetLinkTarget())); err != nil {
				return err
			}
		default:
			landed := &atomic.Int64{}
			written = append(written, landed)
			files = append(files, FileToRestore{Path: e.path, Stat: e.stat, Ref: e.ref, Written: func(n int64) {
				landed.Add(n)

				if opts.Written != nil {
					opts.Written(n)
				}
			}})
		}
	}

	return r.RestoreFiles(ctx, files, func(i int, outcome Outcome, err error) error {
		if opts.Done == nil {
			return err
		}

		f := files[i]

		return opts.Done(RestoredFile{
			Path: f.Path, Ref: f.Ref, Size: f.Stat.GetSize(), Written: written[i].Load(), Outcome: outcome, Err: err,
		})
	})
}

// restoreDir makes path the recorded directory, which its owner can write
// into until RestoreTree sets its mode: whatever else is there, a file or
// a symlink the restore must not write through, is removed.
func restoreDir(path string, mode os.FileMode) error {
	if existing, err := os.Lstat(path); err == nil && !existing.IsDir() {
		if err := os.Remove(path); err != nil {
			return err
		}
	}

	mode = mode.Perm() | 0o300

	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}

	return os.Chmod(path, mode)
}

// restoreSymlink recreates a link as recorded and never follows it.
func restoreSymlink(path, target string) error {
	if existing, err := os.Readlink(path); err == nil && existing == target {
		return nil
	}

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return os.Symlink(target, path)
}

// removeUnrestored deletes everything under dir the restore did not
// write, a directory with all it holds, except the partial files a later
// restore of the same paths resumes from.
func removeUnrestored(dir string, entries []treeEntry, dryRun bool, removing func(string)) error {
	keep := make(map[string]bool, 2*len(entries))
	for _, e := range entries {
		keep[e.path] = true
		keep[PartialPath(e.path)] = true
	}

	folded := make(map[string]string, len(keep))
	for path := range keep {
		folded[strings.ToLower(path)] = path
	}

	// kept reports whether the restore wrote path under any spelling the
	// filesystem treats as the same name
	kept := func(path string) bool {
		if keep[path] {
			return true
		}

		spelled, ok := folded[strings.ToLower(path)]
		if !ok {
			return false
		}

		found, err := os.Lstat(path)
		if err != nil {
			return false
		}

		written, err := os.Lstat(spelled)
		if err != nil {
			return false
		}

		return os.SameFile(found, written)
	}

	var stale []string

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == dir && errors.Is(err, fs.ErrNotExist) {
				return filepath.SkipAll
			}

			return err
		}

		if path == dir || kept(path) {
			return nil
		}

		stale = append(stale, path)

		if d.IsDir() {
			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	for _, path := range stale {
		if removing != nil {
			removing(path)
		}

		if dryRun {
			continue
		}

		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}
