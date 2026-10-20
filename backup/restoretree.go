package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// TreeOptions say what RestoreTree restores and tell its caller how it
// goes. Every callback is optional.
type TreeOptions struct {
	// From restores only the subtree at this slash-separated path of the
	// commit.
	From string
	// Paths restores only these entries, slash-separated paths of the
	// commit, files or directories, each at its own path under the
	// directory. It excludes From and Delete.
	Paths []string
	// Name, when set, names each entry Paths names, or without Paths each
	// entry the tree holds directly, given its path in the tree; what a
	// renamed directory holds keeps its names. It excludes Delete.
	Name func(path string, info os.FileInfo) string
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
// mode and time, deepest first; a directory already there that the tree
// does not hold is made writable while the restore runs and then given
// back its mode. A restore session holds the commit while it runs when the
// store keeps sessions. It refuses a tree naming a path outside dir.
func (r *Restorer) RestoreTree(ctx context.Context, commit *proto.Commit, dir string, opts TreeOptions) error {
	if err := r.checkVerify(); err != nil {
		return err
	}

	switch {
	case len(opts.Paths) > 0 && opts.From != "":
		return errors.New("a restore takes either a subtree or paths")
	case opts.Delete && (len(opts.Paths) > 0 || opts.Name != nil):
		return errors.New("a restore of some paths, or under other names, deletes nothing")
	}

	tops, err := topPaths(opts.Paths)
	if err != nil {
		return err
	}

	reader, err := NewBackupReader(r.Store).WithKey(r.Key).ForCommit(ctx, commit)
	if err != nil {
		return err
	}

	ctx, end, err := HoldRestore(ctx, r.Store, commit.GetBackupSet(), proto.NewObject(commit).Ref())
	if err != nil {
		return err
	}
	defer end()

	l := &treeList{fsys: r.fs(), reader: reader, name: opts.Name}

	if len(tops) > 0 {
		err = l.addPaths(ctx, commit.GetTree(), dir, tops)
	} else {
		tree, parent := commit.GetTree(), []byte(nil)
		if opts.From != "" {
			tree, parent, err = reader.GetTree(ctx, tree, strings.Split(opts.From, "/"))
			if err != nil {
				return err
			}
		}

		l.into = []string{dir}
		err = l.add(ctx, tree, parent, dir, "")
	}

	if err != nil {
		return err
	}

	if opts.Listed != nil {
		opts.Listed(l.files, l.size)
	}

	if !r.DryRun {
		o := &opener{fsys: r.fs()}
		defer o.close(ctx)

		for _, into := range l.into {
			o.open(ctx, into)

			if err := r.fs().MkdirAll(ctx, into); err != nil {
				return err
			}
		}
	}

	if err := r.restoreEntries(ctx, l.entries, opts); err != nil {
		return err
	}

	if opts.Delete {
		if err := r.removeUnrestored(ctx, dir, l.entries, opts.Removing); err != nil {
			return err
		}
	}

	if r.DryRun {
		return nil
	}

	for i := len(l.entries) - 1; i >= 0; i-- {
		e := l.entries[i]
		if !e.info.IsDir() || e.stat.GetLost() {
			continue
		}

		if err := r.putStreams(ctx, r.fs(), e.path, e.stat); err != nil {
			return err
		}

		if err := setMeta(ctx, r.fs(), e.path, e.info.Mode().Perm(), e.info.ModTime()); err != nil {
			return err
		}

		if err := r.putSecurity(ctx, r.fs(), e.path, e.stat); err != nil {
			return err
		}
	}

	return nil
}

// topPaths checks the paths a restore names and drops those inside
// another one it names.
func topPaths(paths []string) ([]string, error) {
	var tops []string

	for _, p := range paths {
		if clean := path.Clean("/" + p)[1:]; clean == "" || clean != p {
			return nil, fmt.Errorf("%q is not a path in the commit", p)
		}

		tops = append(tops, p)
	}

	slices.Sort(tops)

	kept := tops[:0]
	for _, p := range tops {
		if len(kept) > 0 && (p == kept[len(kept)-1] || strings.HasPrefix(p, kept[len(kept)-1]+"/")) {
			continue
		}

		kept = append(kept, p)
	}

	return kept, nil
}

type treeEntry struct {
	path string
	info os.FileInfo
	stat *proto.FileInfo
	ref  *proto.Ref
}

// treeList is every entry a restore writes, parents before their children,
// with the files and bytes it holds and the directories already there it
// writes into.
type treeList struct {
	fsys   WriteFS
	reader *BackupReader
	name   func(string, os.FileInfo) string

	entries     []treeEntry
	files, size int64
	into        []string
}

// addPaths lists each of tops at its path under dir.
func (l *treeList) addPaths(ctx context.Context, root *proto.Ref, dir string, tops []string) error {
	for _, top := range tops {
		parts := strings.Split(top, "/")
		parents, base := parts[:len(parts)-1], parts[len(parts)-1]

		ref, token, err := l.reader.GetTree(ctx, root, parents)
		if err != nil {
			return fmt.Errorf("%s is not in the commit: %w", top, err)
		}

		tree, err := OpenTree(ctx, l.reader.store, ref, l.reader.key, token)
		if err != nil {
			return err
		}

		i := slices.IndexFunc(tree.GetNodes(), func(n *proto.TreeNode) bool { return string(n.GetStat().GetName()) == base })
		if i < 0 {
			return fmt.Errorf("%s is not in the commit", top)
		}

		into := dir
		for _, part := range parents {
			into = l.fsys.Join(into, part)
		}

		if !slices.Contains(l.into, into) {
			l.into = append(l.into, into)
		}

		node := tree.GetNodes()[i]

		path, err := l.entry(top, into, node.GetStat(), node.GetRef())
		if err != nil {
			return err
		}

		if node.GetStat().IsDir() && !node.GetStat().GetLost() {
			if err := l.add(ctx, node.GetRef(), NameToken(l.reader.key, token, node.GetStat().GetName()), path, top); err != nil {
				return err
			}
		}
	}

	return nil
}

// add lists everything the tree at ref holds under dir, renaming the
// entries it holds directly when Name is set and named is empty; named is
// the tree's path when it is itself an entry Paths names.
func (l *treeList) add(ctx context.Context, ref *proto.Ref, parent []byte, dir, named string) error {
	renamed := map[string]string{}

	return l.reader.WalkTree(ctx, ref, parent, func(rel string, info os.FileInfo, ref *proto.Ref) error {
		if !filepath.IsLocal(rel) {
			return fmt.Errorf("the tree holds %q, outside the directory restored into", rel)
		}

		stat, _ := info.Sys().(*proto.FileInfo)
		if stat == nil {
			return fmt.Errorf("no stat for %s", rel)
		}

		parts := strings.Split(rel, string(filepath.Separator))

		if l.name != nil && named == "" {
			if len(parts) == 1 {
				name, err := l.rename(parts[0], info)
				if err != nil {
					return err
				}

				renamed[parts[0]] = name
			}

			parts[0] = renamed[parts[0]]
		}

		path := dir
		for _, part := range parts {
			path = l.fsys.Join(path, part)
		}

		l.append(path, info, stat, ref)

		return nil
	})
}

// entry lists the node at the tree path top in the directory into, under
// the name Name gives it, and returns the path it is written at.
func (l *treeList) entry(top, into string, stat *proto.FileInfo, ref *proto.Ref) (string, error) {
	info := proto.GetOSFileInfo(stat)

	name := string(stat.GetName())
	if !filepath.IsLocal(name) {
		return "", fmt.Errorf("the tree holds %q, outside the directory restored into", name)
	}

	if l.name != nil {
		var err error
		if name, err = l.rename(top, info); err != nil {
			return "", err
		}
	}

	path := l.fsys.Join(into, name)
	l.append(path, info, stat, ref)

	return path, nil
}

func (l *treeList) rename(path string, info os.FileInfo) (string, error) {
	name := l.name(path, info)
	if !filepath.IsLocal(name) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%s cannot be restored as %q, which is not a name", path, name)
	}

	return name, nil
}

func (l *treeList) append(path string, info os.FileInfo, stat *proto.FileInfo, ref *proto.Ref) {
	l.entries = append(l.entries, treeEntry{path: path, info: info, stat: stat, ref: ref})

	if !info.IsDir() && stat.GetType() != proto.NodeType_NODE_SYMLINK {
		l.files++
		l.size += stat.GetSize()
	}
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
		case e.stat.GetLost() && e.stat.GetType() != proto.NodeType_NODE_FILE:
			atomic.AddInt64(&r.stats.Lost, 1)

			if opts.Done != nil {
				if err := opts.Done(RestoredFile{Path: e.path, Outcome: OutcomeLost}); err != nil {
					return err
				}
			}
		case e.info.IsDir():
			if r.DryRun {
				continue
			}

			if err := r.restoreDir(ctx, e.path, e.info.Mode()); err != nil {
				return err
			}
		case e.stat.GetType() == proto.NodeType_NODE_SYMLINK:
			if r.DryRun {
				continue
			}

			err := r.restoreSymlink(ctx, e.path, string(e.stat.GetLinkTarget()))
			if errors.Is(err, errors.ErrUnsupported) {
				atomic.AddInt64(&r.stats.Unsupported, 1)
				continue
			}

			if err != nil {
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
func (r *Restorer) restoreDir(ctx context.Context, path string, mode os.FileMode) error {
	fsys := r.fs()

	if existing, err := fsys.Lstat(ctx, path); err == nil && !existing.IsDir() {
		if err := fsys.Remove(ctx, path); err != nil {
			return err
		}
	}

	if err := fsys.MkdirAll(ctx, path); err != nil {
		return err
	}

	if err := fsys.Chmod(ctx, path, mode.Perm()|0o300); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return err
	}

	return nil
}

// restoreSymlink recreates a link as recorded and never follows it. The
// link is made beside path and renamed over it, so a tree that cannot hold
// links loses nothing that is there.
func (r *Restorer) restoreSymlink(ctx context.Context, path, target string) error {
	fsys := r.fs()

	if existing, err := fsys.Readlink(ctx, path); err == nil && existing == target {
		return nil
	}

	partial := r.partial(path)
	if err := removeIfThere(ctx, fsys, partial); err != nil {
		return err
	}

	if err := fsys.Symlink(ctx, target, partial); err != nil {
		return err
	}

	if existing, err := fsys.Lstat(ctx, path); err == nil && existing.IsDir() {
		if err := fsys.Remove(ctx, path); err != nil {
			return err
		}
	}

	return fsys.Rename(ctx, partial, path)
}

// removeUnrestored deletes everything under dir the restore did not
// write, a directory with all it holds, except the partial files a later
// restore of the same paths resumes from.
func (r *Restorer) removeUnrestored(ctx context.Context, dir string, entries []treeEntry, removing func(string)) error {
	fsys := r.fs()

	keep := make(map[string]bool, 2*len(entries))
	for _, e := range entries {
		keep[e.path] = true
		keep[r.partial(e.path)] = true
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

		found, err := fsys.Lstat(ctx, path)
		if err != nil {
			return false
		}

		written, err := fsys.Lstat(ctx, spelled)
		if err != nil {
			return false
		}

		return os.SameFile(found, written)
	}

	var stale []string

	var walk func(string) error
	walk = func(d string) error {
		list, err := fsys.ReadDir(ctx, d)
		if err != nil {
			if d == dir && errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return err
		}

		for _, entry := range list {
			path := fsys.Join(d, entry.Name())

			switch {
			case !kept(path):
				stale = append(stale, path)
			case entry.IsDir():
				if err := walk(path); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := walk(dir); err != nil {
		return err
	}

	for _, path := range stale {
		if removing != nil {
			removing(path)
		}

		if r.DryRun {
			continue
		}

		if err := fsys.RemoveAll(ctx, path); err != nil {
			return err
		}
	}

	return nil
}
