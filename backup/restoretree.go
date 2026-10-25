package backup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
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
	// DeleteIncomplete lets Delete run on a commit or tree marked
	// incomplete, removing what the store lost from the directory too.
	// Without it such a restore fails with ErrIncomplete before it writes
	// anything.
	DeleteIncomplete bool
	// EscapeNames restores an entry whose name this platform cannot hold,
	// as Windows cannot hold one with a colon or a device name, under
	// EscapeName's name, which a Walker records as the original again.
	// Without it such an entry, with all it holds, is left out and handed
	// to Done as OutcomeUnsupportedName.
	EscapeNames bool
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
// store keeps sessions. It refuses a tree naming a path outside dir, and
// on the local disk writes nothing outside dir through a symlink, not even
// one swapped in while it runs.
func (r *Restorer) RestoreTree(ctx context.Context, commit *proto.Commit, dir string, opts TreeOptions) error {
	if err := r.check(); err != nil {
		return err
	}

	switch {
	case len(opts.Paths) > 0 && opts.From != "":
		return errors.New("a restore takes either a subtree or paths")
	case opts.Delete && (len(opts.Paths) > 0 || opts.Name != nil):
		return errors.New("a restore of some paths, or under other names, deletes nothing")
	}

	refuseDelete := opts.Delete && !opts.DeleteIncomplete
	if refuseDelete && commit.GetIncomplete() {
		return ErrIncomplete
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

	l := &treeList{fsys: r.fs(), reader: reader, name: opts.Name, escape: opts.EscapeNames}

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
		err = l.add(ctx, tree, parent, dir, "", "", opts.From)
	}

	if err != nil {
		return err
	}

	if refuseDelete && l.incomplete {
		return ErrIncomplete
	}

	atomic.AddInt64(&r.stats.PartialDirs, l.partial)
	atomic.AddInt64(&r.stats.NamesEscaped, l.escaped)

	if !r.DryRun {
		l.findLinks()
	}

	if opts.Listed != nil {
		opts.Listed(l.files, l.size)
	}

	if !r.DryRun {
		o := &opener{fsys: r.fs()}
		o.open(ctx, dir)

		if err := r.fs().MkdirAll(ctx, dir); err != nil {
			o.close(ctx)
			return err
		}

		release, err := r.confine(dir)
		if err != nil {
			o.close(ctx)
			return err
		}
		defer release()
		// before release, so the modes go back through the root
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

	if err := ctx.Err(); err != nil {
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
		if !e.info.IsDir() || e.stat.GetLost() || e.unsupported {
			continue
		}

		if err := r.putStreams(ctx, r.fs(), e.path, e.stat); err != nil {
			return err
		}

		owned, err := r.putOwner(ctx, r.fs(), e.path, e.stat)
		if err != nil {
			return err
		}

		mode := e.info.Mode()
		if !owned {
			mode &^= os.ModeSetuid
		}

		if err := r.putXattrs(ctx, r.fs(), e.path, e.stat); err != nil {
			return err
		}

		if err := setMeta(ctx, r.fs(), e.path, mode, e.info.ModTime()); err != nil {
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
	// rel is the slash-separated path under the directory restored into,
	// and at its path in the commit
	rel, at string
	info    os.FileInfo
	stat    *proto.FileInfo
	ref     *proto.Ref
	// linkTo is the index of the entry this file is restored as a hard
	// link to, or -1
	linkTo int
	// unsupported marks an entry left out, with all it holds, because
	// this platform cannot hold its name
	unsupported bool
}

// treeList is every entry a restore writes, parents before their children,
// with the files and bytes it holds and the directories already there it
// writes into.
type treeList struct {
	fsys   WriteFS
	reader *BackupReader
	name   func(string, os.FileInfo) string
	escape bool

	entries     []treeEntry
	files, size int64
	into        []string

	// incomplete is whether a tree listed is marked incomplete, and
	// partial counts the directories that lost entries with their names
	incomplete bool
	partial    int64
	// escaped counts the entries listed under EscapeName's name
	escaped int64
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

		node := tree.GetNodes()[i]

		into, under, held := dir, "", true
		for _, part := range parents {
			if err := checkName(part); err != nil {
				return err
			}

			disk, ok := l.diskName(part, false)
			held = held && ok
			into, under = l.fsys.Join(into, disk), relPath(under, disk)
		}

		if !held {
			l.leaveOut(l.fsys.Join(into, base), relPath(under, base), node.GetStat())
			continue
		}

		if !slices.Contains(l.into, into) {
			l.into = append(l.into, into)
		}

		path, rel, ok, err := l.entry(top, into, under, node.GetStat(), node.GetRef())
		if err != nil {
			return err
		}

		if ok && node.GetStat().IsDir() && !node.GetStat().GetLost() {
			if err := l.add(ctx, node.GetRef(), NameToken(l.reader.key, token, node.GetStat().GetName()), path, rel, top, top); err != nil {
				return err
			}
		}
	}

	return nil
}

// add lists everything the tree at ref holds under dir, which is at under
// in the directory restored into and at at in the commit, renaming the
// entries it holds directly when Name is set and named is empty; named is
// the tree's path when it is itself an entry Paths names.
func (l *treeList) add(ctx context.Context, ref *proto.Ref, parent []byte, dir, under, named, at string) error {
	// by depth, the directory written into, its path under the one
	// restored into and the names written there as the platform keeps
	// them; left is the depth of an entry left out, whose subtree goes
	// with it, or -1
	paths, rels, kept, left := []string{dir}, []string{under}, []map[string]bool{{}}, -1

	incomplete, err := l.reader.walkTree(ctx, ref, parent, func(treePath string, depth int, info os.FileInfo, ref *proto.Ref) error {
		stat, _ := info.Sys().(*proto.FileInfo)
		if stat == nil {
			return fmt.Errorf("no stat for %s", treePath)
		}

		name := string(stat.GetName())
		if err := checkName(name); err != nil {
			return err
		}

		if left >= 0 && depth > left {
			return nil
		}

		left, paths, rels, kept = -1, paths[:depth+1], rels[:depth+1], kept[:depth+1]

		// the plain name sorts before those that end in dots or spaces
		key := keptName(name)
		collides := key != name && kept[depth][key]
		kept[depth][key] = true

		var disk string

		if l.name != nil && named == "" && depth == 0 {
			var err error
			if disk, err = l.rename(name, info); err != nil {
				return err
			}
		} else if d, ok := l.diskName(name, collides); ok {
			disk = d
		} else {
			left = depth
			l.leaveOut(l.fsys.Join(paths[depth], name), relPath(rels[depth], name), stat)

			return nil
		}

		written, rel := l.fsys.Join(paths[depth], disk), relPath(rels[depth], disk)
		l.append(written, rel, path.Join(at, filepath.ToSlash(treePath)), info, stat, ref)

		if info.IsDir() {
			paths, rels, kept = append(paths, written), append(rels, rel), append(kept, map[string]bool{})
		}

		return nil
	}, func(string) { l.partial++ })

	l.incomplete = l.incomplete || incomplete || l.partial > 0

	return err
}

// entry lists the node at the tree path top in the directory into, which
// is at under in the directory restored into, under the name Name gives
// it, and returns the path it is written at, its path under the directory
// restored into and false when it is left out.
func (l *treeList) entry(top, into, under string, stat *proto.FileInfo, ref *proto.Ref) (string, string, bool, error) {
	info := proto.GetOSFileInfo(stat)

	name := string(stat.GetName())
	if err := checkName(name); err != nil {
		return "", "", false, err
	}

	disk, ok := name, true

	if l.name != nil {
		var err error
		if disk, err = l.rename(top, info); err != nil {
			return "", "", false, err
		}
	} else if disk, ok = l.diskName(name, false); !ok {
		l.leaveOut(l.fsys.Join(into, name), relPath(under, name), stat)
		return "", "", false, nil
	}

	path, rel := l.fsys.Join(into, disk), relPath(under, disk)
	l.append(path, rel, top, info, stat, ref)

	return path, rel, true, nil
}

// checkName refuses the name of a tree entry that is no single name in a
// directory, so it cannot reach outside the one it is listed in.
func checkName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") ||
		filepath.Separator == '\\' && strings.ContainsRune(name, '\\') {
		return fmt.Errorf("the tree holds an entry named %q, which is not a name", name)
	}

	return nil
}

// holdsName reports whether this platform holds an entry named name, and
// dropsTrailing whether it drops the dots and spaces a name ends in, as
// Windows does.
var (
	holdsName = func(name string) bool {
		return runtime.GOOS != "windows" || WindowsHolds(name)
	}
	dropsTrailing = runtime.GOOS == "windows"
)

// keptName is the name the platform keeps an entry named name under.
func keptName(name string) string {
	if dropsTrailing {
		return strings.TrimRight(name, ". ")
	}

	return name
}

// diskName is the name an entry named name is written under, and false when
// it is left out because this platform cannot hold it, or it collides with
// a sibling once the platform drops what it ends in, and names are not
// escaped.
func (l *treeList) diskName(name string, collides bool) (string, bool) {
	switch {
	case holdsName(name) && !collides:
		return name, true
	case !l.escape:
		return "", false
	}

	l.escaped++

	return EscapeName(name), true
}

// leaveOut lists an entry left out for its name.
func (l *treeList) leaveOut(path, rel string, stat *proto.FileInfo) {
	l.entries = append(l.entries, treeEntry{path: path, rel: rel, info: proto.GetOSFileInfo(stat), stat: stat, linkTo: -1, unsupported: true})
}

// relPath joins the slash-separated path under and the names below it.
func relPath(under string, names ...string) string {
	return path.Join(append([]string{under}, names...)...)
}

func (l *treeList) rename(path string, info os.FileInfo) (string, error) {
	name := l.name(path, info)
	if !filepath.IsLocal(name) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("%s cannot be restored as %q, which is not a name", path, name)
	}

	return name, nil
}

func (l *treeList) append(path, rel, at string, info os.FileInfo, stat *proto.FileInfo, ref *proto.Ref) {
	l.entries = append(l.entries, treeEntry{path: path, rel: rel, at: at, info: info, stat: stat, ref: ref, linkTo: -1})
	l.incomplete = l.incomplete || stat.GetLost()

	if !info.IsDir() && stat.GetType() != proto.NodeType_NODE_SYMLINK {
		l.files++
		l.size += stat.GetSize()
	}
}

// findLinks has each file recorded as a hard link to another file the
// restore writes, with the same content, link to that one, whose bytes are
// then written once.
func (l *treeList) findLinks() {
	files := map[string]int{}

	for i, e := range l.entries {
		if !e.unsupported && restoredAsFile(e.stat) && len(e.stat.GetHardLink()) == 0 {
			files[e.at] = i
		}
	}

	for i := range l.entries {
		e := &l.entries[i]
		if e.unsupported || len(e.stat.GetHardLink()) == 0 || !restoredAsFile(e.stat) {
			continue
		}

		target, ok := files[string(e.stat.GetHardLink())]
		if ok && l.entries[target].ref.Equal(e.ref) {
			e.linkTo = target
			l.size -= e.stat.GetSize()
		}
	}
}

// restoredAsFile reports whether stat is of a file a restore writes.
func restoredAsFile(stat *proto.FileInfo) bool {
	return stat.GetType() == proto.NodeType_NODE_FILE && !stat.GetLost() && !cloudOnly(stat)
}

// restoreEntries makes every directory and symlink, hands the files to
// RestoreFiles, which takes several at a time, and then links each file
// findLinks found to the one it shares content with, or, where that one
// failed or the link cannot be made, writes it as well.
func (r *Restorer) restoreEntries(ctx context.Context, entries []treeEntry, opts TreeOptions) error {
	var (
		files []FileToRestore
		// made holds the directories the restore made, by their parent
		made = map[string][]os.FileInfo{}
		// links are the entries linked to another, and fileOf the index in
		// files of each entry written
		links  []int
		fileOf = map[int]int{}
	)

	for i, e := range entries {
		if e.unsupported {
			atomic.AddInt64(&r.stats.NamesLeftOut, 1)

			if opts.Done != nil {
				if err := opts.Done(RestoredFile{Path: e.path, Outcome: OutcomeUnsupportedName}); err != nil {
					return err
				}
			}

			continue
		}

		if e.stat.GetMetadataLost() && !e.stat.GetLost() {
			atomic.AddInt64(&r.stats.MetadataLost, 1)
		}

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

			if info, err := r.fs().Lstat(ctx, e.path); err == nil {
				parent, _ := r.fs().Split(e.path)
				made[parent] = append(made[parent], info)
			}
		case e.stat.GetType() == proto.NodeType_NODE_SYMLINK:
			parent, _ := r.fs().Split(e.path)

			if opts.Delete {
				if err := r.clearDir(ctx, e.path, made[parent], opts.Removing); err != nil {
					return err
				}
			}

			if r.DryRun {
				continue
			}

			err := r.restoreSymlink(ctx, e.path, e.stat, made[parent])
			if errors.Is(err, errors.ErrUnsupported) {
				atomic.AddInt64(&r.stats.Unsupported, 1)
				continue
			}

			if err != nil {
				return err
			}
		default:
			if opts.Delete {
				parent, _ := r.fs().Split(e.path)
				if err := r.clearDir(ctx, e.path, made[parent], opts.Removing); err != nil {
					return err
				}
			}

			if e.linkTo >= 0 {
				links = append(links, i)
				continue
			}

			fileOf[i] = len(files)
			files = append(files, FileToRestore{Path: e.path, Rel: e.rel, Stat: e.stat, Ref: e.ref})
		}
	}

	written, err := r.restoreAll(ctx, files, opts)
	if err != nil {
		return err
	}

	var copies []FileToRestore

	for _, i := range links {
		e := entries[i]
		target := entries[e.linkTo]

		if written[fileOf[e.linkTo]] {
			outcome, err := r.restoreLink(ctx, target.path, e.path, e.stat)
			if err == nil {
				if opts.Done != nil {
					if err := opts.Done(RestoredFile{Path: e.path, Ref: e.ref, Size: e.stat.GetSize(), Outcome: outcome}); err != nil {
						return err
					}
				}

				continue
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}
		}

		copies = append(copies, FileToRestore{Path: e.path, Rel: e.rel, Stat: e.stat, Ref: e.ref})
	}

	if len(copies) == 0 {
		return nil
	}

	_, err = r.restoreAll(ctx, copies, opts)

	return err
}

// restoreAll restores files as RestoreFiles does, handing each to Done,
// and reports which of them it wrote or found in place.
func (r *Restorer) restoreAll(ctx context.Context, files []FileToRestore, opts TreeOptions) ([]bool, error) {
	written := make([]atomic.Int64, len(files))
	placed := make([]atomic.Bool, len(files))

	for i := range files {
		files[i].Written = func(n int64) {
			written[i].Add(n)

			if opts.Written != nil {
				opts.Written(n)
			}
		}
	}

	err := r.RestoreFiles(ctx, files, func(i int, outcome Outcome, err error) error {
		switch outcome {
		case OutcomeWritten, OutcomeUnchanged, OutcomeSkipped, OutcomeSalvaged:
			placed[i].Store(err == nil)
		}

		if opts.Done == nil {
			return err
		}

		f := files[i]

		return opts.Done(RestoredFile{
			Path: f.Path, Ref: f.Ref, Size: f.Stat.GetSize(), Written: written[i].Load(), Outcome: outcome, Err: err,
		})
	})

	res := make([]bool, len(files))
	for i := range placed {
		res[i] = placed[i].Load()
	}

	return res, err
}

// restoreLink makes path a hard link to target, a file the restore wrote,
// unless it is one already or Overwrite leaves the file there. The link is
// made beside path and renamed over it, so a tree that cannot hold links
// loses nothing that is there.
func (r *Restorer) restoreLink(ctx context.Context, target, path string, stat *proto.FileInfo) (Outcome, error) {
	fsys := r.fs()

	linker, ok := fsys.(LinkWriteFS)
	if !ok {
		return 0, errors.ErrUnsupported
	}

	if existing, err := fsys.Lstat(ctx, path); err == nil {
		if linked, err := fsys.Lstat(ctx, target); err == nil && os.SameFile(existing, linked) {
			r.countFile(ctx, OutcomeUnchanged)
			return OutcomeUnchanged, nil
		}

		if existing.Mode().IsRegular() && r.Overwrite == OverwriteIfChanged && existing.Size() == stat.GetSize() && existing.ModTime().UnixNano() == stat.GetMtimeNs() {
			r.countFile(ctx, OutcomeSkipped)
			return OutcomeSkipped, nil
		}
	}

	partial := r.partial(path)
	if err := removeIfThere(ctx, fsys, partial); err != nil {
		return 0, err
	}

	if err := linker.Link(ctx, target, partial); err != nil {
		return 0, err
	}

	if err := fsys.Rename(ctx, partial, path); err != nil {
		_ = removeIfThere(ctx, fsys, partial)
		return 0, err
	}

	r.countFile(ctx, OutcomeLinked)

	return OutcomeLinked, nil
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

	if err := fsys.Chmod(ctx, path, settable(mode)&^os.ModeSetuid|0o300); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		return err
	}

	return nil
}

// restoreSymlink recreates a link as recorded and never follows it. The
// link is made beside path and renamed over it, so a tree that cannot hold
// links loses nothing that is there. It refuses to replace one of siblings,
// the directories the restore made beside it, which a filesystem folding
// names finds at path.
func (r *Restorer) restoreSymlink(ctx context.Context, path string, stat *proto.FileInfo, siblings []os.FileInfo) error {
	fsys := r.fs()
	target := string(stat.GetLinkTarget())

	if existing, err := fsys.Readlink(ctx, path); err == nil && existing == target {
		_, err := r.putOwner(ctx, fsys, path, stat)
		return err
	}

	if existing, err := fsys.Lstat(ctx, path); err == nil && existing.IsDir() {
		for _, dir := range siblings {
			if os.SameFile(existing, dir) {
				return fmt.Errorf("the link %s and a directory beside it are one name here", path)
			}
		}
	}

	partial := r.partial(path)
	if err := removeIfThere(ctx, fsys, partial); err != nil {
		return err
	}

	if err := fsys.Symlink(ctx, target, partial); err != nil {
		return err
	}

	if _, err := r.putOwner(ctx, fsys, partial, stat); err != nil {
		return err
	}

	if existing, err := fsys.Lstat(ctx, path); err == nil && existing.IsDir() {
		if err := fsys.Remove(ctx, path); err != nil {
			return err
		}
	}

	return fsys.Rename(ctx, partial, path)
}

// clearDir removes, with all it holds, a directory at path where the tree
// has a file or link, unless it is one of siblings, the directories the
// restore made beside path.
func (r *Restorer) clearDir(ctx context.Context, path string, siblings []os.FileInfo, removing func(string)) error {
	fsys := r.fs()

	existing, err := fsys.Lstat(ctx, path)
	if err != nil || !existing.IsDir() {
		return nil
	}

	for _, dir := range siblings {
		if os.SameFile(existing, dir) {
			return fmt.Errorf("%s and a directory beside it are one name here", path)
		}
	}

	if removing != nil {
		removing(path)
	}

	if r.DryRun {
		return nil
	}

	return fsys.RemoveAll(ctx, path)
}

// removeUnrestored deletes everything under dir the restore did not
// write, a directory with all it holds, except the partial files a later
// restore of the same paths resumes from.
func (r *Restorer) removeUnrestored(ctx context.Context, dir string, entries []treeEntry, removing func(string)) error {
	fsys := r.fs()

	keep := make(map[string]bool, 2*len(entries))
	// written holds the paths the restore wrote, by their directory
	written := map[string][]string{}

	for _, e := range entries {
		keep[e.path] = true
		keep[r.partial(e.path)] = true

		parent, _ := fsys.Split(e.path)
		written[parent] = append(written[parent], e.path)
	}

	var stale []string

	// walk lists d, spelled as the restore wrote it. A listed name the
	// restore did not write may still be one it wrote under a spelling the
	// filesystem stores otherwise, by case, Unicode form or trailing dots
	// and spaces; it is the same entry as one written there but not listed.
	var walk func(string) error
	walk = func(d string) error {
		list, err := fsys.ReadDir(ctx, d)
		if err != nil {
			if d == dir && errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return err
		}

		listed := make(map[string]bool, len(list))
		for _, entry := range list {
			listed[fsys.Join(d, entry.Name())] = true
		}

		var (
			aliases     map[string]os.FileInfo
			aliasesRead bool
		)

		spelling := func(path string) (string, bool) {
			if keep[path] {
				return path, true
			}

			if !aliasesRead {
				aliasesRead, aliases = true, map[string]os.FileInfo{}

				for _, w := range written[d] {
					if listed[w] {
						continue
					}

					if info, err := fsys.Lstat(ctx, w); err == nil {
						aliases[w] = info
					}
				}
			}

			if len(aliases) == 0 {
				return "", false
			}

			found, err := fsys.Lstat(ctx, path)
			if err != nil {
				return "", false
			}

			for w, info := range aliases {
				if os.SameFile(found, info) {
					return w, true
				}
			}

			return "", false
		}

		for _, entry := range list {
			path, kept := spelling(fsys.Join(d, entry.Name()))

			switch {
			case !kept:
				stale = append(stale, fsys.Join(d, entry.Name()))
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
		if err := ctx.Err(); err != nil {
			return err
		}

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
