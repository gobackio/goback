package mysql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
)

// ErrNoDump is returned when no dump can be restored to the moment asked
// for.
var ErrNoDump = errors.New("no dump to restore from")

// Restore loads a dump into an empty server and, given a binlog set,
// replays the binlogs after it.
type Restore struct {
	Objects backup.ObjectStore
	Key     *storekey.Key
	// Latest finds a set's newest commit; it returns backup.ErrNotFound for
	// a set without one.
	Latest  func(ctx context.Context, set string) (*proto.Ref, error)
	DumpSet string
	// BinlogSet, when empty, restores the dump alone.
	BinlogSet string
	// At, when set, is the moment the restore stops at; zero replays to the
	// end of the binlog set.
	At     time.Time
	Client Client
	// WorkDir is where the binlogs are fetched to before the replay.
	WorkDir string
	// To, when set, only writes the dump and the binlogs into this empty
	// directory, for the caller to apply.
	To string
	// FS is the tree To names a directory of; nil is the local disk.
	FS backup.WriteFS
	// Force loads into a server that holds databases already.
	Force bool
}

// RestoreResult says what a restore chose and did.
type RestoreResult struct {
	Dump    *proto.Commit
	DumpRef *proto.Ref
	// Binlog is the binlog commit the binlogs come from; nil without a
	// binlog set or when it has no commits.
	Binlog       *proto.Ref
	BinlogFormat string
	// Binlogs names the binlogs replayed, in order.
	Binlogs []string
	// ReplayArgs are mysqlbinlog's arguments; empty when nothing is
	// replayed. With To, they name the binlogs as FS names them.
	ReplayArgs []string
	// DumpPath is where a restore with To wrote the dump, as FS names it.
	DumpPath string
}

// Run restores into the server, or into To.
func (r *Restore) Run(ctx context.Context) (*RestoreResult, error) {
	if r.To != "" {
		if err := emptyDir(ctx, r.fs(), r.To); err != nil {
			return nil, err
		}
	}

	var binlogs *binlogCommit

	result := &RestoreResult{}

	if r.BinlogSet != "" {
		ref, err := r.Latest(ctx, r.BinlogSet)
		switch {
		case errors.Is(err, backup.ErrNotFound):
		case err != nil:
			return nil, err
		default:
			if binlogs, err = readBinlogCommit(ctx, r.Objects, r.Key, ref); err != nil {
				return nil, err
			}

			result.Binlog = ref
			result.BinlogFormat = binlogs.meta[MetaBinlogFormat]
		}
	}

	ref, dump, plan, err := r.chooseDump(ctx, binlogs)
	if err != nil {
		return nil, err
	}

	result.Dump, result.DumpRef = dump, ref

	if r.To == "" {
		if err := r.checkTarget(ctx, len(plan.files) > 0); err != nil {
			return nil, err
		}
	}

	if err := r.load(ctx, dump, ref, result); err != nil {
		return nil, err
	}

	if len(plan.files) == 0 {
		return result, nil
	}

	return result, r.replay(ctx, dump, plan, result)
}

// replayPlan is what a dump replays after it.
type replayPlan struct {
	files []*proto.TreeNode
	// key and token open the chain directory's names and files
	key *storekey.Key
}

// chooseDump picks the newest dump that ended a second or more before At
// and, given binlogs, that the binlog set continues within the dump's chain.
func (r *Restore) chooseDump(ctx context.Context, binlogs *binlogCommit) (*proto.Ref, *proto.Commit, replayPlan, error) {
	ref, err := r.Latest(ctx, r.DumpSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil, replayPlan{}, fmt.Errorf("%w: the set %s has no commits", ErrNoDump, r.DumpSet)
	}

	if err != nil {
		return nil, nil, replayPlan{}, err
	}

	var refused []string

	for ref != nil && len(ref.Hash) > 0 {
		commit, err := getCommit(ctx, r.Objects, ref)
		if errors.Is(err, backup.ErrNotFound) {
			break
		}

		if err != nil {
			return nil, nil, replayPlan{}, err
		}

		files, reason := r.unusable(commit, binlogs)
		if reason == "" {
			plan := replayPlan{files: files}
			if binlogs != nil {
				plan.key = binlogs.key
			}

			return ref, commit, plan, nil
		}

		refused = append(refused, fmt.Sprintf("%s: %s", time.Unix(commit.GetTimestamp(), 0).UTC().Format(time.RFC3339), reason))
		ref = commit.GetParent()
	}

	if len(refused) == 0 {
		return nil, nil, replayPlan{}, ErrNoDump
	}

	return nil, nil, replayPlan{}, fmt.Errorf("%w; refused:\n  %s", ErrNoDump, strings.Join(refused, "\n  "))
}

// unusable says why dump can't be restored to At with binlogs, or returns
// the binlogs to replay after it.
func (r *Restore) unusable(dump *proto.Commit, binlogs *binlogCommit) ([]*proto.TreeNode, string) {
	if !r.At.IsZero() && time.Unix(dump.GetTimestamp()+1, 0).After(r.At) {
		return nil, "ended after the moment asked for"
	}

	if binlogs == nil {
		return nil, ""
	}

	meta := dump.GetMetadata()

	start := meta[MetaBinlogFile]
	if _, _, ok := splitBinlog(start); !ok {
		return nil, "records no binlog position"
	}

	if _, err := strconv.ParseUint(meta[MetaBinlogPos], 10, 64); err != nil {
		return nil, fmt.Sprintf("binlog position %q", meta[MetaBinlogPos])
	}

	id, _ := identityOf(meta, start)
	if binlogs.hasID && !binlogs.identity.Same(id) {
		return nil, fmt.Sprintf("of %s, the binlog set of %s", id, binlogs.identity)
	}

	chain := chainOf(meta)

	files := binlogs.chains[chain]
	if len(files) == 0 {
		return nil, fmt.Sprintf("the binlog set holds nothing of its chain %d", chain)
	}

	first := string(files[0].Stat.Name)
	if compareBinlogs(start, first) < 0 {
		return nil, fmt.Sprintf("starts in %s, before the first binlog %s of its chain %d", start, first, chain)
	}

	i := slices.IndexFunc(files, func(n *proto.TreeNode) bool { return compareBinlogs(string(n.Stat.Name), start) >= 0 })
	if i < 0 {
		// the binlog set holds nothing written after the dump yet
		return nil, ""
	}

	if compareBinlogs(string(files[i].Stat.Name), start) != 0 {
		return nil, gap(start, string(files[i].Stat.Name), true).Error()
	}

	// a dump's start file closes after the dump began; one that closed
	// before is another chain's file of the same name
	if _, partial := isPartial(string(files[i].Stat.Name)); !partial && files[i].Stat.GetMtimeNs() < dump.GetScanStartNs()-int64(time.Second) {
		return nil, fmt.Sprintf("its start file %s in chain %d closed before the dump began: the dump is of another chain", start, chain)
	}

	var replay []*proto.TreeNode

	reached := r.At.IsZero()
	for j := i; j < len(files); j++ {
		if j > i {
			prev := string(files[j-1].Stat.Name)
			if binlog, partial := isPartial(prev); partial {
				return nil, fmt.Sprintf("the binlog set lacks the end of %s", binlog)
			}

			if !follows(prev, string(files[j].Stat.Name)) {
				return nil, gap(prev, string(files[j].Stat.Name), false).Error()
			}
		}

		replay = append(replay, files[j])

		if !r.At.IsZero() && files[j].Stat.GetMtimeNs() >= r.At.UnixNano() {
			reached = true
			break
		}
	}

	if !reached && chain != binlogs.chain {
		return nil, fmt.Sprintf("its chain %d ended before the moment asked for", chain)
	}

	return replay, ""
}

// checkTarget refuses a server that holds databases of its own, and one
// without mysqlbinlog when there are binlogs to replay.
func (r *Restore) checkTarget(ctx context.Context, replay bool) error {
	if replay {
		if _, err := r.Client.BinlogVersion(ctx); err != nil {
			return fmt.Errorf("there are binlogs to replay, but no mysqlbinlog: %w", err)
		}
	}

	if r.Force {
		return nil
	}

	rows, err := r.Client.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return err
	}

	var found []string
	for _, row := range rows {
		switch row[0] {
		case "mysql", "information_schema", "performance_schema", "sys", "test":
		default:
			found = append(found, row[0])
		}
	}

	if len(found) > 0 {
		return fmt.Errorf("the server holds the databases %s; restore into an empty server, or pass --force", strings.Join(found, ", "))
	}

	return nil
}

// load streams the dump into the server, or writes it into To.
func (r *Restore) load(ctx context.Context, dump *proto.Commit, ref *proto.Ref, result *RestoreResult) error {
	ctx, end, err := backup.HoldRestore(ctx, r.Objects, r.DumpSet, ref)
	if err != nil {
		return err
	}
	defer end()

	reader, err := backup.NewBackupReader(r.Objects).WithKey(r.Key).ForCommit(ctx, dump)
	if err != nil {
		return err
	}

	key, err := backup.CommitKey(ctx, r.Objects, dump, r.Key)
	if err != nil {
		return err
	}

	tree, err := backup.OpenTree(ctx, r.Objects, dump.GetTree(), key, nil)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(tree.Nodes, func(n *proto.TreeNode) bool { return string(n.Stat.Name) == DumpFile })
	if i < 0 {
		return fmt.Errorf("the dump commit holds no %s", DumpFile)
	}

	content, err := reader.ReadFile(ctx, tree.Nodes[i].Ref)
	if err != nil {
		return err
	}

	if r.To == "" {
		return r.Client.Load(ctx, content)
	}

	fsys := r.fs()
	result.DumpPath = fsys.Join(r.To, DumpFile)

	return writeFile(ctx, fsys, result.DumpPath, content)
}

// replay fetches the planned binlogs and replays them from the dump's
// position up to At.
func (r *Restore) replay(ctx context.Context, dump *proto.Commit, plan replayPlan, result *RestoreResult) error {
	ctx, end, err := backup.HoldRestore(ctx, r.Objects, r.BinlogSet, result.Binlog)
	if err != nil {
		return err
	}
	defer end()

	fsys, dir := r.fs(), r.To
	if dir == "" {
		fsys = backup.LocalFS{}

		var err error
		if dir, err = os.MkdirTemp(r.WorkDir, "goback-binlogs-"); err != nil {
			return err
		}
		defer os.RemoveAll(dir)
	}

	restorer := &backup.Restorer{Store: r.Objects, Key: plan.key, FS: fsys}

	paths := make([]string, 0, len(plan.files))
	for _, node := range plan.files {
		// a partial binlog replays under its binlog's name
		name, _ := isPartial(string(node.Stat.Name))
		if !filepath.IsLocal(name) {
			return fmt.Errorf("the binlog set holds %q", name)
		}

		path := fsys.Join(dir, name)
		if _, err := restorer.RestoreFile(ctx, path, node.Stat, node.Ref); err != nil {
			return fmt.Errorf("fetching %s: %w", name, err)
		}

		paths = append(paths, path)
		result.Binlogs = append(result.Binlogs, name)
	}

	result.ReplayArgs = ReplayArgs(dump.GetMetadata(), r.At, paths)

	if r.To != "" {
		return nil
	}

	return r.Client.Replay(ctx, result.ReplayArgs)
}

// ReplayArgs are mysqlbinlog's arguments for replaying files after the dump
// whose metadata is meta, up to at when it is set.
func ReplayArgs(meta map[string]string, at time.Time, files []string) []string {
	args := []string{"--start-position=" + meta[MetaBinlogPos]}

	if !at.IsZero() {
		args = append(args, "--stop-datetime="+at.UTC().Format(time.DateTime))
	}

	if meta[MetaGTIDMode] == "ON" {
		args = append(args, "--skip-gtids")
	}

	return append(args, files...)
}

// readBinlogCommit reads the chains the binlog commit at ref holds.
func readBinlogCommit(ctx context.Context, objects backup.Getter, key *storekey.Key, ref *proto.Ref) (*binlogCommit, error) {
	commit, err := getCommit(ctx, objects, ref)
	if err != nil {
		return nil, err
	}

	key, err = backup.CommitKey(ctx, objects, commit, key)
	if err != nil {
		return nil, err
	}

	root, err := backup.OpenTree(ctx, objects, commit.GetTree(), key, nil)
	if err != nil {
		return nil, err
	}

	held := &binlogCommit{meta: commit.GetMetadata(), chain: chainOf(commit.GetMetadata()), chains: map[int][]*proto.TreeNode{}, key: key}
	held.identity, held.hasID = identityOf(held.meta, held.meta[MetaFirstBinlog])

	for _, node := range root.Nodes {
		n, ok := parseChainDir(string(node.Stat.Name))
		if !ok || !node.Stat.IsDir() {
			continue
		}

		tree, err := backup.OpenTree(ctx, objects, node.Ref, key, backup.NameToken(key, nil, node.Stat.Name))
		if err != nil {
			return nil, fmt.Errorf("the binlog set's %s: %w", node.Stat.Name, err)
		}

		files := slices.Clone(tree.Nodes)
		slices.SortFunc(files, func(a, b *proto.TreeNode) int { return compareBinlogs(string(a.Stat.Name), string(b.Stat.Name)) })
		held.chains[n] = files
	}

	return held, nil
}

func getCommit(ctx context.Context, objects backup.Getter, ref *proto.Ref) (*proto.Commit, error) {
	obj, err := objects.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	if obj.GetCommit() == nil {
		return nil, fmt.Errorf("object %x is not a commit", ref.Hash)
	}

	return obj.GetCommit(), nil
}

func (r *Restore) fs() backup.WriteFS {
	if r.FS == nil {
		return backup.LocalFS{}
	}

	return r.FS
}

// emptyDir checks that dir is empty, or makes it for its owner only, as
// far as the tree holds modes.
func emptyDir(ctx context.Context, fsys backup.WriteFS, dir string) error {
	entries, err := fsys.ReadDir(ctx, dir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := fsys.MkdirAll(ctx, dir); err != nil {
			return err
		}

		if err := fsys.Chmod(ctx, dir, 0o700); err != nil && !errors.Is(err, errors.ErrUnsupported) {
			return err
		}

		return nil
	}

	if err != nil {
		return err
	}

	if len(entries) > 0 {
		return fmt.Errorf("%s is not empty", dir)
	}

	return nil
}

// writeFile writes content to path, refusing a file there.
func writeFile(ctx context.Context, fsys backup.WriteFS, path string, content io.Reader) error {
	if _, err := fsys.Lstat(ctx, path); !errors.Is(err, fs.ErrNotExist) {
		if err == nil {
			err = &fs.PathError{Op: "write", Path: path, Err: fs.ErrExist}
		}

		return err
	}

	_, err := fsys.WriteFile(ctx, path, content)

	return err
}
