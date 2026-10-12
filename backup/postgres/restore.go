package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

// ErrNoBase is returned when no base backup can be restored to the moment
// asked for.
var ErrNoBase = errors.New("no base backup to restore from")

// Restore writes a base backup into a data directory and, given a WAL set,
// sets the cluster up to recover from it when Postgres starts on it.
type Restore struct {
	Objects backup.ObjectStore
	Key     *storekey.Key
	// Latest finds a set's newest complete commit; it returns
	// backup.ErrNotFound for a set without one.
	Latest  func(ctx context.Context, set string) (*proto.Ref, error)
	BaseSet string
	// WALSet, when empty, restores the base alone: Postgres recovers to the
	// end of the base backup.
	WALSet string
	// At, when set, is the moment recovery stops at; zero recovers to the
	// end of the WAL set.
	At time.Time
	// Hold, when set, keeps the chosen base from being collected while the
	// restore writes it, until the returned func is called.
	Hold func(ctx context.Context, set string, ref *proto.Ref) (context.Context, func(), error)
	// RestoreCommand is the restore_command that fetches the WAL commit's
	// files, with %f and %p for Postgres to fill in.
	RestoreCommand func(walCommit *proto.Ref) string
}

// RestoreResult says what a restore chose.
type RestoreResult struct {
	Base    *proto.Commit
	BaseRef *proto.Ref
	// WAL is the WAL commit recovery reads from; nil when the WAL set has
	// none, and then the cluster recovers only to the end of its base.
	WAL *proto.Ref
}

// Run restores into dir, which must be empty or missing.
func (r *Restore) Run(ctx context.Context, dir string) (*RestoreResult, error) {
	if err := emptyDir(dir); err != nil {
		return nil, err
	}

	walRef, wal, err := r.wal(ctx)
	if err != nil {
		return nil, err
	}

	baseRef, base, err := r.chooseBase(ctx, wal)
	if err != nil {
		return nil, err
	}

	if r.Hold != nil {
		held, release, err := r.Hold(ctx, r.BaseSet, baseRef)
		if err != nil {
			return nil, err
		}
		defer release()

		ctx = held
	}

	if err := r.writeTree(ctx, base, dir); err != nil {
		return nil, err
	}

	result := &RestoreResult{Base: base, BaseRef: baseRef}
	if wal == nil {
		return result, nil
	}

	result.WAL = walRef

	return result, r.configureRecovery(dir, walRef)
}

type walCommit struct {
	commit   *proto.Commit
	systemID uint64
	first    string
	// end is where its WAL ends, its partial segment included; zero when
	// it doesn't say.
	end uint64
}

func (r *Restore) wal(ctx context.Context) (*proto.Ref, *walCommit, error) {
	if r.WALSet == "" {
		return nil, nil, nil
	}

	ref, err := r.Latest(ctx, r.WALSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, err
	}

	commit, err := r.commit(ctx, ref)
	if err != nil {
		return nil, nil, err
	}

	systemID, err := commitSystemID(commit)
	if err != nil {
		return nil, nil, err
	}

	wal := &walCommit{commit: commit, systemID: systemID, first: commit.GetMetadata()[MetaFirstWALFile]}

	if end, ok := commit.GetMetadata()[MetaEndLSN]; ok {
		if wal.end, err = ParseLSN(end); err != nil {
			return nil, nil, fmt.Errorf("the WAL commit's end: %w", err)
		}
	}

	return ref, wal, nil
}

// chooseBase picks the newest base backup of the WAL set's cluster that
// ended before At and whose WAL from its start to its stop the WAL commit
// holds.
func (r *Restore) chooseBase(ctx context.Context, wal *walCommit) (*proto.Ref, *proto.Commit, error) {
	ref, err := r.Latest(ctx, r.BaseSet)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil, fmt.Errorf("%w: the set %s has no commits", ErrNoBase, r.BaseSet)
	}

	if err != nil {
		return nil, nil, err
	}

	var refused []string

	for ref != nil && len(ref.Hash) > 0 {
		commit, err := r.commit(ctx, ref)
		if errors.Is(err, backup.ErrNotFound) {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		if reason := r.unusable(commit, wal); reason != "" {
			refused = append(refused, fmt.Sprintf("%s: %s", time.Unix(commit.GetTimestamp(), 0).UTC().Format(time.RFC3339), reason))
		} else {
			return ref, commit, nil
		}

		ref = commit.GetParent()
	}

	if len(refused) == 0 {
		return nil, nil, ErrNoBase
	}

	return nil, nil, fmt.Errorf("%w; refused:\n  %s", ErrNoBase, strings.Join(refused, "\n  "))
}

func (r *Restore) unusable(base *proto.Commit, wal *walCommit) string {
	if !r.At.IsZero() && time.Unix(base.GetTimestamp()+1, 0).After(r.At) {
		return "ended after the moment asked for"
	}

	start := base.GetMetadata()[MetaStartWALFile]
	if _, ok := walPosition(start); !ok {
		return "records no start WAL file"
	}

	if wal == nil {
		return ""
	}

	systemID, err := commitSystemID(base)
	if err != nil {
		return err.Error()
	}

	if wal.systemID != 0 && systemID != wal.systemID {
		return fmt.Sprintf("of cluster %d, the WAL set of %d", systemID, wal.systemID)
	}

	if !Carry(wal.first)(start) {
		return fmt.Sprintf("starts in %s, before the WAL set's first file %s", start, wal.first)
	}

	// recovery reads a segment from the WAL set before the base's own
	// copy, and a padded partial one ends where the WAL set does
	if stop, err := ParseLSN(base.GetMetadata()[MetaStopLSN]); err == nil && wal.end != 0 && stop > wal.end {
		return fmt.Sprintf("stops at %s, after the WAL set's end at %s", FormatLSN(stop), FormatLSN(wal.end))
	}

	return ""
}

func (r *Restore) commit(ctx context.Context, ref *proto.Ref) (*proto.Commit, error) {
	obj, err := r.Objects.Get(ctx, ref)
	if err != nil {
		return nil, err
	}

	if obj.GetCommit() == nil {
		return nil, fmt.Errorf("object %x is not a commit", ref.Hash)
	}

	return obj.GetCommit(), nil
}

func commitSystemID(commit *proto.Commit) (uint64, error) {
	id, ok := commit.GetMetadata()[MetaSystemID]
	if !ok {
		return 0, nil
	}

	systemID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("system id %q: %w", id, err)
	}

	return systemID, nil
}

func emptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dir, 0o700)
	}

	if err != nil {
		return err
	}

	if len(entries) > 0 {
		return fmt.Errorf("%s is not empty", dir)
	}

	return os.Chmod(dir, 0o700)
}

// writeTree writes the base's data directory into dir.
func (r *Restore) writeTree(ctx context.Context, base *proto.Commit, dir string) error {
	restorer := &backup.Restorer{Store: r.Objects, Key: r.Key}

	reader, err := backup.NewBackupReader(r.Objects).WithKey(r.Key).ForCommit(ctx, base)
	if err != nil {
		return err
	}

	files, fctx := errgroup.WithContext(ctx)
	files.SetLimit(backup.DefaultRestoreWorkers())

	err = reader.WalkTree(ctx, base.GetTree(), nil, func(rel string, info os.FileInfo, ref *proto.Ref) error {
		if !filepath.IsLocal(rel) {
			return fmt.Errorf("the base backup holds %q, outside the data directory", rel)
		}

		path := filepath.Join(dir, rel)
		stat, _ := info.Sys().(*proto.FileInfo)

		switch {
		case info.IsDir():
			if err := os.MkdirAll(path, 0o700); err != nil {
				return err
			}

			return os.Chmod(path, info.Mode().Perm())
		case stat.GetType() == proto.NodeType_NODE_SYMLINK:
			return os.Symlink(string(stat.GetLinkTarget()), path)
		}

		files.Go(func() error {
			_, err := restorer.RestoreFile(fctx, path, stat, ref)
			return err
		})

		return fctx.Err()
	})

	if waited := files.Wait(); err == nil {
		err = waited
	}

	return err
}

// configureRecovery makes Postgres recover from the WAL commit when it
// starts on dir: through recovery.signal and postgresql.auto.conf from
// Postgres 12, through recovery.conf before.
func (r *Restore) configureRecovery(dir string, walRef *proto.Ref) error {
	major, err := majorVersion(dir)
	if err != nil {
		return err
	}

	settings := []string{
		"restore_command = " + confString(r.RestoreCommand(walRef)),
		"recovery_target_action = 'promote'",
	}

	if !r.At.IsZero() {
		settings = append(settings, "recovery_target_time = "+confString(r.At.UTC().Format("2006-01-02 15:04:05.999999+00")))
	}

	name := "postgresql.auto.conf"
	if major < 12 {
		name = "recovery.conf"
		// the default from Postgres 12 on
		settings = append(settings, "recovery_target_timeline = 'latest'")
	}

	conf, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(conf, "\n# written by goback postgres restore\n%s\n", strings.Join(settings, "\n")); err != nil {
		_ = conf.Close()
		return err
	}

	if err := conf.Close(); err != nil {
		return err
	}

	if major < 12 {
		return nil
	}

	return os.WriteFile(filepath.Join(dir, "recovery.signal"), nil, 0o600)
}

// majorVersion reads the major version of the cluster in dir from its
// PG_VERSION: 9 for 9.6, 10 and on as they are.
func majorVersion(dir string) (int, error) {
	content, err := os.ReadFile(filepath.Join(dir, "PG_VERSION"))
	if err != nil {
		return 0, fmt.Errorf("the base backup's version: %w", err)
	}

	text := strings.TrimSpace(string(content))
	major, _, _ := strings.Cut(text, ".")

	n, err := strconv.Atoi(major)
	if err != nil {
		return 0, fmt.Errorf("the base backup's PG_VERSION %q: %w", text, err)
	}

	return n, nil
}

// confString quotes s as a postgresql.conf string.
func confString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// FetchWAL writes the file name of the WAL commit at ref to dst, as
// restore_command does with %f and %p. A segment the commit holds only as
// the newest timeline's partial segment is written padded with zeros to
// its full size. It returns backup.ErrNotFound for a file the commit does
// not hold, which Postgres asks for as a matter of course.
func FetchWAL(ctx context.Context, objects backup.ObjectStore, key *storekey.Key, ref *proto.Ref, name, dst string) error {
	obj, err := objects.Get(ctx, ref)
	if err != nil {
		return err
	}

	commit := obj.GetCommit()
	if commit == nil {
		return fmt.Errorf("%x is not a commit", ref.Hash)
	}

	key, err = backup.CommitKey(ctx, objects, commit, key)
	if err != nil {
		return err
	}

	tree, err := backup.OpenTree(ctx, objects, commit.Tree, key, nil)
	if err != nil {
		return err
	}

	names := make([]string, len(tree.Nodes))
	for i, node := range tree.Nodes {
		names[i] = string(node.Stat.Name)
	}

	pad := false

	i := slices.Index(names, name)
	if i < 0 && servesPartial(names, name) {
		i, pad = slices.Index(names, name+partialSuffix), true
	}

	if i < 0 {
		return fmt.Errorf("%w: %s", backup.ErrNotFound, name)
	}

	restorer := &backup.Restorer{Store: objects, Key: key}
	if _, err := restorer.RestoreFile(ctx, dst, tree.Nodes[i].Stat, tree.Nodes[i].Ref); err != nil {
		return err
	}

	if pad {
		return padSegment(dst)
	}

	return nil
}

// servesPartial reports whether a commit holding names serves segment from
// its partial file: one of the newest timeline, newer than every whole
// segment of it. An older timeline's partial segment ends where the next
// timeline's first segment goes on, and recovery reads that instead.
func servesPartial(names []string, segment string) bool {
	if !isSegment(segment) || !slices.Contains(names, segment+partialSuffix) {
		return false
	}

	newest := uint32(0)
	for _, name := range names {
		if len(name) >= 8 && (isSegment(name) || strings.HasSuffix(name, partialSuffix) || strings.HasSuffix(name, ".history")) {
			if t, err := strconv.ParseUint(name[:8], 16, 32); err == nil {
				newest = max(newest, uint32(t))
			}
		}
	}

	timeline := segment[:8]
	if segmentTimeline(segment) != newest {
		return false
	}

	for _, name := range names {
		if isSegment(name) && name[:8] == timeline && name[8:] > segment[8:] {
			return false
		}
	}

	return true
}

// padSegment extends the partial segment at path with zeros to the size
// its header records: outside standby mode Postgres refuses a restored
// segment of any other size.
func padSegment(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	hdr, err := readSegmentHeader(f)
	if err != nil {
		_ = f.Close()
		return err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	if info.Size() < int64(hdr.segSize) {
		if err := f.Truncate(int64(hdr.segSize)); err != nil {
			_ = f.Close()
			return err
		}
	}

	return f.Close()
}
