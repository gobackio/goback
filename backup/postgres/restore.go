package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// ErrNoBase is returned when no base backup can be restored to the moment
// asked for.
var ErrNoBase = errors.New("no base backup to restore from")

// Restore writes a base backup into a data directory and, given a WAL set,
// sets the cluster up to recover from it when Postgres starts on it. The
// owner of the data directory is left to the caller.
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
	// FS is the tree the data directory is written onto; nil is the local
	// disk.
	FS backup.WriteFS
	// RestoreCommand, when set, is the restore_command that fetches the WAL
	// commit's files, with %f and %p for Postgres to fill in. When nil, the
	// WAL recovery reads is copied into the data directory's pg_wal.
	RestoreCommand func(walCommit *proto.Ref) string
	// Workers bounds how many files are written at once; 0 means
	// backup.DefaultRestoreWorkers.
	Workers int
}

// RestoreResult says what a restore chose and did.
type RestoreResult struct {
	Base    *proto.Commit
	BaseRef *proto.Ref
	// WAL is the WAL commit recovery reads from; nil when the WAL set has
	// none, and then the cluster recovers only to the end of its base.
	WAL *proto.Ref
	// Copied names the WAL files copied into pg_wal, as they are named
	// there; empty with a RestoreCommand.
	Copied []string
	// ModeUnset is true when FS could not give the data directory mode
	// 0700, which Postgres needs before it starts on it.
	ModeUnset bool
}

// Run restores into dir, which must be empty or missing.
func (r *Restore) Run(ctx context.Context, dir string) (*RestoreResult, error) {
	modeSet, err := emptyDir(ctx, r.fs(), dir)
	if err != nil {
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

	if err := r.writeTree(ctx, base, dir); err != nil {
		return nil, err
	}

	result := &RestoreResult{Base: base, BaseRef: baseRef, ModeUnset: !modeSet}
	if wal == nil {
		return result, nil
	}

	result.WAL = walRef

	return result, r.configureRecovery(ctx, dir, base, walRef, wal, result)
}

func (r *Restore) fs() backup.WriteFS {
	if r.FS == nil {
		return backup.LocalFS{}
	}

	return r.FS
}

type walCommit struct {
	commit   *proto.Commit
	files    *commitFiles
	systemID uint64
	first    string
	// end is where its WAL ends, its partial segment included; zero when
	// it doesn't say.
	end uint64
	// path is the history of its newest timeline, which recovery follows;
	// nil when it lacks that timeline's history file.
	path history
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

	if wal.files, err = openCommitFiles(ctx, r.Objects, r.Objects, commit, r.Key); err != nil {
		return nil, nil, err
	}

	if wal.path, err = wal.files.history(ctx); err != nil {
		return nil, nil, err
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
	if base.GetIncomplete() {
		return "lost content"
	}

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

	stop, err := ParseLSN(base.GetMetadata()[MetaStopLSN])
	if err != nil {
		return ""
	}

	if wal.path != nil {
		timeline := baseTimeline(base)
		if s, ok := wal.path.span(timeline); !ok || stop > s.end {
			return fmt.Sprintf("stops at %s on timeline %d, off the history of the WAL set's timeline %d", FormatLSN(stop), timeline, wal.path.newest())
		}
	}

	// recovery reads a segment from the WAL set before the base's own
	// copy, and a padded partial one ends where the WAL set does
	if wal.end != 0 && stop > wal.end {
		return fmt.Sprintf("stops at %s, after the WAL set's end at %s", FormatLSN(stop), FormatLSN(wal.end))
	}

	return ""
}

// baseTimeline is the timeline base started on.
func baseTimeline(base *proto.Commit) uint32 {
	if t, err := strconv.ParseUint(base.GetMetadata()[MetaTimeline], 10, 32); err == nil {
		return uint32(t)
	}

	return segmentTimeline(base.GetMetadata()[MetaStartWALFile])
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

// emptyDir makes dir, or checks that it is empty, and gives it mode 0700;
// modeSet is false when the tree holds no modes.
func emptyDir(ctx context.Context, fsys backup.WriteFS, dir string) (modeSet bool, err error) {
	entries, err := fsys.ReadDir(ctx, dir)

	switch {
	case errors.Is(err, fs.ErrNotExist):
		err = fsys.MkdirAll(ctx, dir)
	case err == nil && len(entries) > 0:
		err = fmt.Errorf("%s is not empty", dir)
	}

	if err != nil {
		return false, err
	}

	err = fsys.Chmod(ctx, dir, 0o700)
	if errors.Is(err, errors.ErrUnsupported) {
		return false, nil
	}

	return err == nil, err
}

// writeTree writes the base's data directory into dir, refusing a tree
// that cannot hold its links.
func (r *Restore) writeTree(ctx context.Context, base *proto.Commit, dir string) error {
	restorer := &backup.Restorer{Store: r.Objects, Key: r.Key, FS: r.FS, Workers: r.Workers}
	if err := restorer.RestoreTree(ctx, base, dir, backup.TreeOptions{}); err != nil {
		return err
	}

	if restorer.Stats().Unsupported == 0 {
		return nil
	}

	links, err := r.links(ctx, base)
	if err != nil {
		return err
	}

	return fmt.Errorf("the data directory needs the links %s, which the tree it is written onto cannot hold", strings.Join(links, ", "))
}

// links lists the symlinks of base's tree.
func (r *Restore) links(ctx context.Context, base *proto.Commit) ([]string, error) {
	reader, err := backup.NewBackupReader(r.Objects).WithKey(r.Key).ForCommit(ctx, base)
	if err != nil {
		return nil, err
	}

	var links []string

	err = reader.WalkTree(ctx, base.GetTree(), nil, func(path string, info os.FileInfo, _ *proto.Ref) error {
		if stat, _ := info.Sys().(*proto.FileInfo); stat.GetType() == proto.NodeType_NODE_SYMLINK {
			links = append(links, filepath.ToSlash(path))
		}

		return nil
	})

	return links, err
}

// configureRecovery makes Postgres recover from the WAL commit when it
// starts on dir: through recovery.signal and postgresql.auto.conf from
// Postgres 12, through recovery.conf before. Without a RestoreCommand, it
// copies the WAL into pg_wal first.
func (r *Restore) configureRecovery(ctx context.Context, dir string, base *proto.Commit, walRef *proto.Ref, wal *walCommit, result *RestoreResult) error {
	top, err := openCommitFiles(ctx, r.Objects, r.Objects, base, r.Key)
	if err != nil {
		return err
	}

	version, err := top.read(ctx, "PG_VERSION")
	if err != nil {
		return fmt.Errorf("the base backup's version: %w", err)
	}

	major, err := majorVersion(version)
	if err != nil {
		return err
	}

	// from 12 on, archive recovery needs a restore_command; one that always
	// fails makes Postgres read each file from pg_wal
	command := "exit 1"
	if r.RestoreCommand != nil {
		command = r.RestoreCommand(walRef)
	} else if result.Copied, err = r.copyWAL(ctx, dir, base, walRef, wal, major); err != nil {
		return err
	}

	settings := []string{
		"restore_command = " + confString(command),
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

	conf, err := top.read(ctx, name)
	if err != nil && !errors.Is(err, backup.ErrNotFound) {
		return err
	}

	conf = fmt.Appendf(conf, "\n# written by goback postgres restore\n%s\n", strings.Join(settings, "\n"))

	fsys := r.fs()
	if _, err := fsys.WriteFile(ctx, fsys.Join(dir, name), bytes.NewReader(conf)); err != nil {
		return err
	}

	if major < 12 {
		return nil
	}

	_, err = fsys.WriteFile(ctx, fsys.Join(dir, "recovery.signal"), bytes.NewReader(nil))

	return err
}

// majorVersion reads the major version of a cluster from its PG_VERSION:
// 9 for 9.6, 10 and on as they are.
func majorVersion(content []byte) (int, error) {
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
// not hold, which Postgres asks for as a matter of course, and for a
// segment off the history of the commit's newest timeline, which recovery
// follows: an older timeline's past where that history leaves it.
func FetchWAL(ctx context.Context, objects backup.ObjectStore, key *storekey.Key, ref *proto.Ref, name, dst string) error {
	obj, err := objects.Get(ctx, ref)
	if err != nil {
		return err
	}

	commit := obj.GetCommit()
	if commit == nil {
		return fmt.Errorf("%x is not a commit", ref.Hash)
	}

	ctx, end, err := backup.HoldRestore(ctx, objects, commit.GetBackupSet(), ref)
	if err != nil {
		return err
	}
	defer end()

	tree, err := openCommitFiles(ctx, objects, objects, commit, key)
	if err != nil {
		return err
	}

	names := tree.names
	pad := false

	i := slices.Index(names, name)
	if i < 0 && servesPartial(names, name) {
		i, pad = slices.Index(names, name+partialSuffix), true
	}

	if i >= 0 && isSegment(name) && segmentTimeline(name) != newestTimeline(names) {
		h, err := tree.history(ctx)
		if err != nil {
			return err
		}

		if !tree.onHistory(h, i) {
			i = -1
		}
	}

	if i < 0 {
		return fmt.Errorf("%w: %s", backup.ErrNotFound, name)
	}

	return tree.restoreWAL(ctx, backup.LocalFS{}, dst, i, pad)
}

// servesPartial reports whether a commit holding names serves segment from
// its partial file: one of the newest timeline, newer than every whole
// segment of it. An older timeline's partial segment ends where the next
// timeline's first segment goes on, and recovery reads that instead.
func servesPartial(names []string, segment string) bool {
	if !isSegment(segment) || !slices.Contains(names, segment+partialSuffix) {
		return false
	}

	timeline := segment[:8]
	if segmentTimeline(segment) != newestTimeline(names) {
		return false
	}

	for _, name := range names {
		if isSegment(name) && name[:8] == timeline && name[8:] > segment[8:] {
			return false
		}
	}

	return true
}
