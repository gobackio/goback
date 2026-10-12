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

	tree, err := openWALTree(ctx, r.Objects, r.Objects, commit, r.Key)
	if err != nil {
		return nil, nil, err
	}

	if wal.path, err = tree.history(ctx); err != nil {
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
	return (&backup.Restorer{Store: r.Objects, Key: r.Key}).RestoreTree(ctx, base, dir, backup.TreeOptions{})
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

	tree, err := openWALTree(ctx, objects, objects, commit, key)
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

		// a whole segment's size is the cluster's segment size
		if size := uint64(tree.nodes[i].Stat.GetSize()); h != nil && size > 0 && !h.holds(name, size) {
			i = -1
		}
	}

	if i < 0 {
		return fmt.Errorf("%w: %s", backup.ErrNotFound, name)
	}

	restorer := &backup.Restorer{Store: objects, Key: tree.key}
	if _, err := restorer.RestoreFile(ctx, dst, tree.nodes[i].Stat, tree.nodes[i].Ref); err != nil {
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
