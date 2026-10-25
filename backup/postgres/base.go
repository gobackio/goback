package postgres

import (
	archive "archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"

	"github.com/gobackio/goback/backup"
)

// ErrForeignCluster is returned for a base backup of another cluster than
// the set's.
var ErrForeignCluster = errors.New("base backup of another cluster")

// BaseBackup commits one base backup to the base set.
type BaseBackup struct {
	// Walker stores the commit; its Set is the base set. Run sets its
	// Stream and Metadata.
	Walker *backup.Walker
}

// Run stores tar, the output of pg_basebackup -D - -Ft -X fetch, as the
// set's next commit: the data directory as a tree, together with what the
// backup records about itself. wait, when set, is called once tar is read to
// its end and reports how its writer ended; an error from it leaves the set
// without a commit.
func (b *BaseBackup) Run(ctx context.Context, tar io.Reader, wait func() error) (*backup.WalkResult, error) {
	w := b.Walker

	systemID, err := b.previous(ctx)
	if err != nil {
		return nil, err
	}

	described := map[string]*bytes.Buffer{
		"backup_label":      nil,
		"backup_manifest":   nil,
		"global/pg_control": nil,
	}

	ends := &walScan{}

	w.Stream = &backup.Stream{
		Tar: &endOfArchive{r: tar},
		Inspect: func(hdr *archive.Header) io.Writer {
			if dir, name := path.Split(path.Clean(hdr.Name)); (dir == "pg_wal/" || dir == "pg_xlog/") && isSegment(name) {
				return ends.segment(name)
			}

			if _, ok := described[hdr.Name]; !ok {
				return nil
			}

			described[hdr.Name] = &bytes.Buffer{}

			return described[hdr.Name]
		},
		Finish: func() error {
			if wait != nil {
				if err := wait(); err != nil {
					return err
				}
			}

			info, err := describe(described, ends)
			if err != nil {
				return err
			}

			if systemID != 0 && info.SystemID != systemID {
				return fmt.Errorf("%w: the backup is of %d, the set of %d", ErrForeignCluster, info.SystemID, systemID)
			}

			w.Metadata = info.Metadata()

			return nil
		},
	}

	return w.Run(ctx)
}

// previous names the cluster the set's latest commit belongs to; zero for a
// set without one.
func (b *BaseBackup) previous(ctx context.Context) (uint64, error) {
	w := b.Walker

	ref, err := w.Index.LatestCommit(ctx, w.Set)
	if errors.Is(err, backup.ErrNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	obj, err := w.Objects.Get(ctx, ref)
	if err != nil {
		return 0, err
	}

	id, ok := obj.GetCommit().GetMetadata()[MetaSystemID]
	if !ok {
		return 0, nil
	}

	systemID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the base set's system id %q: %w", id, err)
	}

	return systemID, nil
}

// describe reads what a base backup records about itself out of the files
// that hold it. Before Postgres 13 there is no backup_manifest, and where
// the backup stopped is read from the WAL it holds.
func describe(files map[string]*bytes.Buffer, ends *walScan) (BaseInfo, error) {
	var info BaseInfo

	for _, name := range []string{"backup_label", "global/pg_control"} {
		if files[name] == nil {
			return info, fmt.Errorf("the base backup holds no %s", name)
		}
	}

	if err := info.readBackupLabel(files["backup_label"]); err != nil {
		return info, err
	}

	if manifest := files["backup_manifest"]; manifest != nil {
		if err := info.readManifest(manifest); err != nil {
			return info, err
		}
	} else {
		stop, ok := ends.stops[info.StartLSN]
		if !ok {
			return info, fmt.Errorf("the base backup holds no backup_manifest, and its WAL no end of the backup that started at %s", FormatLSN(info.StartLSN))
		}

		info.StopLSN = stop
	}

	return info, info.readControl(files["global/pg_control"])
}

const tarBlock = 512

// endOfArchive completes the end-of-archive marker that pg_basebackup 13 and 14
// cuts short when it writes its manifest into a tar on stdout: a zero block
// and part of a second. Any other stream passes through as it is.
type endOfArchive struct {
	r     io.Reader
	n     int64
	zeros int64
	ended bool
	pad   int64
}

func (e *endOfArchive) Read(p []byte) (int, error) {
	if e.ended {
		n := min(int64(len(p)), e.pad)
		clear(p[:n])
		e.pad -= n

		if e.pad == 0 {
			return int(n), io.EOF
		}

		return int(n), nil
	}

	n, err := e.r.Read(p)
	e.n += int64(n)

	last := n - 1
	for last >= 0 && p[last] == 0 {
		last--
	}

	if last >= 0 {
		e.zeros = int64(n - 1 - last)
	} else {
		e.zeros += int64(n)
	}

	cut := e.n % tarBlock
	if err != io.EOF || cut == 0 || e.zeros < tarBlock+cut {
		return n, err
	}

	e.ended, e.pad = true, tarBlock-cut
	if n > 0 {
		return n, nil
	}

	return e.Read(p)
}
