package postgres

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/gobackio/goback/backup"
)

// BaseInfo is what a base backup records about itself.
type BaseInfo struct {
	SystemID     uint64
	Timeline     uint32
	StartLSN     uint64
	StopLSN      uint64
	StartWALFile string
}

// Metadata is the commit metadata a base backup's set carries.
func (i BaseInfo) Metadata() map[string]string {
	return map[string]string{
		MetaSystemID:     strconv.FormatUint(i.SystemID, 10),
		MetaTimeline:     strconv.FormatUint(uint64(i.Timeline), 10),
		MetaStartLSN:     FormatLSN(i.StartLSN),
		MetaStopLSN:      FormatLSN(i.StopLSN),
		MetaStartWALFile: i.StartWALFile,
		backup.MetaKind:  backup.KindPostgresBase,
	}
}

var startWAL = regexp.MustCompile(`^START WAL LOCATION: ([0-9A-F]+/[0-9A-F]+) \(file ([0-9A-F]{24})\)$`)

// readBackupLabel fills in the start of the backup from its backup_label;
// before Postgres 11 the label names no timeline, and the start WAL file's
// name does.
func (i *BaseInfo) readBackupLabel(r io.Reader) error {
	var sawStart, sawTimeline bool

	lines := bufio.NewScanner(r)
	for lines.Scan() {
		line := lines.Text()

		if m := startWAL.FindStringSubmatch(line); m != nil {
			lsn, err := ParseLSN(m[1])
			if err != nil {
				return err
			}

			i.StartLSN, i.StartWALFile, sawStart = lsn, m[2], true
		}

		if rest, ok := strings.CutPrefix(line, "START TIMELINE: "); ok {
			timeline, err := strconv.ParseUint(rest, 10, 32)
			if err != nil {
				return fmt.Errorf("backup_label: timeline %q: %w", rest, err)
			}

			i.Timeline, sawTimeline = uint32(timeline), true
		}
	}

	if err := lines.Err(); err != nil {
		return err
	}

	if !sawStart {
		return errors.New("backup_label names no start WAL location")
	}

	if !sawTimeline {
		timeline, err := strconv.ParseUint(i.StartWALFile[:8], 16, 32)
		if err != nil {
			return fmt.Errorf("backup_label: start WAL file %q: %w", i.StartWALFile, err)
		}

		i.Timeline = uint32(timeline)
	}

	return nil
}

// readManifest fills in where the backup stopped from its backup_manifest.
func (i *BaseInfo) readManifest(r io.Reader) error {
	var manifest struct {
		Ranges []struct {
			Timeline uint32 `json:"Timeline"`
			End      string `json:"End-LSN"`
		} `json:"WAL-Ranges"`
	}

	if err := json.NewDecoder(r).Decode(&manifest); err != nil {
		return fmt.Errorf("backup_manifest: %w", err)
	}

	if len(manifest.Ranges) == 0 {
		return errors.New("backup_manifest lists no WAL range")
	}

	stop, err := ParseLSN(manifest.Ranges[len(manifest.Ranges)-1].End)
	if err != nil {
		return fmt.Errorf("backup_manifest: %w", err)
	}

	i.StopLSN = stop

	return nil
}

// readControl fills in the cluster from global/pg_control, whose first
// field is the system identifier in every version.
func (i *BaseInfo) readControl(r io.Reader) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("global/pg_control: %w", err)
	}

	i.SystemID = binary.LittleEndian.Uint64(buf)

	return nil
}
