package pack

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// SessionBeginExt and SessionEndExt name a session's markers in the
// storage: <id>.begin holds the session as it began and <id>.end how it
// ended. An end is only ever created once, so a commit and anything ending
// the session agree on a single outcome. The end is what says a session's
// archives are committed, so it is only deleted after its begin, once no
// archive of the session is stored, live or retired.
const (
	SessionBeginExt = ".begin"
	SessionEndExt   = ".end"
)

type sessionOutcome string

const (
	sessionCommitted sessionOutcome = "committed"
	sessionAborted   sessionOutcome = "aborted"
)

type beginMarker struct {
	ID      string    `json:"id"`
	AgentID string    `json:"agent_id"`
	Set     string    `json:"set"`
	Restore []byte    `json:"restore,omitempty"`
	Started time.Time `json:"started"`
	// Sealed is the newest generation that had left a seal when the
	// session began.
	Sealed uint64 `json:"sealed,omitempty"`
}

func (ps *PackStorage) markBegun(s *backup.Session, sealed uint64) error {
	data, err := json.Marshal(beginMarker{ID: s.ID, AgentID: s.AgentID, Set: s.Set, Restore: s.Restore.GetHash(), Started: s.Started, Sealed: sealed})
	if err != nil {
		return err
	}

	err = ps.storage.CreateNew(s.ID+SessionBeginExt, data)
	if err != nil {
		return fmt.Errorf("marking session %s begun: %w", s.ID, err)
	}

	return nil
}

// markEnded ends the session with want unless it has ended already, and
// returns the outcome it has.
func (ps *PackStorage) markEnded(id string, want sessionOutcome) (sessionOutcome, error) {
	err := ps.storage.CreateNew(id+SessionEndExt, []byte(want))
	if err == nil {
		return want, nil
	}

	if !errors.Is(err, ErrFileExists) {
		return "", fmt.Errorf("marking session %s ended: %w", id, err)
	}

	outcome, _, err := ps.sessionEnd(id)

	return outcome, err
}

// sessionEnd returns the outcome the session ended with, and whether it
// ended.
func (ps *PackStorage) sessionEnd(id string) (sessionOutcome, bool, error) {
	file, err := ps.storage.Open(id + SessionEndExt)
	if notExist(err) {
		return "", false, nil
	}

	if err != nil {
		return "", false, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return "", false, err
	}

	return sessionOutcome(data), true, nil
}

// placeUnknown tells from its session's markers what an archive the index
// does not know is, and whether to load it. Every archive a session that
// committed left is committed, even one whose index file is gone, and only
// an archive of a session that ended without committing is deleted. A live
// session's archive waits for its index file, and for the session to be
// indexed, which reconcileSessions does.
func (ps *PackStorage) placeUnknown(name string) (ArchiveInfo, bool, error) {
	committed := ArchiveInfo{Name: name}

	id := ParsePlacement(name).Session
	if id == "" || ps.markedCommitted(name) {
		return committed, true, nil
	}

	outcome, ended, err := ps.sessionEnd(id)
	if err != nil {
		return ArchiveInfo{}, false, err
	}

	if ended {
		if outcome == sessionCommitted {
			return committed, true, nil
		}

		ps.logger.Info("deleting archive a session left behind", "archive", name, "session", id, "outcome", outcome)
		ps.deleteArchiveFiles(name)

		return ArchiveInfo{}, false, nil
	}

	finalized := ps.hasIndexFile(name)

	if !finalized {
		return ArchiveInfo{}, false, nil
	}

	_, err = ps.index.GetSession(id)
	if err == nil {
		return ArchiveInfo{Name: name, Session: id, State: ArchivePending}, true, nil
	}

	if !errors.Is(err, backup.ErrNoSession) {
		return ArchiveInfo{}, false, err
	}

	// written before sessions left markers
	if !ps.stored(id + SessionBeginExt) {
		return committed, true, nil
	}

	return ArchiveInfo{}, false, nil
}

// reconcileSessions brings the index's sessions in line with the markers:
// an indexed session that has ended is ended, and one that began without
// ending is indexed again if the index lost it, so it expires like any
// other, last seen when it last stored one of the listed archives.
func (ps *PackStorage) reconcileSessions(listed map[string]*ListedFile) error {
	// ends before begins: a collection deletes a session's begin before its
	// end, so a begin listed after its end was missed is one still stored
	ended, err := ps.markerIDs(SessionEndExt)
	if err != nil {
		return err
	}

	begun, err := ps.markerIDs(SessionBeginExt)
	if err != nil {
		return err
	}

	indexed, err := ps.index.ListSessions()
	if err != nil {
		return err
	}

	known := make(map[string]bool, len(indexed))
	for _, s := range indexed {
		known[s.ID] = true

		if ended[s.ID] {
			if err := ps.endSession(s.ID); err != nil {
				return err
			}
		}
	}

	lastStored := make(map[string]time.Time)
	for name, file := range listed {
		if id := ParsePlacement(name).Session; id != "" && file.Modified.After(lastStored[id]) {
			lastStored[id] = file.Modified
		}
	}

	for id := range begun {
		if ended[id] || known[id] {
			continue
		}

		s, err := ps.readBegin(id)
		if notExist(err) {
			// a collection pruned the session's markers since the listing
			continue
		}

		if err != nil {
			return err
		}

		if lastStored[id].After(s.LastSeen) {
			s.LastSeen = lastStored[id]
		}

		ps.logger.Info("indexing a session the index lost", "session", id)

		if err := ps.index.BeginSession(s); err != nil {
			return err
		}
	}

	return nil
}

// pruneSessionMarkers deletes the markers of every session that ended at
// least a claim's limit before now, that no horizon lists and that live
// does not hold, once no archive of the session is stored, live or
// retired, and returns how many sessions it pruned. A writer that stalled
// past the limit finds its claim lapsed and deletes what it stored, so a
// late archive still meets the end that has a rebuild delete it. The begin
// goes first, so a session never looks begun without its end, and a
// session whose begin stays keeps its end.
func (ps *PackStorage) pruneSessionMarkers(now time.Time, live map[string]bool, horizons ...[]string) (int, error) {
	ends, err := ListInfo(ps.storage, SessionEndExt)
	if err != nil {
		return 0, err
	}

	ended := make(map[string]bool, len(ends))
	for _, end := range ends {
		if now.Sub(end.Modified) >= ps.claimLimit() {
			ended[strings.TrimSuffix(end.Name, SessionEndExt)] = true
		}
	}

	if len(ended) == 0 {
		return 0, nil
	}

	archives, err := ps.storage.List(ArchiveSuffix)
	if err != nil {
		return 0, err
	}

	held := make(map[string]bool, len(live))
	for id := range live {
		held[id] = true
	}

	for _, horizon := range horizons {
		for _, id := range horizon {
			held[id] = true
		}
	}

	for _, name := range archives {
		if id := ParsePlacement(strings.TrimSuffix(name, ArchiveSuffix)).Session; id != "" {
			held[id] = true
		}
	}

	pruned := 0

	for id := range ended {
		if held[id] {
			continue
		}

		if err := ps.storage.Delete(id + SessionBeginExt); err != nil && !notExist(err) {
			ps.logger.Warn("deleting the begin of an ended session failed", "session", id, "err", err)
			continue
		}

		if err := ps.storage.Delete(id + SessionEndExt); err != nil && !notExist(err) {
			ps.logger.Warn("deleting the end of an ended session failed", "session", id, "err", err)
			continue
		}

		pruned++
	}

	return pruned, nil
}

func (ps *PackStorage) markerIDs(ext string) (map[string]bool, error) {
	names, err := ps.storage.List(ext)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]bool, len(names))
	for _, name := range names {
		ids[strings.TrimSuffix(name, ext)] = true
	}

	return ids, nil
}

func (ps *PackStorage) readBeginMarker(id string) (*beginMarker, error) {
	file, err := ps.storage.Open(id + SessionBeginExt)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var marker beginMarker
	if err := json.NewDecoder(file).Decode(&marker); err != nil {
		return nil, fmt.Errorf("reading the begin of session %s: %w", id, err)
	}

	return &marker, nil
}

func (ps *PackStorage) readBegin(id string) (*backup.Session, error) {
	marker, err := ps.readBeginMarker(id)
	if err != nil {
		return nil, err
	}

	s := &backup.Session{ID: marker.ID, AgentID: marker.AgentID, Set: marker.Set, Started: marker.Started, LastSeen: marker.Started}
	if len(marker.Restore) > 0 {
		s.Restore = &proto.Ref{Hash: marker.Restore}
	}

	return s, nil
}
