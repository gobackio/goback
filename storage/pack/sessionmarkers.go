package pack

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// SessionMarkers keeps what each session of a store began as and how it
// ended, which tells a rebuild without the index which archives committed.
// An end is only ever recorded once, so a commit and anything ending the
// session agree on a single outcome.
type SessionMarkers interface {
	// MarkBegun records the session as it began.
	MarkBegun(begin SessionBegin) error
	// MarkEnded ends the session with outcome unless it has ended, and
	// returns the outcome it has.
	MarkEnded(id string, outcome SessionOutcome) (SessionOutcome, error)
	// SessionEnd returns the outcome the session ended with, and whether
	// it ended.
	SessionEnd(id string) (SessionOutcome, bool, error)
	// SessionBegin returns the session as it began, ErrFileNotFound when
	// nothing records it.
	SessionBegin(id string) (*SessionBegin, error)
	// Begun lists the sessions recorded as begun.
	Begun() (map[string]bool, error)
	// Ended lists the sessions that ended, with when.
	Ended() (map[string]time.Time, error)
	// DeleteMarkers forgets the session, its begin before its end.
	DeleteMarkers(id string) error
}

// SessionOutcome is how a session ended.
type SessionOutcome string

// The outcomes a session ends with.
const (
	SessionCommitted SessionOutcome = "committed"
	SessionAborted   SessionOutcome = "aborted"
)

// SessionBegin is a session as it began.
type SessionBegin struct {
	ID      string    `json:"id"`
	AgentID string    `json:"agent_id"`
	Set     string    `json:"set"`
	Restore []byte    `json:"restore,omitempty"`
	Started time.Time `json:"started"`
	// Sealed is the newest generation that had left a seal when the
	// session began.
	Sealed uint64 `json:"sealed,omitempty"`
}

// bucketMarkers keeps the markers in the storage as <id>.begin and
// <id>.end.
type bucketMarkers struct {
	storage ArchiveStorage
}

func (m bucketMarkers) MarkBegun(begin SessionBegin) error {
	data, err := json.Marshal(begin)
	if err != nil {
		return err
	}

	return m.storage.CreateNew(begin.ID+SessionBeginExt, data)
}

func (m bucketMarkers) MarkEnded(id string, want SessionOutcome) (SessionOutcome, error) {
	err := m.storage.CreateNew(id+SessionEndExt, []byte(want))
	if err == nil {
		return want, nil
	}

	if !errors.Is(err, ErrFileExists) {
		return "", err
	}

	outcome, _, err := m.SessionEnd(id)

	return outcome, err
}

func (m bucketMarkers) SessionEnd(id string) (SessionOutcome, bool, error) {
	file, err := m.storage.Open(id + SessionEndExt)
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

	return SessionOutcome(data), true, nil
}

func (m bucketMarkers) SessionBegin(id string) (*SessionBegin, error) {
	file, err := m.storage.Open(id + SessionBeginExt)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var begin SessionBegin
	if err := json.NewDecoder(file).Decode(&begin); err != nil {
		return nil, fmt.Errorf("reading the begin of session %s: %w", id, err)
	}

	return &begin, nil
}

func (m bucketMarkers) Begun() (map[string]bool, error) {
	names, err := m.storage.List(SessionBeginExt)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]bool, len(names))
	for _, name := range names {
		ids[strings.TrimSuffix(name, SessionBeginExt)] = true
	}

	return ids, nil
}

func (m bucketMarkers) Ended() (map[string]time.Time, error) {
	ends, err := ListInfo(m.storage, SessionEndExt)
	if err != nil {
		return nil, err
	}

	ended := make(map[string]time.Time, len(ends))
	for _, end := range ends {
		ended[strings.TrimSuffix(end.Name, SessionEndExt)] = end.Modified
	}

	return ended, nil
}

func (m bucketMarkers) DeleteMarkers(id string) error {
	if err := m.storage.Delete(id + SessionBeginExt); err != nil && !notExist(err) {
		return err
	}

	if err := m.storage.Delete(id + SessionEndExt); err != nil && !notExist(err) {
		return err
	}

	return nil
}

// routedMarkers keeps the markers of some sessions in bucket and of the
// rest in other, where MarkBegun records them; the store records a session
// it keeps in the bucket there itself. A session's later markers go where
// its begin went.
type routedMarkers struct {
	bucket SessionMarkers
	other  SessionMarkers
}

func (m routedMarkers) MarkBegun(begin SessionBegin) error {
	return m.other.MarkBegun(begin)
}

// of is where the session's markers are: other once it holds either.
func (m routedMarkers) of(id string) (SessionMarkers, error) {
	_, err := m.other.SessionBegin(id)
	if err == nil {
		return m.other, nil
	}

	if !notExist(err) {
		return nil, err
	}

	_, ended, err := m.other.SessionEnd(id)
	if err != nil {
		return nil, err
	}

	if ended {
		return m.other, nil
	}

	return m.bucket, nil
}

func (m routedMarkers) MarkEnded(id string, want SessionOutcome) (SessionOutcome, error) {
	markers, err := m.of(id)
	if err != nil {
		return "", err
	}

	return markers.MarkEnded(id, want)
}

func (m routedMarkers) SessionEnd(id string) (SessionOutcome, bool, error) {
	outcome, ended, err := m.other.SessionEnd(id)
	if err != nil || ended {
		return outcome, ended, err
	}

	return m.bucket.SessionEnd(id)
}

func (m routedMarkers) SessionBegin(id string) (*SessionBegin, error) {
	begin, err := m.other.SessionBegin(id)
	if !notExist(err) {
		return begin, err
	}

	return m.bucket.SessionBegin(id)
}

func (m routedMarkers) Begun() (map[string]bool, error) {
	begun, err := m.bucket.Begun()
	if err != nil {
		return nil, err
	}

	others, err := m.other.Begun()
	if err != nil {
		return nil, err
	}

	for id := range others {
		begun[id] = true
	}

	return begun, nil
}

func (m routedMarkers) Ended() (map[string]time.Time, error) {
	ended, err := m.bucket.Ended()
	if err != nil {
		return nil, err
	}

	others, err := m.other.Ended()
	if err != nil {
		return nil, err
	}

	for id, at := range others {
		ended[id] = at
	}

	return ended, nil
}

func (m routedMarkers) DeleteMarkers(id string) error {
	markers, err := m.of(id)
	if err != nil {
		return err
	}

	return markers.DeleteMarkers(id)
}
