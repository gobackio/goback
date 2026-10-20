package pack

import (
	"errors"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// NewInMemoryIndex returns an empty index.
func NewInMemoryIndex() *InMemoryIndex {
	return &InMemoryIndex{
		index:    make(map[string]map[[proto.HashSize]byte]IndexRecord),
		archives: make(map[string]ArchiveInfo),
		sessions: make(map[string]*backup.Session),
		claimed:  make(map[string]time.Time),
	}
}

var (
	_ ArchiveIndex = (*InMemoryIndex)(nil)
	_ ClaimIndex   = (*InMemoryIndex)(nil)
)

// InMemoryIndex is an ArchiveIndex kept entirely in memory, for tests and
// short-lived tools.
type InMemoryIndex struct {
	mtx      sync.RWMutex
	index    map[string]map[[proto.HashSize]byte]IndexRecord
	archives map[string]ArchiveInfo
	sessions map[string]*backup.Session
	claimed  map[string]time.Time

	// Now is the clock claims are aged by; nil means time.Now.
	Now func() time.Time
}

func (i *InMemoryIndex) now() time.Time {
	if i.Now != nil {
		return i.Now()
	}

	return time.Now()
}

// LocateObject implements pack.ArchiveIndex.
func (i *InMemoryIndex) LocateObject(ref *proto.Ref, scope Scope, exclude ...string) (IndexLocation, error) {
	var sum [proto.HashSize]byte
	copy(sum[:], ref.Hash)

	i.mtx.RLock()
	defer i.mtx.RUnlock()

outer:
	for archive, records := range i.index {
		for _, excluded := range exclude {
			if excluded == archive {
				continue outer
			}
		}

		if !scope.Visible(i.archives[archive]) {
			continue
		}

		if record, ok := records[sum]; ok {
			return IndexLocation{
				Archive: archive,
				Record:  record,
			}, nil
		}
	}

	return IndexLocation{}, ErrRecordNotFound
}

// LocateCopies implements pack.ArchiveIndex.
func (i *InMemoryIndex) LocateCopies(refs []*proto.Ref, scope Scope) (map[string][]IndexLocation, error) {
	return i.locateCopies(refs, scope, func(IndexRecord) bool { return true })
}

// LocateTombstones implements pack.ArchiveIndex.
func (i *InMemoryIndex) LocateTombstones(refs []*proto.Ref, scope Scope) (map[string][]IndexLocation, error) {
	return i.locateCopies(refs, scope, func(record IndexRecord) bool {
		return proto.ObjectType(record.Type) == proto.ObjectType_TOMBSTONE
	})
}

func (i *InMemoryIndex) locateCopies(refs []*proto.Ref, scope Scope, match func(IndexRecord) bool) (map[string][]IndexLocation, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	copies := make(map[string][]IndexLocation)

	for _, ref := range refs {
		var sum [proto.HashSize]byte
		copy(sum[:], ref.Hash)

		for archive, records := range i.index {
			if !scope.Visible(i.archives[archive]) {
				continue
			}

			if record, ok := records[sum]; ok && match(record) {
				copies[string(ref.Hash)] = append(copies[string(ref.Hash)], IndexLocation{Archive: archive, Record: record})
			}
		}
	}

	return copies, nil
}

// LookupArchive implements pack.ArchiveIndex.
func (i *InMemoryIndex) LookupArchive(archive string) (ArchiveInfo, bool, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	info, ok := i.archives[archive]

	return info, ok, nil
}

// IndexArchive implements pack.ArchiveIndex.
func (i *InMemoryIndex) IndexArchive(archive ArchiveInfo, index IndexFile) error {
	a := make(map[[proto.HashSize]byte]IndexRecord)

	for _, record := range index {
		a[record.Sum] = record
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	if known, ok := i.archives[archive.Name]; ok {
		if known.Created.IsZero() {
			known.Created = archive.Created
			i.archives[archive.Name] = known
		}

		return nil
	}

	if archive.State == ArchivePending {
		if _, ok := i.sessions[archive.Session]; !ok {
			return backup.ErrNoSession
		}
	}

	i.index[archive.Name] = a
	i.archives[archive.Name] = archive

	return nil
}

// DeleteArchives implements pack.ArchiveIndex.
func (i *InMemoryIndex) DeleteArchives(names []string) error {
	i.mtx.Lock()
	for _, name := range names {
		delete(i.index, name)
		delete(i.archives, name)
	}
	i.mtx.Unlock()

	return nil
}

// BeginSession implements SessionIndex.
func (i *InMemoryIndex) BeginSession(s *backup.Session) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	if _, ok := i.sessions[s.ID]; ok {
		return errors.New("session already exists")
	}

	copied := *s
	i.sessions[s.ID] = &copied

	return nil
}

// TouchSession implements SessionIndex.
func (i *InMemoryIndex) TouchSession(id string, at time.Time) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	s, ok := i.sessions[id]
	if !ok {
		return backup.ErrNoSession
	}

	s.LastSeen = at

	return nil
}

// GetSession implements SessionIndex.
func (i *InMemoryIndex) GetSession(id string) (*backup.Session, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	s, ok := i.sessions[id]
	if !ok {
		return nil, backup.ErrNoSession
	}

	copied := *s

	return &copied, nil
}

// ListSessions implements SessionIndex.
func (i *InMemoryIndex) ListSessions() ([]*backup.Session, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	sessions := make([]*backup.Session, 0, len(i.sessions))
	for _, s := range i.sessions {
		copied := *s
		sessions = append(sessions, &copied)
	}

	return sessions, nil
}

// EndSession implements SessionIndex.
func (i *InMemoryIndex) EndSession(id string) ([]string, error) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var dropped []string
	for name, info := range i.archives {
		if info.State != ArchiveCommitted && info.Session == id {
			dropped = append(dropped, name)
			delete(i.archives, name)
			delete(i.index, name)
			delete(i.claimed, name)
		}
	}

	delete(i.sessions, id)

	return dropped, nil
}

// PendingArchives implements SessionIndex.
func (i *InMemoryIndex) PendingArchives(id string) ([]string, error) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var names []string
	for name, info := range i.archives {
		if info.Session == id && info.State == ArchivePending {
			names = append(names, name)
		}
	}

	return names, nil
}

// CommitSession implements SessionIndex.
func (i *InMemoryIndex) CommitSession(id string) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	if _, ok := i.sessions[id]; !ok {
		return backup.ErrNoSession
	}

	for name, info := range i.archives {
		if info.Session != id {
			continue
		}

		switch info.State {
		case ArchivePending:
			info.State = ArchiveCommitted
			info.Session = ""
			i.archives[name] = info
		case ArchiveLost:
			delete(i.archives, name)
			delete(i.index, name)
			delete(i.claimed, name)
		}
	}

	return nil
}

// Close implements io.Closer.
func (i *InMemoryIndex) Close() error {
	i.mtx.Lock()
	i.index = make(map[string]map[[proto.HashSize]byte]IndexRecord)
	i.archives = make(map[string]ArchiveInfo)
	i.sessions = make(map[string]*backup.Session)
	i.claimed = make(map[string]time.Time)
	i.mtx.Unlock()

	return nil
}

// CountObjects reports the indexed and the distinct object count.
func (i *InMemoryIndex) CountObjects() (uint64, uint64, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	var total uint64

	distinct := make(map[[proto.HashSize]byte]bool)

	for archive, records := range i.index {
		if state := i.archives[archive].State; state != ArchiveCommitted && state != ArchivePending {
			continue
		}

		total += uint64(len(records))

		for sum := range records {
			distinct[sum] = true
		}
	}

	return total, uint64(len(distinct)), nil
}

// OpenArchive implements ClaimIndex.
func (i *InMemoryIndex) OpenArchive(name, session string) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	if _, ok := i.sessions[session]; !ok {
		return backup.ErrNoSession
	}

	i.archives[name] = ArchiveInfo{Name: name, Session: session, State: ArchiveOpen}
	i.index[name] = make(map[[proto.HashSize]byte]IndexRecord)
	i.claimed[name] = i.now()

	return nil
}

// AddObjects implements ClaimIndex.
func (i *InMemoryIndex) AddObjects(archive string, records []IndexRecord) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	info, ok := i.archives[archive]
	if !ok {
		return backup.ErrNoSession
	}

	if info.State != ArchiveOpen {
		return ErrClaimLapsed
	}

	for _, record := range records {
		i.index[archive][record.Sum] = record
	}

	return nil
}

// FinalizeArchive implements ClaimIndex.
func (i *InMemoryIndex) FinalizeArchive(name string, within time.Duration, created time.Time) error {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	info, ok := i.archives[name]
	if !ok {
		return backup.ErrNoSession
	}

	if info.State != ArchiveOpen || i.now().Sub(i.claimed[name]) >= within {
		return ErrClaimLapsed
	}

	info.State = ArchivePending
	info.Created = created
	i.archives[name] = info
	delete(i.claimed, name)

	return nil
}

// Holds implements ClaimIndex.
func (i *InMemoryIndex) Holds(refs []*proto.Ref, session string) (map[string]bool, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	held := make(map[string]bool)

	for name, info := range i.archives {
		if info.Session != session || (info.State != ArchivePending && info.State != ArchiveOpen) {
			continue
		}

		for _, ref := range refs {
			var sum [proto.HashSize]byte
			copy(sum[:], ref.Hash)

			if _, ok := i.index[name][sum]; ok {
				held[string(ref.Hash)] = true
			}
		}
	}

	return held, nil
}

// Claims implements ClaimIndex.
func (i *InMemoryIndex) Claims(session string) ([]Claim, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	now := i.now()

	var claims []Claim
	for name, info := range i.archives {
		if info.Session == session && info.State == ArchiveOpen {
			claims = append(claims, Claim{Archive: name, Age: now.Sub(i.claimed[name])})
		}
	}

	return claims, nil
}

// Abandon implements ClaimIndex.
func (i *InMemoryIndex) Abandon(name string, within time.Duration) (bool, error) {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	info := i.archives[name]
	if info.State != ArchiveOpen || i.now().Sub(i.claimed[name]) < within {
		return false, nil
	}

	info.State = ArchiveLost
	i.archives[name] = info
	delete(i.claimed, name)

	return true, nil
}

// Lost implements ClaimIndex.
func (i *InMemoryIndex) Lost(session string) ([]*proto.Ref, error) {
	i.mtx.RLock()
	defer i.mtx.RUnlock()

	held := func(sum [proto.HashSize]byte) bool {
		for name, info := range i.archives {
			kept := info.State == ArchiveCommitted || (info.State == ArchivePending && info.Session == session)
			if _, ok := i.index[name][sum]; ok && kept {
				return true
			}
		}

		return false
	}

	seen := make(map[[proto.HashSize]byte]bool)

	for name, info := range i.archives {
		if info.Session != session || (info.State != ArchiveLost && info.State != ArchivePending) {
			continue
		}

		for sum, record := range i.index[name] {
			if record.Type == uint32(proto.ObjectType_COMMIT) {
				untomb := proto.TombstoneRef(proto.TombstoneRef(&proto.Ref{Hash: sum[:]}))
				seen[[proto.HashSize]byte(untomb.Hash)] = true
			}
		}
	}

	var lost []*proto.Ref
	for name, info := range i.archives {
		if info.Session != session || info.State != ArchiveLost {
			continue
		}

		for sum, record := range i.index[name] {
			if record.Type == uint32(proto.ObjectType_COMMIT) || seen[sum] || held(sum) {
				continue
			}

			seen[sum] = true
			lost = append(lost, &proto.Ref{Hash: append([]byte(nil), sum[:]...)})
		}
	}

	return lost, nil
}
