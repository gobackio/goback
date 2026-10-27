package pack

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// writeSession is the in-process side of a session: its open archive and
// the objects written but not yet answered for by the index. The root
// session (no backup.Session) writes root archives that are committed on
// finalize.
type writeSession struct {
	id        string
	session   *backup.Session
	placement Placement
	// sealed is the generation the session's begin marker records, known
	// without reading the marker when began is set
	sealed uint64
	began  bool

	mtx       sync.Mutex
	archive   *archive
	lastWrite time.Time
	lastTouch time.Time
	// failure is the first error that lost an archive of this session
	failure error

	pendingMtx sync.RWMutex
	pending    map[string]pendingObject
}

// fail records the first error that lost one of the session's archives;
// the caller holds the session lock.
func (ws *writeSession) fail(err error) {
	if err != nil && ws.failure == nil {
		ws.failure = err
	}
}

// failed returns the error that lost an archive of the session, if any.
func (ws *writeSession) failed() error {
	ws.mtx.Lock()
	defer ws.mtx.Unlock()

	return ws.failure
}

func newWriteSession(s *backup.Session) *writeSession {
	ws := &writeSession{pending: make(map[string]pendingObject)}
	if s != nil {
		ws.id = s.ID
		ws.session = s
		ws.placement = sessionPlacement(s)
	}

	return ws
}

func (ws *writeSession) state() ArchiveState {
	if ws.session == nil {
		return ArchiveCommitted
	}

	return ArchivePending
}

func (ws *writeSession) archiveInfo(name string) ArchiveInfo {
	return ArchiveInfo{Name: name, Session: ws.id, State: ws.state()}
}

func (ws *writeSession) addPending(a *archive, ref *proto.Ref) {
	rec := a.indexLocation(ref)
	if rec == nil {
		return
	}

	ws.pendingMtx.Lock()
	ws.pending[string(ref.Hash)] = pendingObject{archive: a, record: rec}
	ws.pendingMtx.Unlock()
}

// now is the time sessions are stamped with: the index's
// when every process shares it, this machine's otherwise.
func (ps *PackStorage) now(ctx context.Context) (time.Time, error) {
	if clock, ok := ps.index.(SharedClock); ok {
		return clock.SharedNow(ctx)
	}

	return time.Now(), nil
}

// BeginSession registers the session, assigning an id when it has none,
// and returns a context under which writes belong to it.
func (ps *PackStorage) BeginSession(ctx context.Context, s *backup.Session) (context.Context, error) {
	if s.ID == "" {
		s.ID = uuid.New().String()
	}

	now, err := ps.now(ctx)
	if err != nil {
		return nil, err
	}

	s.Started = now
	s.LastSeen = now

	// its commit checks the seals newer than this; the newest this process
	// listed or wrote stands in for listing them
	sealed := ps.sealSeen.Load()

	err = ps.markBegun(ctx, s, sealed)
	if err != nil {
		return nil, err
	}

	err = ps.index.BeginSession(s)
	if err != nil {
		return nil, err
	}

	ws := newWriteSession(s)
	ws.sealed, ws.began = sealed, true

	ps.sessionsMtx.Lock()
	ps.sessions[s.ID] = ws
	ps.sessionsMtx.Unlock()

	return backup.WithSession(ctx, s), nil
}

// EndSession drops what the context's session has not committed.
func (ps *PackStorage) EndSession(ctx context.Context) error {
	s, ok := backup.SessionFromContext(ctx)
	if !ok {
		return nil
	}

	return ps.endSession(s.ID)
}

// TouchSession implements backup.SessionToucher.
func (ps *PackStorage) TouchSession(ctx context.Context) error {
	s, ok := backup.SessionFromContext(ctx)
	if !ok {
		return nil
	}

	now := time.Now()
	if err := ps.index.TouchSession(s.ID, now); err != nil {
		return err
	}

	ps.sessionsMtx.Lock()
	ws := ps.sessions[s.ID]
	ps.sessionsMtx.Unlock()

	if ws != nil {
		ws.mtx.Lock()
		ws.lastTouch = now
		ws.mtx.Unlock()
	}

	return nil
}

// LookupSession returns a live session by id.
func (ps *PackStorage) LookupSession(_ context.Context, id string) (*backup.Session, error) {
	return ps.index.GetSession(id)
}

func (ps *PackStorage) endSession(id string) error {
	// a session that committed keeps its archives whatever the index says
	outcome, err := ps.markEnded(id, SessionAborted)
	if err != nil {
		return err
	}

	return ps.closeSession(id, outcome)
}

// closeSession drops a session whose end marker holds outcome.
func (ps *PackStorage) closeSession(id string, outcome SessionOutcome) error {
	ps.sessionsMtx.Lock()
	ws := ps.sessions[id]
	delete(ps.sessions, id)
	ps.sessionsMtx.Unlock()

	if ws != nil {
		ws.mtx.Lock()
		ps.discardArchive(ws)
		ws.mtx.Unlock()
	}

	dropped, err := ps.index.EndSession(id)
	if err != nil {
		return err
	}

	for _, name := range dropped {
		ps.mtx.RLock()
		var loaded *archive
		for _, a := range ps.archives {
			if a.name == name {
				loaded = a
			}
		}
		ps.mtx.RUnlock()

		// an index restored from before the commit still has the archive
		// pending; the marker says otherwise, and the marker wins
		if outcome == SessionCommitted || ps.markedCommitted(name) {
			ps.logger.Warn("keeping an archive the index had pending, which a commit marked committed", "archive", name, "session", id)
			ps.adoptArchive(name, loaded)

			continue
		}

		if loaded != nil {
			ps.dropArchive(loaded)
		} else {
			ps.deleteArchiveFiles(name)
		}
	}

	return nil
}

// adoptArchive indexes an archive the index let go of as committed.
func (ps *PackStorage) adoptArchive(name string, loaded *archive) {
	if loaded != nil {
		ps.mtx.Lock()
		ps.archives = slices.DeleteFunc(ps.archives, func(a *archive) bool { return a == loaded })
		ps.mtx.Unlock()

		_ = loaded.Close()
	}

	_, err := ps.openArchive(name, nil, nil)
	if err != nil {
		ps.logger.Error("indexing an archive a commit marked committed failed; it stays in the storage", "archive", name, "err", err)
	}
}

// writeSessionFor returns the session of the context, or the root session.
// A session this process does not know is recreated only while the index
// still holds it; one that ended gets backup.ErrNoSession.
func (ps *PackStorage) writeSessionFor(ctx context.Context) (*writeSession, error) {
	s, _ := backup.SessionFromContext(ctx)

	id := ""
	if s != nil {
		id = s.ID
	}

	ps.sessionsMtx.Lock()
	defer ps.sessionsMtx.Unlock()

	ws, ok := ps.sessions[id]
	if !ok {
		if s != nil {
			if _, err := ps.index.GetSession(id); err != nil {
				return nil, err
			}
		}

		ws = newWriteSession(s)
		ps.sessions[id] = ws
	}

	return ws, nil
}

func (ps *PackStorage) lookupWriteSession(id string) *writeSession {
	ps.sessionsMtx.Lock()
	defer ps.sessionsMtx.Unlock()

	return ps.sessions[id]
}

func (ps *PackStorage) writeSessions() []*writeSession {
	ps.sessionsMtx.Lock()
	defer ps.sessionsMtx.Unlock()

	sessions := make([]*writeSession, 0, len(ps.sessions))
	for _, ws := range ps.sessions {
		sessions = append(sessions, ws)
	}

	return sessions
}

// touchSessionOf renews the context's session, so a session that only
// reads, like a restore, keeps its lease.
func (ps *PackStorage) touchSessionOf(ctx context.Context) {
	s, ok := backup.SessionFromContext(ctx)
	if !ok {
		return
	}

	ps.sessionsMtx.Lock()
	ws := ps.sessions[s.ID]
	ps.sessionsMtx.Unlock()

	if ws != nil {
		ps.touchSession(ws)
	}
}

// RestoreLeases implements backup.RestoreLeaser over the sessions whose
// lease has not run out.
func (ps *PackStorage) RestoreLeases(_ context.Context) ([]*proto.Ref, error) {
	sessions, err := ps.index.ListSessions()
	if err != nil {
		return nil, err
	}

	now := time.Now()

	var refs []*proto.Ref
	for _, s := range sessions {
		if s.Restore == nil || (ps.sessionLease > 0 && now.Sub(s.LastSeen) >= ps.sessionLease) {
			continue
		}

		refs = append(refs, s.Restore)
	}

	return refs, nil
}

// touchSession renews the session's lease, at most every quarter lease.
func (ps *PackStorage) touchSession(ws *writeSession) {
	if ws.session == nil || ps.sessionLease <= 0 {
		return
	}

	now := time.Now()

	ws.mtx.Lock()
	due := now.Sub(ws.lastTouch) >= ps.sessionLease/4
	if due {
		ws.lastTouch = now
	}
	ws.mtx.Unlock()

	if !due {
		return
	}

	err := ps.index.TouchSession(ws.id, now)
	if err != nil {
		ps.logger.Warn("renewing session failed", "session", ws.id, "err", err)
	}
}

func (ps *PackStorage) flushSession(ws *writeSession) error {
	ws.mtx.Lock()
	defer ws.mtx.Unlock()

	return ps.finalizeLocked(ws)
}

// Flush finalizes the open archives of every session.
func (ps *PackStorage) Flush() error {
	var grp errgroup.Group

	for _, ws := range ps.writeSessions() {
		ws := ws
		grp.Go(func() error {
			return ps.flushSession(ws)
		})
	}

	err := grp.Wait()
	if err != nil {
		return err
	}

	return nil
}

// Sweep finalizes archives idle past the idle timeout and ends sessions
// whose lease ran out, as of now. Nothing runs it but the caller.
func (ps *PackStorage) Sweep(now time.Time) {
	if ps.idleFinalize > 0 {
		for _, ws := range ps.writeSessions() {
			ws.mtx.Lock()
			idle := ws.archive != nil && now.Sub(ws.lastWrite) >= ps.idleFinalize
			if idle {
				if err := ps.finalizeLocked(ws); err != nil {
					ps.logger.Warn("finalizing idle archive failed", "err", err)
				}
			}
			ws.mtx.Unlock()
		}
	}

	if ps.sessionLease > 0 {
		ps.expireSessions(now)
	}
}

func (ps *PackStorage) expireSessions(now time.Time) {
	sessions, err := ps.index.ListSessions()
	if err != nil {
		ps.logger.Warn("listing sessions failed", "err", err)
		return
	}

	for _, s := range sessions {
		if now.Sub(s.LastSeen) < ps.sessionLease {
			continue
		}

		ps.logger.Info("ending session, lease expired", "session", s.ID, "agent", s.AgentID)
		err := ps.endSession(s.ID)
		if err != nil {
			ps.logger.Warn("ending session failed", "session", s.ID, "err", err)
		}
	}
}
