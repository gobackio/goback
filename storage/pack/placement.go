package pack

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// Placement is the decoded prefix of an archive name: the session that
// wrote it and has not committed (<session>/<id>), or none for a committed
// archive at the root.
type Placement struct {
	Session string
}

// ParsePlacement decodes the prefix of an archive name.
func ParsePlacement(name string) Placement {
	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return Placement{}
	}

	return Placement{Session: parts[0]}
}

// Dir is the prefix under which archives of this placement are written.
func (p Placement) Dir() string {
	return p.Session
}

// sessionPlacement is where a session's archives are written.
func sessionPlacement(s *backup.Session) Placement {
	return Placement{Session: s.ID}
}

// ArchiveState is the visibility of an archive: committed archives are
// served to everyone, pending ones only to the session that wrote them.
// Open and lost archives are only ever held by a ClaimIndex: an open one is
// still being written by some process, so its objects exist for the session
// but cannot be read yet; a lost one was open when its claim lapsed.
type ArchiveState uint8

const (
	ArchiveCommitted ArchiveState = iota
	ArchivePending
	ArchiveOpen
	ArchiveLost
)

// ArchiveInfo is what the archive index records about an archive.
type ArchiveInfo struct {
	Name    string
	Session string
	State   ArchiveState
	// Created is when the bucket created the archive's index file, which
	// versions every record the archive holds; zero while it has none.
	Created time.Time
}

// Scope filters LocateObject: the committed archives plus the pending
// archives of Session.
type Scope struct {
	Session string
}

// Visible reports whether an archive is served to this scope.
func (s Scope) Visible(a ArchiveInfo) bool {
	switch a.State {
	case ArchiveCommitted:
		return true
	case ArchivePending:
		return s.Session != "" && a.Session == s.Session
	}

	return false
}

// ScopeOf is the scope of a context: the session it carries, else the
// committed archives alone.
func ScopeOf(ctx context.Context) Scope {
	if s, ok := backup.SessionFromContext(ctx); ok {
		return Scope{Session: s.ID}
	}

	return Scope{}
}

// SessionIndex keeps the live sessions of a store, so any node can tell a
// session id from a stale one and a janitor can expire the dead.
type SessionIndex interface {
	BeginSession(s *backup.Session) error
	TouchSession(id string, at time.Time) error
	GetSession(id string) (*backup.Session, error)
	ListSessions() ([]*backup.Session, error)
	// EndSession drops the session and its uncommitted archives, returning
	// the names of the archives dropped.
	EndSession(id string) ([]string, error)
	// PendingArchives names the session's archives that a commit would
	// make committed.
	PendingArchives(id string) ([]string, error)
	// CommitSession flips the session's pending archives to committed and
	// forgets its lost ones.
	CommitSession(id string) error
}

// SharedClock is implemented by an index whose processes share one clock.
// The store stamps when a session begins and when a collection completes
// by it, because a collection compares the two.
type SharedClock interface {
	SharedNow(ctx context.Context) (time.Time, error)
}

// ErrClaimLapsed is what a ClaimIndex answers for an open archive whose
// claim ran out before it was finalized.
var ErrClaimLapsed = errors.New("the claim on the archive lapsed")

// A Claim is an open archive of a session and how long ago it was claimed,
// by the index's clock.
type Claim struct {
	Archive string
	Age     time.Duration
}

// A ClaimIndex lets several processes write one session. A process claims
// each archive it opens and indexes the archive's objects as it writes
// them, so every process sees them at once; the claim holds for a bounded
// time, and one that lapsed can never be finalized. Ages are measured by
// the index's own clock, the one every process shares.
type ClaimIndex interface {
	// OpenArchive claims a new archive for a live session.
	OpenArchive(name, session string) error
	// AddObjects indexes objects written to an open archive, or answers
	// ErrClaimLapsed when the archive is no longer open and
	// backup.ErrNoSession when its session ended.
	AddObjects(archive string, records []IndexRecord) error
	// FinalizeArchive turns an open archive pending, created at created,
	// when it was claimed less than within ago, and answers ErrClaimLapsed
	// otherwise, or backup.ErrNoSession when its session ended.
	FinalizeArchive(name string, within time.Duration, created time.Time) error
	// Holds reports whether a pending or open archive of the session holds
	// the object.
	Holds(ref *proto.Ref, session string) (bool, error)
	// Claims lists the open archives of the session.
	Claims(session string) ([]Claim, error)
	// Abandon turns an open archive lost when it was claimed at least
	// within ago, and reports whether it did.
	Abandon(name string, within time.Duration) (bool, error)
	// Lost lists the objects the session's lost archives held that none of
	// its pending archives and no committed archive holds, leaving out
	// commits and the un-tombstones of the commits its lost and pending
	// archives hold, which belong to the commit that was refused.
	Lost(session string) ([]*proto.Ref, error)
}
