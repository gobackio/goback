package backup

import (
	"context"
	"errors"
	"time"

	"github.com/gobackio/goback/proto"
)

// Session is one backup or restore run of an agent against a set. Objects
// written under a session are visible only to that session until a commit.
type Session struct {
	ID      string
	AgentID string
	Set     string
	// Restore names the object a restore session reads; retirement and
	// garbage collection keep it and everything it names while the session
	// lives.
	Restore  *proto.Ref
	Started  time.Time
	LastSeen time.Time
}

// RestoreLeaser reports the refs that live restore sessions are reading.
type RestoreLeaser interface {
	// RestoreLeases returns the refs of every live restore session.
	RestoreLeases(ctx context.Context) ([]*proto.Ref, error)
}

// ErrNoSession is returned when a session id names no live session.
var ErrNoSession = errors.New("no such session")

// ErrSealChanged is returned by a Walker with Carry over a previous commit
// sealed under another store key or none, whose entries it cannot carry.
var ErrSealChanged = errors.New("the set's latest commit is sealed under another store key or none")

type sessionKey struct{}

// WithSession attaches the session to the context.
func WithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, s)
}

// SessionFromContext returns the request's session, if any.
func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(sessionKey{}).(*Session)
	return s, ok && s != nil
}

// SessionStore opens and closes backup sessions. BeginSession fills in the
// session's id and returns a context carrying it; every Put and Get made
// with that context belongs to the session. EndSession drops whatever the
// session has not committed.
type SessionStore interface {
	// BeginSession opens s, fills in its ID and returns a context that
	// carries it.
	BeginSession(ctx context.Context, s *Session) (context.Context, error)
	// EndSession ends the context's session.
	EndSession(ctx context.Context) error
}

// HoldRestore opens a restore session naming ref when store keeps
// sessions, so retirement and garbage collection keep it until end is
// called; read through held meanwhile. Without sessions it holds nothing.
func HoldRestore(ctx context.Context, store ObjectStore, set string, ref *proto.Ref) (held context.Context, end func(), err error) {
	sessions, ok := store.(SessionStore)
	if !ok {
		return ctx, func() {}, nil
	}

	held, err = sessions.BeginSession(ctx, &Session{Set: set, Restore: ref})
	if err != nil {
		return nil, nil, err
	}

	// what was read stays read, and a session that outlived its lease is
	// gone already
	return held, func() { _ = sessions.EndSession(held) }, nil
}

// Leased is a session store that ends sessions after a lease without a
// write.
type Leased interface {
	// SessionLease is how long a session lives without a write.
	SessionLease() time.Duration
}
