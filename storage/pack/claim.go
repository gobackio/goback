package pack

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

const (
	// DefaultClaimOpen is how long an archive stays open unless WithClaims
	// says otherwise.
	DefaultClaimOpen = 10 * time.Second
	// DefaultClaimGrace is how much longer a claim holds while its archive
	// is closed, unless WithClaims says otherwise.
	DefaultClaimGrace = 2 * time.Minute
	// claimPoll is how often a commit looks again at archives another
	// process holds open.
	claimPoll = 100 * time.Millisecond
)

// rowWriter indexes the objects of an open archive as they are written.
// Whoever writes rows while none are being written writes everything
// queued, so writers that arrive meanwhile share the next insert rather
// than each making their own.
type rowWriter struct {
	insert func([]IndexRecord) error

	mtx     sync.Mutex
	idle    *sync.Cond
	busy    bool
	queue   []IndexRecord
	waiting []chan error
}

func newRowWriter(insert func([]IndexRecord) error) *rowWriter {
	w := &rowWriter{insert: insert}
	w.idle = sync.NewCond(&w.mtx)

	return w
}

// enqueue adds a record; the returned channel says whether its row went in.
func (w *rowWriter) enqueue(record IndexRecord) chan error {
	done := make(chan error, 1)

	w.mtx.Lock()
	w.queue = append(w.queue, record)
	w.waiting = append(w.waiting, done)
	w.mtx.Unlock()

	return done
}

// wait returns once the record behind done is indexed, writing the queue
// itself when nobody else is.
func (w *rowWriter) wait(done chan error) error {
	w.mtx.Lock()
	if !w.busy {
		w.writeLocked()
	}
	w.mtx.Unlock()

	return <-done
}

// drain returns once everything queued is indexed, and the first error it
// met.
func (w *rowWriter) drain() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	for w.busy {
		w.idle.Wait()
	}

	return w.writeLocked()
}

// writeLocked writes the queue until it is empty; the caller holds mtx and
// holds it again on return.
func (w *rowWriter) writeLocked() error {
	var first error

	w.busy = true

	for len(w.queue) > 0 {
		batch, waiting := w.queue, w.waiting
		w.queue, w.waiting = nil, nil

		w.mtx.Unlock()
		err := w.insert(batch)
		w.mtx.Lock()

		for _, done := range waiting {
			done <- err
		}

		if first == nil {
			first = err
		}
	}

	w.busy = false
	w.idle.Broadcast()

	return first
}

// claim takes the claim on a new archive of a session and arranges for the
// archive to be finalized once it has been open for as long as it may be.
func (ps *PackStorage) claim(a *archive) error {
	if err := ps.claims.OpenArchive(a.name, a.session); err != nil {
		return err
	}

	a.rows = newRowWriter(func(records []IndexRecord) error {
		return ps.claims.AddObjects(a.name, records)
	})
	a.opened = time.Now()
	a.closing = time.AfterFunc(ps.claimOpen, func() {
		if err := ps.finalizeArchive(a); err != nil {
			ps.logger.Warn("finalizing archive failed", "archive", a.name, "err", err)
		}
	})

	return nil
}

// claimLimit is how long a claim holds: the archive's open time and the
// grace for closing it.
func (ps *PackStorage) claimLimit() time.Duration {
	return ps.claimOpen + ps.claimGrace
}

// settle waits until no archive of the session is held open anywhere,
// turning lost the ones whose claim lapsed, and refuses the commit when a
// lost archive held objects nothing else holds.
func (ps *PackStorage) settle(session string) error {
	for {
		claims, err := ps.claims.Claims(session)
		if err != nil {
			return err
		}

		held := false

		for _, claim := range claims {
			if claim.Age < ps.claimLimit() {
				held = true

				continue
			}

			abandoned, err := ps.claims.Abandon(claim.Archive, ps.claimLimit())
			if err != nil {
				return err
			}

			if abandoned {
				ps.logger.Warn("an archive of the session was held open past its claim; its objects are lost",
					"session", session, "archive", claim.Archive, "age", claim.Age)
				ps.deleteArchiveFiles(claim.Archive)
			}
		}

		if !held {
			break
		}

		time.Sleep(claimPoll)
	}

	lost, err := ps.claims.Lost(session)
	if err != nil {
		return err
	}

	if len(lost) > 0 {
		return fmt.Errorf("%w: %d objects of session %s", backup.ErrSessionLost, len(lost), session)
	}

	return nil
}

// kept refuses a commit whose own record or un-tombstone was in an archive
// that lapsed, which settle does not answer for: Lost leaves both out.
func (ps *PackStorage) kept(ctx context.Context, ws *writeSession, commit *proto.Ref) error {
	has, err := ps.Has(ctx, commit)
	if err != nil {
		return err
	}

	untomb := proto.TombstoneRef(proto.TombstoneRef(commit))

	located, err := ps.index.LocateTombstones([]*proto.Ref{untomb}, Scope{Session: ws.id})
	if err != nil {
		return err
	}

	if !has || len(located[string(untomb.Hash)]) == 0 {
		return fmt.Errorf("%w: an archive holding commit %x of session %s lapsed", backup.ErrSessionLost, commit.Hash, ws.id)
	}

	return nil
}

// lapsed reports whether err says an archive's claim ran out.
func lapsed(err error) bool {
	return errors.Is(err, ErrClaimLapsed)
}
