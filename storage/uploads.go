package storage

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/upload"
)

// MaxUpload is the largest upload file a store takes.
const MaxUpload = 1 << 30

// uploadTTL is how long an upload id waits for its file.
const uploadTTL = time.Hour

// issued is what BeginUpload answered an upload id with.
type issued struct {
	agent   string
	session string
	size    int64
	expires time.Time
}

type uploads struct {
	mtx sync.Mutex
	ids map[string]issued
}

// BeginUpload issues the id under which the caller sends SendUpload an
// upload file of size bytes, in the same session.
func (s *Store) BeginUpload(ctx context.Context, size int64) (string, error) {
	p, err := auth.Require(ctx)
	if err != nil {
		return "", err
	}

	if size <= 0 || size > MaxUpload {
		return "", fmt.Errorf("%w: an upload file of %d bytes, at most %d", ErrInvalidRequest, size, MaxUpload)
	}

	session, err := s.writeSession(ctx)
	if err != nil {
		return "", err
	}

	id := rand.Text()
	now := time.Now()

	s.uploads.mtx.Lock()
	defer s.uploads.mtx.Unlock()

	if s.uploads.ids == nil {
		s.uploads.ids = map[string]issued{}
	}

	for other, up := range s.uploads.ids {
		if now.After(up.expires) {
			delete(s.uploads.ids, other)
		}
	}

	s.uploads.ids[id] = issued{agent: p.AgentID, session: session, size: size, expires: now.Add(uploadTTL)}

	return id, nil
}

// SendUpload ingests the upload file r under id, which BeginUpload issued
// to the caller in its session; an id takes one file. A file longer than
// BeginUpload was told is ErrInvalidRequest. The commits a file holds go
// to CommitUploaded, or are put together once the rest is stored.
func (s *Store) SendUpload(ctx context.Context, id string, r io.Reader) (backup.UploadStatus, error) {
	p, err := auth.Require(ctx)
	if err != nil {
		return backup.UploadStatus{}, err
	}

	session, err := s.writeSession(ctx)
	if err != nil {
		return backup.UploadStatus{}, err
	}

	up, err := s.uploads.take(id, p.AgentID, session)
	if err != nil {
		return backup.UploadStatus{}, err
	}

	done, err := upload.Ingest(ctx, &capped{r: r, left: up.size}, up.size, s)
	if err != nil {
		return backup.UploadStatus{}, err
	}

	status := backup.UploadStatus{ID: id, State: backup.UploadDone, Objects: done.Objects, Stored: done.Stored, Bytes: up.size, BytesTotal: up.size}

	switch {
	case len(done.Commits) == 0:
		return status, nil
	case s.CommitUploaded != nil:
		return s.CommitUploaded(ctx, id, done.Commits)
	}

	receipt, err := s.PutCommits(ctx, done.Commits)
	if err != nil {
		return backup.UploadStatus{}, err
	}

	return receipt.Answer(status), nil
}

// Answer is status, of the upload that held the commits, as it stands
// once the store put them: done with the commits as stored, or failed for
// what they lack.
func (r *CommitsReceipt) Answer(status backup.UploadStatus) backup.UploadStatus {
	if len(r.Missing) > 0 {
		missing := &backup.MissingError{Refs: allOf(r.Missing), Commits: r.Missing}

		status.State, status.Missing, status.CommitsMissing = backup.UploadFailed, missing.Refs, missing.Commits
		status.Error = missing.Error()

		return status
	}

	status.State, status.Commits = backup.UploadDone, r.Commits
	status.Commit = r.Commits[len(r.Commits)-1]

	return status
}

// allOf is every ref of refs once, in order.
func allOf(refs [][]*proto.Ref) []*proto.Ref {
	var out []*proto.Ref
	seen := map[string]bool{}

	for _, list := range refs {
		for _, ref := range list {
			if !seen[string(ref.GetHash())] {
				seen[string(ref.GetHash())] = true
				out = append(out, ref)
			}
		}
	}

	return out
}

// take removes id if it was issued to agent in session and hasn't expired.
func (u *uploads) take(id, agent, session string) (issued, error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	up, ok := u.ids[id]
	if !ok || time.Now().After(up.expires) {
		return issued{}, fmt.Errorf("upload %s: %w", id, backup.ErrNotFound)
	}

	if up.agent != agent || up.session != session {
		return issued{}, fmt.Errorf("%w: upload %s belongs to another session", auth.ErrForbidden, id)
	}

	delete(u.ids, id)

	return up, nil
}

// writeSession is the id of the caller's session, which writes need when
// the store has sessions.
func (s *Store) writeSession(ctx context.Context) (string, error) {
	if s.Sessions == nil {
		return "", nil
	}

	session, ok := backup.SessionFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("%w: writes need a session", backup.ErrNoSession)
	}

	return session.ID, nil
}

// capped fails a read past the size an upload declared.
type capped struct {
	r    io.Reader
	left int64
}

func (c *capped) Read(p []byte) (int, error) {
	if c.left <= 0 {
		var one [1]byte

		for {
			n, err := c.r.Read(one[:])
			if n > 0 {
				return 0, fmt.Errorf("%w: the upload file is longer than declared", ErrInvalidRequest)
			}

			if err != nil {
				return 0, err
			}
		}
	}

	if int64(len(p)) > c.left {
		p = p[:c.left]
	}

	n, err := c.r.Read(p)
	c.left -= int64(n)

	return n, err
}
