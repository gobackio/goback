package backup

import (
	"context"
	"io"

	"github.com/gobackio/goback/proto"
)

// Uploader takes upload files (see storage/upload) whole, in the caller's
// session. A file may hold the session's commits, which the store puts
// together, as PutCommits does, once it holds everything else the session
// uploaded; the commit ends the session as Put's does.
type Uploader interface {
	// Upload sends the upload file of size bytes that file holds. The status
	// is UploadDone once the store ingested it, and committed the commit it
	// held; otherwise UploadStatus tells, by the status's ID, when it has.
	Upload(ctx context.Context, file io.Reader, size int64) (UploadStatus, error)
	// UploadStatus tells how far the store got with the caller's uploads,
	// one status per id, in order. It needs no session, so it also answers
	// once a commit ended the session.
	UploadStatus(ctx context.Context, ids []string) ([]UploadStatus, error)
}

// UploadState is where an upload is.
type UploadState int32

const (
	// UploadWaiting is an upload whose file hasn't arrived yet.
	UploadWaiting UploadState = iota + 1
	UploadIngesting
	UploadDone
	UploadFailed
)

// UploadStatus is how far a store got with an upload: Objects counts the
// objects ingested, Stored those of them the store didn't hold already, and
// Bytes the file's bytes read of BytesTotal. While it is ingesting, Phase
// names the progress phase the store is in, such as progress.PhaseIngest,
// and Objects counts that phase's units, of ObjectsTotal when the phase
// knows its total. Error says why an upload failed.
//
// For a file that held commits, Commits are the commits as the store
// stored them once the upload is done, in the file's order, and Commit is
// the last of them. One that failed for what the commits reference and the
// store holds nowhere names that in Missing, and what each commit lacks in
// CommitsMissing, as a MissingError would, and leaves the session open to
// send them and the commits again.
type UploadStatus struct {
	ID             string
	State          UploadState
	Objects        int64
	ObjectsTotal   int64
	Stored         int64
	Bytes          int64
	BytesTotal     int64
	Error          string
	Phase          string
	Commit         *proto.Object
	Commits        []*proto.Object
	Missing        []*proto.Ref
	CommitsMissing [][]*proto.Ref
}

// MissingError is the error of an upload that failed for what its commits
// lack, nil for any other.
func (s UploadStatus) MissingError() *MissingError {
	if len(s.Missing) == 0 {
		return nil
	}

	return &MissingError{Refs: s.Missing, Commits: s.CommitsMissing}
}
