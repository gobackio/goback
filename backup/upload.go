package backup

import (
	"context"
	"io"
)

// Uploader takes upload files (see storage/upload) whole, in the caller's
// session.
type Uploader interface {
	// Upload sends the upload file of size bytes that file holds. The status
	// is UploadDone once the store ingested it; otherwise UploadStatus
	// tells, by the status's ID, when it has.
	Upload(ctx context.Context, file io.Reader, size int64) (UploadStatus, error)
	// UploadStatus tells how far the store got with uploads, one status per
	// id, in order.
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
// Bytes the file's bytes read of BytesTotal. Error says why an upload
// failed.
type UploadStatus struct {
	ID         string
	State      UploadState
	Objects    int64
	Stored     int64
	Bytes      int64
	BytesTotal int64
	Error      string
}
