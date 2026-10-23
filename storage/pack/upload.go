package pack

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"io/fs"

	"github.com/pkg/errors"
)

// ErrUploadMismatch is returned when the bytes an archive was written from
// are not the bytes its storage kept.
var ErrUploadMismatch = errors.New("stored archive does not match what was written")

// Checksummer is implemented by an ArchiveStorage whose backend reports a
// checksum of a stored file, so an upload can be checked without reading
// it back. An empty sum means the backend has none for that file.
type Checksummer interface {
	Checksum(name string) ([]byte, error)
}

// StoredFile is implemented by a File created for writing whose storage
// answers the upload with what it stored. Once the file is closed, Stored
// returns that file's info, as Stat of an Open of it would, and its MD5;
// ok is false when the storage did not say.
type StoredFile interface {
	Stored() (info fs.FileInfo, md5 []byte, ok bool)
}

func storedInfo(file any) (fs.FileInfo, []byte, bool) {
	stored, ok := file.(StoredFile)
	if !ok {
		return nil, nil, false
	}

	return stored.Stored()
}

// hashedWriter hashes everything on its way to the storage, so what was
// sent can be compared with what the storage reports it kept. MD5 because
// that is the checksum object stores answer with; it guards against a
// damaged upload, not against an attacker.
type hashedWriter struct {
	to      writeFile
	sum     hash.Hash
	written int64
}

func newHashedWriter(to writeFile) *hashedWriter {
	return &hashedWriter{to: to, sum: md5.New()}
}

func (h *hashedWriter) Write(p []byte) (int, error) {
	n, err := h.to.Write(p)
	_, _ = h.sum.Write(p[:n])
	h.written += int64(n)

	return n, err
}

func (h *hashedWriter) Close() error { return h.to.Close() }

var _ io.WriteCloser = (*hashedWriter)(nil)

// verifyUpload compares the archive's storage-side checksum with the one
// taken while writing. A storage that reports no checksum is passed over.
func (a *archive) verifyUpload() error {
	written, ok := a.writeFile.(*hashedWriter)
	if !ok {
		return nil
	}

	_, stored, ok := storedInfo(written.to)
	if !ok {
		sums, ok := a.storage.(Checksummer)
		if !ok {
			return nil
		}

		var err error

		stored, err = sums.Checksum(a.archiveName())
		if err != nil {
			return errors.Wrapf(err, "reading the checksum of archive %s", a.name)
		}
	}

	if len(stored) == 0 {
		a.logger.Debug("the storage reports no checksum for an archive", "archive", a.name)

		return nil
	}

	sent := written.sum.Sum(nil)
	if !bytes.Equal(stored, sent) {
		return errors.Wrapf(ErrUploadMismatch, "archive %s: wrote %s, storage kept %s",
			a.name, hex.EncodeToString(sent), hex.EncodeToString(stored))
	}

	return nil
}
