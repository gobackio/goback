// Package upload is the upload file: objects a client sends a store in one
// piece, each after the ones among them it references. A Writer writes
// one, a Reader reads it back, and Ingest stores what it holds.
package upload

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/progress"
	"github.com/gobackio/goback/proto"

	gproto "google.golang.org/protobuf/proto"
)

// magic starts every upload file.
const magic = "goback-upload/1\n"

// MaxObject is the largest object an upload file holds.
const MaxObject = 32 << 20

// ErrInvalid is an upload file that can't be read.
var ErrInvalid = errors.New("invalid upload file")

// Writer writes an upload file: each object as a varint of its length and
// its bytes.
type Writer struct {
	w io.Writer
	n int64
}

// NewWriter starts an upload file on w.
func NewWriter(w io.Writer) (*Writer, error) {
	n, err := io.WriteString(w, magic)

	return &Writer{w: w, n: int64(n)}, err
}

// Write appends obj, which must come after the objects of the file it
// references.
func (w *Writer) Write(obj *proto.Object) error {
	data, err := gproto.Marshal(obj)
	if err != nil {
		return err
	}

	if len(data) > MaxObject {
		return fmt.Errorf("object %x is %d bytes, more than an upload file holds", obj.Ref().GetHash(), len(data))
	}

	var size [binary.MaxVarintLen64]byte
	n, err := w.w.Write(size[:binary.PutUvarint(size[:], uint64(len(data)))])
	w.n += int64(n)

	if err != nil {
		return err
	}

	n, err = w.w.Write(data)
	w.n += int64(n)

	return err
}

// Len is how many bytes the file has so far.
func (w *Writer) Len() int64 { return w.n }

// Reader reads an upload file.
type Reader struct {
	r    *bufio.Reader
	read int64
}

// NewReader starts reading the upload file r.
func NewReader(r io.Reader) (*Reader, error) {
	rd := &Reader{r: bufio.NewReader(r)}

	head := make([]byte, len(magic))
	if _, err := io.ReadFull(rd.r, head); err != nil {
		return nil, truncated(err)
	}

	if string(head) != magic {
		return nil, fmt.Errorf("%w: it doesn't start like one", ErrInvalid)
	}

	rd.read = int64(len(magic))

	return rd, nil
}

// Next returns the next object, or io.EOF after the last.
func (r *Reader) Next() (*proto.Object, error) {
	size, err := binary.ReadUvarint(r.r)
	if err == io.EOF {
		return nil, io.EOF
	}

	if err != nil {
		return nil, truncated(err)
	}

	if size > MaxObject {
		return nil, fmt.Errorf("%w: an object of %d bytes", ErrInvalid, size)
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r.r, data); err != nil {
		return nil, truncated(err)
	}

	r.read += int64(uvarintLen(size)) + int64(size)

	obj := &proto.Object{}
	if err := gproto.Unmarshal(data, obj); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}

	return obj, nil
}

// Read is how many bytes of the file Next has read.
func (r *Reader) Read() int64 { return r.read }

// truncated is ErrInvalid for a file that ends early; other errors are the
// reader's own.
func truncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: it ends early", ErrInvalid)
	}

	return err
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte

	return binary.PutUvarint(buf[:], v)
}

// Ingest stores about batchBytes of objects at once, and at most
// batchObjects.
const (
	batchBytes   = 8 << 20
	batchObjects = 4096
)

// Ingested is what Ingest did: the objects the file held, and how many of
// them the store didn't hold already. Commits are the commits the file
// held, in its order, which Ingest leaves for the caller to put together.
type Ingested struct {
	Objects, Stored int64
	Commits         []*proto.Object
}

const (
	// MaxCommits is the most commits an upload file holds.
	MaxCommits = 1000
	// MaxCommitBytes is the most bytes the commits of an upload file take
	// together, which Ingest holds until the file ends.
	MaxCommitBytes = 16 << 20
)

// missingOf returns the refs of batch b lacks: for a sealed object any
// copy b holds, and for one in the clear only a copy b verified where it
// tells them apart, so a sealed copy forged under its ref is no reason to
// skip it.
func missingOf(ctx context.Context, b backup.Batcher, batch []*proto.Object) ([]*proto.Ref, error) {
	verifier, verifies := b.(backup.VerifiedBatcher)

	var sealed, plain []*proto.Ref
	for _, obj := range batch {
		if obj.GetSealed() != nil || !verifies {
			sealed = append(sealed, obj.Ref())
		} else {
			plain = append(plain, obj.Ref())
		}
	}

	var missing []*proto.Ref

	if len(sealed) > 0 {
		lacks, err := b.Missing(ctx, sealed)
		if err != nil {
			return nil, err
		}

		missing = append(missing, lacks...)
	}

	if len(plain) > 0 {
		lacks, err := verifier.MissingVerified(ctx, plain)
		if err != nil {
			return nil, err
		}

		missing = append(missing, lacks...)
	}

	return missing, nil
}

// Ingest stores the objects of the upload file r in b, a batch at a time,
// skipping those b holds already, so ingesting a file twice stores it once.
// It asks b about at most 4,096 refs at once. The commits the file holds,
// at most MaxCommits of at most MaxCommitBytes together, are not stored
// but answered in Ingested.Commits.
// It reports its progress as progress.OpIngest, bytes counting the file's
// bytes against size.
func Ingest(ctx context.Context, r io.Reader, size int64, b backup.Batcher) (Ingested, error) {
	var done Ingested

	rd, err := NewReader(r)
	if err != nil {
		return done, err
	}

	phase := progress.Start(ctx, progress.OpIngest, progress.PhaseIngest, 0, size)
	defer phase.Finish()

	var (
		batch       []*proto.Object
		bytes       int
		commitBytes int
		read        = rd.Read()
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		missing, err := missingOf(ctx, b, batch)
		if err != nil {
			return err
		}

		lacks := make(map[string]bool, len(missing))
		for _, ref := range missing {
			lacks[string(ref.GetHash())] = true
		}

		var put []*proto.Object
		for _, obj := range batch {
			if lacks[string(obj.Ref().GetHash())] {
				put = append(put, obj)
			}
		}

		if len(put) > 0 {
			if err := b.PutObjects(ctx, put); err != nil {
				return err
			}
		}

		done.Objects += int64(len(batch))
		done.Stored += int64(len(put))
		phase.Add(int64(len(batch)), rd.Read()-read)
		read = rd.Read()
		batch, bytes = nil, 0

		return nil
	}

	for {
		obj, err := rd.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return done, err
		}

		if obj.GetCommit() != nil {
			if len(done.Commits) == MaxCommits {
				return done, fmt.Errorf("%w: it holds more than %d commits", ErrInvalid, MaxCommits)
			}

			if commitBytes += gproto.Size(obj); commitBytes > MaxCommitBytes {
				return done, fmt.Errorf("%w: its commits take more than %d bytes", ErrInvalid, MaxCommitBytes)
			}

			done.Commits = append(done.Commits, obj)
			done.Objects++

			continue
		}

		batch = append(batch, obj)
		bytes += gproto.Size(obj)

		if bytes >= batchBytes || len(batch) >= batchObjects {
			if err := flush(); err != nil {
				return done, err
			}
		}
	}

	return done, flush()
}
