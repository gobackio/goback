package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

// slowBucket answers no request but reads: a read hands out one byte
// every pause, up to size, and with stall then waits for its request to
// end; a write never moves.
type slowBucket struct {
	size  int64
	pause time.Duration
	stall bool
}

func (b *slowBucket) ErrorCode(err error) gcerrors.ErrorCode {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return gcerrors.DeadlineExceeded
	}

	return gcerrors.Unknown
}

func (b *slowBucket) As(any) bool             { return false }
func (b *slowBucket) ErrorAs(error, any) bool { return false }
func (b *slowBucket) Close() error            { return nil }

func (b *slowBucket) Attributes(ctx context.Context, _ string) (*driver.Attributes, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *slowBucket) ListPaged(ctx context.Context, _ *driver.ListOptions) (*driver.ListPage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *slowBucket) NewRangeReader(ctx context.Context, _ string, offset, length int64, _ *driver.ReaderOptions) (driver.Reader, error) {
	left := b.size - offset
	if length >= 0 {
		left = min(left, length)
	}

	return &slowReader{ctx: ctx, bucket: b, left: left}, nil
}

func (b *slowBucket) NewTypedWriter(ctx context.Context, _, _ string, _ *driver.WriterOptions) (driver.Writer, error) {
	return stuckWriter{ctx: ctx}, nil
}

func (b *slowBucket) Copy(context.Context, string, string, *driver.CopyOptions) error {
	return errors.New("not faked")
}

func (b *slowBucket) Delete(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func (b *slowBucket) SignedURL(context.Context, string, *driver.SignedURLOptions) (string, error) {
	return "", errors.New("not faked")
}

type slowReader struct {
	ctx    context.Context
	bucket *slowBucket
	left   int64
}

func (r *slowReader) Read(buf []byte) (int, error) {
	if r.left == 0 {
		if r.bucket.stall {
			<-r.ctx.Done()
			return 0, r.ctx.Err()
		}

		return 0, io.EOF
	}

	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-time.After(r.bucket.pause):
	}

	buf[0] = 'x'
	r.left--

	return 1, nil
}

func (r *slowReader) Close() error { return nil }
func (r *slowReader) Attributes() *driver.ReaderAttributes {
	return &driver.ReaderAttributes{Size: r.left}
}
func (r *slowReader) As(any) bool { return false }

type stuckWriter struct{ ctx context.Context }

func (w stuckWriter) Write([]byte) (int, error) {
	<-w.ctx.Done()
	return 0, w.ctx.Err()
}

func (w stuckWriter) Close() error { return w.ctx.Err() }

const shortTimeout = 100 * time.Millisecond

func slowStore(b *slowBucket) *storage.BucketStore {
	return storage.NewBucketStore(blob.NewBucket(b), storage.WithOperationTimeout(shortTimeout))
}

// within runs fn and fails the test if it takes much longer than the
// store's timeout.
func within(t *testing.T, fn func()) {
	t.Helper()

	start := time.Now()
	fn()
	require.Less(t, time.Since(start), 20*shortTimeout)
}

func TestABucketRequestThatHangsFailsAfterTheTimeout(t *testing.T) {
	store := slowStore(&slowBucket{})

	within(t, func() {
		_, err := store.Open("a.idx")
		require.Error(t, err)
	})

	within(t, func() {
		_, err := store.List(".idx")
		require.Error(t, err)
	})

	within(t, func() {
		require.Error(t, store.Delete("a.idx"))
	})
}

func TestAStreamedReadThatKeepsMovingIsNeverCut(t *testing.T) {
	size := int64(8)
	store := slowStore(&slowBucket{size: size, pause: shortTimeout / 4})

	file, err := store.OpenListed(pack.ListedFile{Name: "a.pack", Size: size})
	require.NoError(t, err)

	start := time.Now()
	read, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("x"), int(size)), read)
	require.Greater(t, time.Since(start), shortTimeout, "the read as a whole ran past the timeout")

	buf := make([]byte, size)
	_, err = file.(io.ReaderAt).ReadAt(buf, 0)
	require.NoError(t, err)

	var copied bytes.Buffer
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = file.(io.WriterTo).WriteTo(&copied)
	require.NoError(t, err)
	require.Equal(t, read, copied.Bytes())
}

func TestAStreamedReadThatStallsFailsAfterTheTimeout(t *testing.T) {
	store := slowStore(&slowBucket{size: 3, pause: time.Millisecond, stall: true})

	file, err := store.OpenListed(pack.ListedFile{Name: "a.pack", Size: 8})
	require.NoError(t, err)

	within(t, func() {
		_, err := io.ReadAll(file)
		require.Error(t, err)
	})

	within(t, func() {
		_, err := file.(io.ReaderAt).ReadAt(make([]byte, 8), 0)
		require.Error(t, err)
	})
}

func TestAWriteThatStallsFailsAfterTheTimeout(t *testing.T) {
	store := slowStore(&slowBucket{})

	file, err := store.Create("a.pack")
	require.NoError(t, err)

	within(t, func() {
		_, err := file.Write(make([]byte, 64<<10))
		if err == nil {
			err = file.Close()
		}
		require.Error(t, err)
	})
}
