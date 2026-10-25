package backup

import (
	"context"
	"sync"

	"github.com/gobackio/goback/proto"

	pb "google.golang.org/protobuf/proto"
)

const (
	// batchBytes bounds the blobs of one PutObjects call.
	batchBytes = 4 << 20
	// batchCalls bounds the PutObjects calls under way at once.
	batchCalls = 8
)

// blobBatcher puts the blobs of a walk with PutObjects. A blob goes up at
// once while no call is under way; the blobs of a session put meanwhile
// gather until a call ends or they fill a batch.
type blobBatcher struct {
	store ObjectStore

	mtx     sync.Mutex
	open    *blobBatch
	queue   []*blobBatch
	sending int
}

type blobBatch struct {
	// ctx is the first put's, cancelled once every put gave up waiting
	ctx     context.Context
	cancel  context.CancelFunc
	session *Session
	objects []*proto.Object
	size    int
	waiting int
	done    chan struct{}
	err     error
}

func newBlobBatcher(store ObjectStore) *blobBatcher {
	return &blobBatcher{store: store}
}

// put stores blob with the others of its batch and returns once they are
// stored, or once ctx is done.
func (b *blobBatcher) put(ctx context.Context, blob *proto.Object) error {
	session, _ := SessionFromContext(ctx)
	size := pb.Size(blob)

	b.mtx.Lock()

	if open := b.open; open != nil && (open.session != session || open.size+size > batchBytes) {
		b.seal()
	}

	if b.open == nil {
		batch := &blobBatch{session: session, done: make(chan struct{})}
		batch.ctx, batch.cancel = context.WithCancel(context.WithoutCancel(ctx))
		b.open = batch
	}

	batch := b.open
	batch.objects = append(batch.objects, blob)
	batch.size += size
	batch.waiting++

	if b.sending == 0 {
		b.seal()
	}

	b.mtx.Unlock()

	select {
	case <-batch.done:
		return batch.err
	case <-ctx.Done():
		b.mtx.Lock()
		batch.waiting--
		if batch.waiting == 0 {
			batch.cancel()

			if b.open == batch {
				b.open = nil
			}
		}
		b.mtx.Unlock()

		return ctx.Err()
	}
}

// seal queues the open batch and sends what the calls under way leave room
// for; b.mtx is held.
func (b *blobBatcher) seal() {
	b.queue = append(b.queue, b.open)
	b.open = nil
	b.drain()
}

// drain sends queued batches while fewer than batchCalls are under way;
// b.mtx is held.
func (b *blobBatcher) drain() {
	for b.sending < batchCalls && len(b.queue) > 0 {
		batch := b.queue[0]
		b.queue = b.queue[1:]
		b.sending++

		go b.send(batch)
	}
}

func (b *blobBatcher) send(batch *blobBatch) {
	err := PutObjects(batch.ctx, b.store, batch.objects)
	batch.cancel()

	b.mtx.Lock()
	batch.err = err
	b.sending--

	if b.open != nil && len(b.queue) == 0 {
		b.seal()
	} else {
		b.drain()
	}
	b.mtx.Unlock()

	close(batch.done)
}
