package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/fileblob"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// shapedConn delays every byte crossing it by delay in each direction and,
// with rate above zero, lets at most rate bytes a second through each way,
// the way a long link between an agent and its store does. It counts the
// bytes it receives into each of received.
type shapedConn struct {
	net.Conn
	delay    time.Duration
	rate     float64
	received []*atomic.Int64

	out  chan packet
	in   chan packet
	done chan struct{}
	once sync.Once

	sendMtx  sync.Mutex
	sendBusy time.Time

	pending []byte
	readErr error
}

// linkQueue is how much a shaped link holds back before it stops reading.
const linkQueue = 5 * time.Millisecond

type packet struct {
	data []byte
	at   time.Time
}

func shape(conn net.Conn, delay time.Duration, rate float64, received ...*atomic.Int64) *shapedConn {
	c := &shapedConn{
		Conn:     conn,
		delay:    delay,
		rate:     rate,
		received: received,
		out:      make(chan packet, 1<<14),
		in:       make(chan packet, 1<<14),
		done:     make(chan struct{}),
	}

	go c.send()
	go c.receive()

	return c
}

// release is when n bytes ready at ready have crossed a link that was
// busy until busy.
func (c *shapedConn) release(busy *time.Time, ready time.Time, n int) time.Time {
	// an idle link keeps a little credit, or every late wakeup of the
	// reader would cost the link that much of its rate
	if idle := ready.Add(-linkQueue); busy.Before(idle) {
		*busy = idle
	}

	if c.rate > 0 {
		*busy = busy.Add(time.Duration(float64(n) / c.rate * float64(time.Second)))
	}

	if busy.Before(ready) {
		return ready.Add(c.delay)
	}

	return busy.Add(c.delay)
}

func (c *shapedConn) send() {
	for {
		select {
		case p := <-c.out:
			time.Sleep(time.Until(p.at))

			if _, err := c.Conn.Write(p.data); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *shapedConn) receive() {
	defer close(c.in)

	var busy time.Time

	for {
		buf := make([]byte, 64<<10)

		n, err := c.Conn.Read(buf)
		if n > 0 {
			for _, r := range c.received {
				r.Add(int64(n))
			}
			c.in <- packet{data: buf[:n], at: c.release(&busy, time.Now(), n)}

			// a link queues little before it drops, so the sender must
			// feel the cap rather than fill an endless buffer
			time.Sleep(time.Until(busy) - linkQueue)
		}

		if err != nil {
			c.readErr = err
			return
		}
	}
}

func (c *shapedConn) Write(p []byte) (int, error) {
	c.sendMtx.Lock()
	at := c.release(&c.sendBusy, time.Now(), len(p))
	c.sendMtx.Unlock()

	select {
	case c.out <- packet{data: append([]byte(nil), p...), at: at}:
		return len(p), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}

func (c *shapedConn) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		pkt, ok := <-c.in
		if !ok {
			if c.readErr == nil {
				return 0, io.EOF
			}

			return 0, c.readErr
		}

		time.Sleep(time.Until(pkt.at))
		c.pending = pkt.data
	}

	n := copy(p, c.pending)
	c.pending = c.pending[n:]

	return n, nil
}

func (c *shapedConn) Close() error {
	c.once.Do(func() { close(c.done) })

	return c.Conn.Close()
}

// slowPack answers every Get after delay, like a store reading its
// objects from a bucket.
type slowPack struct {
	*packIndex
	delay time.Duration
}

func (s slowPack) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	time.Sleep(s.delay)
	return s.packIndex.Get(ctx, ref)
}

// signedBucket hands out URLs into an HTTP server over the bucket's
// directory, the way Cloud Storage signs ranges of its objects.
type signedBucket struct {
	*BucketStore
	url string
}

func (s signedBucket) SignRange(_ context.Context, name string, offset, length int64, ttl time.Duration) (*proto.Location, error) {
	return &proto.Location{
		Url:     s.url + "/" + ArchiveKey(name),
		Header:  map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)},
		Length:  length,
		Expires: timestamppb.New(time.Now().Add(ttl)),
	}, nil
}

// link is the network and bucket a benchmark restores across.
type link struct {
	rtt time.Duration
	// rate caps each connection in bytes a second, 0 for no cap
	rate float64
	// get is the bucket's latency for every read
	get time.Duration
}

func (l link) String() string {
	s := fmt.Sprintf("rtt=%s/get=%s", l.rtt, l.get)
	if l.rate > 0 {
		s += fmt.Sprintf("/cap=%dMBps", int(l.rate/(1<<20)))
	}

	return s
}

// restoreFixture is a directory of archives holding files sealed under
// one key.
type restoreFixture struct {
	dir   string
	key   *storekey.Key
	files []*proto.Ref
	sizes []int64
	total int64
}

func openPack(tb testing.TB, archives pack.ArchiveStorage) *pack.PackStorage {
	tb.Helper()

	store, err := pack.NewPackStorage(
		pack.WithArchiveStorage(archives),
		pack.WithArchiveIndex(pack.NewInMemoryIndex()),
		pack.WithLogger(slog.New(slog.DiscardHandler)),
	)
	require.NoError(tb, err)
	require.NoError(tb, store.Open())

	return store
}

func newRestoreFixture(tb testing.TB, count int, size int64, content func(data []byte, i int)) *restoreFixture {
	tb.Helper()

	sizes := make([]int64, count)
	for i := range sizes {
		sizes[i] = size
	}

	return newSizedFixture(tb, sizes, content)
}

// newSizedFixture stores a file of each of sizes.
func newSizedFixture(tb testing.TB, sizes []int64, content func(data []byte, i int)) *restoreFixture {
	tb.Helper()

	key, err := storekey.Generate("bench")
	require.NoError(tb, err)

	f := &restoreFixture{dir: tb.TempDir(), key: key}

	bucket, err := fileblob.OpenBucket(f.dir, nil)
	require.NoError(tb, err)

	store := openPack(tb, NewBucketStore(bucket))

	for i, size := range sizes {
		data := make([]byte, size)
		content(data, i)

		ref, err := backup.PutFile(context.Background(), store, key, size, &readerOf{data: data})
		require.NoError(tb, err)

		f.files = append(f.files, ref)
		f.sizes = append(f.sizes, size)
		f.total += size
	}

	require.NoError(tb, store.Close())

	return f
}

func random(data []byte, i int) { rand.New(rand.NewSource(int64(i))).Read(data) }

// repeated fills data with one random MiB over and over, so the chunker
// cuts the same incompressible parts from every copy.
func repeated(data []byte, i int) {
	chunk := make([]byte, 1<<20)
	random(chunk, i)

	for at := 0; at < len(data); at += len(chunk) {
		copy(data[at:], chunk)
	}
}

type readerOf struct{ data []byte }

func (r *readerOf) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}

	n := copy(p, r.data)
	r.data = r.data[n:]

	return n, nil
}

// traffic is what a client of serveShaped received: bytes over every
// connection, bytes from the store server alone, requests to the bucket,
// and the calls it made to the store server. While failing is above zero
// the bucket refuses requests, counting it down.
type traffic struct {
	received, served, fetches, calls, failing atomic.Int64
}

// serveShaped runs a TLS store server over f's archives on loopback TCP
// and, when located, a bucket the client reads them from itself. It
// returns a client whose connections all cross l, and what it received.
func serveShaped(tb testing.TB, f *restoreFixture, l link, located bool) (*Client, *traffic) {
	tb.Helper()

	seen := &traffic{}
	dial := func(ctx context.Context, network, addr string, counters ...*atomic.Int64) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return shape(conn, l.rtt/2, l.rate, append(counters, &seen.received)...), nil
	}

	bucket, err := fileblob.OpenBucket(f.dir, nil)
	require.NoError(tb, err)

	buckets := NewBucketStore(bucket)
	var archives pack.ArchiveStorage = buckets
	var httpClient *http.Client

	if located {
		files := http.FileServer(http.Dir(f.dir))
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen.fetches.Add(1)
			time.Sleep(l.get)

			if seen.failing.Add(-1) >= 0 {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}

			files.ServeHTTP(w, r)
		}))
		server.EnableHTTP2 = true
		server.StartTLS()
		tb.Cleanup(server.Close)

		transport := server.Client().Transport.(*http.Transport).Clone()
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) { return dial(ctx, network, addr) }
		transport.ForceAttemptHTTP2 = true
		httpClient = &http.Client{Transport: transport}

		archives = signedBucket{BucketStore: buckets, url: server.URL}
	}

	packs := openPack(tb, archives)
	tb.Cleanup(func() { _ = packs.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	index := slowPack{packIndex: &packIndex{PackStorage: packs, latest: map[string]*proto.Ref{}}, delay: l.get}
	remote := NewServer(NewStore(index, nil))
	serverTLS, clientTLS := testTLS(tb)
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(serverTLS)),
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(testSecret), remote.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(testSecret), remote.StreamInterceptor()),
	)
	proto.RegisterStoreServer(srv, remote)

	go func() { _ = srv.Serve(listener) }()
	tb.Cleanup(srv.Stop)

	received := &downloads{}
	con, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), append([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
		grpc.WithPerRPCCredentials(auth.Credentials{Secret: testSecret, AgentID: "bench"}),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) { return dial(ctx, "tcp", addr, &seen.served) }),
		grpc.WithStatsHandler(received),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			seen.calls.Add(1)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			seen.calls.Add(1)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}, downloadWindows...)...)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = con.Close() })

	return &Client{store: proto.NewStoreClient(con), admin: adminpb.NewAdminClient(con), HTTP: httpClient, downloads: received}, seen
}

// restoreAll restores every file of f into a new directory, workers files
// at once, and returns where.
func restoreAll(tb testing.TB, restorer *backup.Restorer, f *restoreFixture, workers int) string {
	into := tb.TempDir()

	files, ctx := errgroup.WithContext(context.Background())
	files.SetLimit(workers)

	for i, ref := range f.files {
		files.Go(func() error {
			path := filepath.Join(into, fmt.Sprintf("file%d", i))
			stat := &proto.FileInfo{Size: f.sizes[i], Mode: 0o644, MtimeNs: time.Now().UnixNano()}

			_, err := restorer.RestoreFile(ctx, path, stat, ref)

			return err
		})
	}

	require.NoError(tb, files.Wait())

	return into
}

// benchRestore restores f across l per op and reports the bytes received
// per restored byte beside the throughput.
func benchRestore(b *testing.B, f *restoreFixture, l link, located bool, workers int) {
	client, seen := serveShaped(b, f, l, located)
	restorer := &backup.Restorer{Store: client, Key: f.key, Workers: workers}

	b.SetBytes(f.total)
	b.ResetTimer()

	for range b.N {
		restoreAll(b, restorer, f, workers)
	}

	b.ReportMetric(float64(seen.received.Load())/float64(b.N)/float64(f.total), "wire/logical")
}

var benchLinks = []link{
	{rtt: 30 * time.Millisecond},
	{rtt: 30 * time.Millisecond, get: 20 * time.Millisecond},
	{rtt: 30 * time.Millisecond, get: 20 * time.Millisecond, rate: 100 << 20},
}

// BenchmarkRemoteRestore restores across a shaped link from a real store
// server, the parts served by it or read from the bucket: one large file,
// one made of a single repeated chunk, and many medium ones, eight files
// at once as on a four-core host.
func BenchmarkRemoteRestore(b *testing.B) {
	if testing.Short() {
		b.Skip("restores hundreds of megabytes")
	}

	const workers = 8

	fixtures := []struct {
		name string
		f    func() *restoreFixture
	}{
		{"large", sync.OnceValue(func() *restoreFixture { return newRestoreFixture(b, 1, 256<<20, random) })},
		{"repeated", sync.OnceValue(func() *restoreFixture { return newRestoreFixture(b, 1, 256<<20, repeated) })},
		{"medium", sync.OnceValue(func() *restoreFixture { return newRestoreFixture(b, 64, 4<<20, random) })},
	}

	for _, l := range benchLinks {
		for _, fixture := range fixtures {
			for _, mode := range []string{"proxied", "located"} {
				b.Run(fixture.name+"/"+mode+"/"+l.String(), func(b *testing.B) {
					benchRestore(b, fixture.f(), l, mode == "located", workers)
				})
			}
		}
	}
}

func TestARestoreReadsLocatedPartsFromTheBucket(t *testing.T) {
	f := newRestoreFixture(t, 3, 3<<20, random)
	repeats := newRestoreFixture(t, 1, 4<<20, repeated)

	for _, located := range []bool{false, true} {
		client, seen := serveShaped(t, f, link{}, located)
		into := restoreAll(t, &backup.Restorer{Store: client, Key: f.key, Workers: 4}, f, 4)

		for i := range f.files {
			want := make([]byte, f.sizes[i])
			random(want, i)

			got, err := os.ReadFile(filepath.Join(into, fmt.Sprintf("file%d", i)))
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, got), "file %d restores whole", i)
		}

		if located {
			require.NotZero(t, seen.fetches.Load(), "the parts come from the bucket")
		} else {
			require.Zero(t, seen.fetches.Load())
		}

		require.Greater(t, client.Downloaded(), f.total*9/10, "what came from the server and the bucket is counted")
		require.LessOrEqual(t, client.Downloaded(), seen.received.Load(), "and no more than crossed the wire")

		client, seen = serveShaped(t, repeats, link{}, located)
		restoreAll(t, &backup.Restorer{Store: client, Key: repeats.key}, repeats, 1)
		require.Less(t, seen.received.Load(), repeats.total/2, "a part the file repeats crosses the wire once")
	}
}

// newTreeFixture stores a directory of dirs directories, each of subs
// directories of files files, with names as random as sealed ones, and
// returns it beside the root's ref.
func newTreeFixture(tb testing.TB, dirs, subs, files int) (*restoreFixture, *proto.Ref) {
	tb.Helper()

	f := &restoreFixture{dir: tb.TempDir()}

	bucket, err := fileblob.OpenBucket(f.dir, nil)
	require.NoError(tb, err)

	store := openPack(tb, NewBucketStore(bucket))
	rnd := rand.New(rand.NewSource(1))
	ctx := context.Background()

	name := func() []byte {
		b := make([]byte, 32)
		rnd.Read(b)

		return b
	}

	node := func(kind proto.NodeType, ref *proto.Ref) *proto.TreeNode {
		return &proto.TreeNode{
			Stat: &proto.FileInfo{Name: name(), Mode: 0o644, Size: rnd.Int63n(1 << 30), MtimeNs: rnd.Int63(), Type: kind},
			Ref:  ref,
		}
	}

	put := func(nodes []*proto.TreeNode) *proto.Ref {
		slices.SortFunc(nodes, func(a, b *proto.TreeNode) int { return bytes.Compare(a.Stat.Name, b.Stat.Name) })
		tree := proto.NewObject(&proto.Tree{Nodes: nodes})
		require.NoError(tb, store.Put(ctx, tree))

		return tree.Ref()
	}

	var top []*proto.TreeNode
	for range dirs {
		var mid []*proto.TreeNode
		for range subs {
			var leaves []*proto.TreeNode
			for range files {
				leaves = append(leaves, node(proto.NodeType_NODE_FILE, &proto.Ref{Hash: name()}))
			}

			mid = append(mid, node(proto.NodeType_NODE_DIRECTORY, put(leaves)))
		}

		top = append(top, node(proto.NodeType_NODE_DIRECTORY, put(mid)))
	}

	root := put(top)
	require.NoError(tb, store.Close())

	return f, root
}

// BenchmarkRemoteGetTree prefetches a tree of 64 directories of 16
// directories of 64 files two levels deep, as a backup does its base
// commit.
func BenchmarkRemoteGetTree(b *testing.B) {
	f, root := sync.OnceValues(func() (*restoreFixture, *proto.Ref) { return newTreeFixture(b, 64, 16, 64) })()

	for _, l := range append([]link{{}}, benchLinks...) {
		b.Run(l.String(), func(b *testing.B) {
			client, seen := serveShaped(b, f, l, true)
			b.ResetTimer()

			for range b.N {
				objects, err := client.GetTree(context.Background(), root, 2)
				require.NoError(b, err)
				require.Len(b, objects, 1+64+64*16)
			}

			b.ReportMetric(float64(seen.received.Load())/float64(b.N), "wire-B/op")
			b.ReportMetric(float64(seen.served.Load())/float64(b.N), "served-B/op")
		})
	}
}

func TestGetTreeServesTreesTheBucketCouldBeAskedFor(t *testing.T) {
	f, root := newTreeFixture(t, 3, 4, 5)

	for _, located := range []bool{false, true} {
		client, seen := serveShaped(t, f, link{}, located)

		objects, err := client.GetTree(context.Background(), root, 2)
		require.NoError(t, err)
		require.Len(t, objects, 1+3+3*4)

		var dirs int
		for _, obj := range objects {
			require.NotNil(t, obj.GetTree())

			for _, node := range obj.GetTree().GetNodes() {
				if node.GetStat().IsDir() {
					dirs++
				}
			}
		}

		require.Equal(t, 3+3*4, dirs, "every directory's tree is among them")

		_, err = client.Get(context.Background(), root)
		require.NoError(t, err)

		require.Zero(t, seen.fetches.Load(), "the server serves trees itself")
	}
}

func TestWalkTreeReadsTheWholeTreeInOneCall(t *testing.T) {
	f, root := newTreeFixture(t, 3, 4, 5)

	for _, located := range []bool{false, true} {
		client, seen := serveShaped(t, f, link{}, located)

		var dirs, files int
		err := backup.NewBackupReader(client).WalkTree(context.Background(), root, nil, func(_ string, info os.FileInfo, _ *proto.Ref) error {
			if info.IsDir() {
				dirs++
			} else {
				files++
			}

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3+3*4, dirs)
		require.Equal(t, 3*4*5, files)

		require.EqualValues(t, 1, seen.calls.Load(), "one GetTree")
		require.Zero(t, seen.fetches.Load())
	}
}

// mixedSizes are the sizes of count small files: empty, inline, one part
// and a few.
func mixedSizes(count int) []int64 {
	pattern := []int64{0, 100, 4000, 9000, 20_000, 50_000}

	sizes := make([]int64, count)
	for i := range sizes {
		sizes[i] = pattern[i%len(pattern)]
	}

	return sizes
}

// restoreFiles restores every file of f into into through RestoreFiles.
func restoreFiles(tb testing.TB, restorer *backup.Restorer, f *restoreFixture, into string) {
	tb.Helper()

	files := make([]backup.FileToRestore, len(f.files))
	for i, ref := range f.files {
		files[i] = backup.FileToRestore{
			Path: filepath.Join(into, fmt.Sprintf("file%d", i)),
			Stat: &proto.FileInfo{Size: f.sizes[i], Mode: 0o644, MtimeNs: time.Unix(1_700_000_000, 0).UnixNano()},
			Ref:  ref,
		}
	}

	require.NoError(tb, restorer.RestoreFiles(context.Background(), files, func(_ int, _ backup.Outcome, err error) error { return err }))
}

func requireRestored(tb testing.TB, f *restoreFixture, into string) {
	tb.Helper()

	for i := range f.files {
		want := make([]byte, f.sizes[i])
		random(want, i)

		got, err := os.ReadFile(filepath.Join(into, fmt.Sprintf("file%d", i)))
		require.NoError(tb, err)
		require.True(tb, bytes.Equal(want, got), "file %d restores whole", i)

		_, err = os.Lstat(backup.PartialPath(filepath.Join(into, fmt.Sprintf("file%d", i))))
		require.True(tb, os.IsNotExist(err), "file %d leaves no partial file", i)
	}
}

func TestRestoreFilesReadsSmallFilesInGroups(t *testing.T) {
	f := newSizedFixture(t, mixedSizes(3000), random)

	for _, located := range []bool{false, true} {
		client, seen := serveShaped(t, f, link{}, located)
		into := t.TempDir()
		restoreFiles(t, &backup.Restorer{Store: client, Key: f.key, Workers: 8}, f, into)
		requireRestored(t, f, into)

		require.LessOrEqual(t, seen.calls.Load(), int64(20))

		fetches := int64(20)
		if located {
			// each round of sizes leaves a record between the files' that
			// none of them is read from, and a signed run spans no record
			// it was not asked for
			fetches += int64(len(f.files) / 6)
		}

		require.LessOrEqual(t, seen.fetches.Load(), fetches)
	}
}

func TestRestoreFilesFallsBackFileByFileWhenAGroupFails(t *testing.T) {
	f := newSizedFixture(t, mixedSizes(200), random)

	client, seen := serveShaped(t, f, link{}, true)
	seen.failing.Store(1)

	into := t.TempDir()
	restoreFiles(t, &backup.Restorer{Store: client, Key: f.key, Workers: 8}, f, into)
	requireRestored(t, f, into)

	// which files the failed fetch held depends on which fetch came first,
	// so the count is measured against a restore where none fails
	healthy, unbroken := serveShaped(t, f, link{}, true)
	restoreFiles(t, &backup.Restorer{Store: healthy, Key: f.key, Workers: 8}, f, t.TempDir())
	require.Greater(t, seen.calls.Load(), unbroken.calls.Load()+1, "the files were read one by one")
}

func TestRestoreFilesReadsLargeFilesOnTheirOwn(t *testing.T) {
	sizes := append(mixedSizes(300), 3<<20, 2<<20, 5<<20)
	f := newSizedFixture(t, sizes, random)

	client, seen := serveShaped(t, f, link{}, true)
	into := t.TempDir()
	restoreFiles(t, &backup.Restorer{Store: client, Key: f.key, Workers: 8}, f, into)
	requireRestored(t, f, into)

	require.LessOrEqual(t, seen.calls.Load(), int64(1+3*2), "one ReadFiles, and a Get and a ReadFile per large file")
}

func TestRestoreFilesTakesInlineFilesFromTheirObjects(t *testing.T) {
	sizes := make([]int64, 100)
	for i := range sizes {
		sizes[i] = int64(1 + i*proto.InlineLimit/len(sizes))
	}

	f := newSizedFixture(t, sizes, random)

	client, seen := serveShaped(t, f, link{}, true)
	into := t.TempDir()
	restoreFiles(t, &backup.Restorer{Store: client, Key: f.key, Workers: 8}, f, into)
	requireRestored(t, f, into)

	require.EqualValues(t, 1, seen.calls.Load())
	require.Zero(t, seen.fetches.Load(), "an inline file's content comes with its object")
}

func TestRestoreFilesOverADestinationFetchesOnlyWhatItLacks(t *testing.T) {
	sizes := mixedSizes(120)
	sizes[10], sizes[11] = 600_000, 900_000

	f := newSizedFixture(t, sizes, random)
	path := func(into string, i int) string { return filepath.Join(into, fmt.Sprintf("file%d", i)) }

	client, _ := serveShaped(t, f, link{}, true)
	into := t.TempDir()
	restoreFiles(t, &backup.Restorer{Store: client, Key: f.key, Workers: 8}, f, into)

	require.NoError(t, os.Remove(path(into, 4)))
	require.NoError(t, os.WriteFile(path(into, 5), make([]byte, f.sizes[5]), 0o644))
	require.NoError(t, os.Truncate(path(into, 10), f.sizes[10]/2))

	content := make([]byte, f.sizes[11])
	random(content, 11)
	require.NoError(t, os.Remove(path(into, 11)))
	require.NoError(t, os.Mkdir(filepath.Join(into, backup.StagingDir), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(into, backup.StagingDir, backup.StagingMarker), nil, 0o600))
	require.NoError(t, os.WriteFile(backup.PartialPath(path(into, 11)), content[:len(content)/2], 0o600))

	client, seen := serveShaped(t, f, link{}, true)
	restorer := &backup.Restorer{Store: client, Key: f.key, Workers: 8}
	restoreFiles(t, restorer, f, into)
	requireRestored(t, f, into)

	stats := restorer.Stats()
	require.EqualValues(t, len(f.files)-4, stats.Unchanged)
	require.EqualValues(t, 4, stats.Written)
	require.Less(t, stats.BytesFromStore, f.sizes[4]+f.sizes[5]+f.sizes[10]+f.sizes[11])
	require.LessOrEqual(t, seen.calls.Load(), int64(2), "the file objects, then the parts the destination lacks")
}
