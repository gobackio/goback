package storage

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage/mapping/gen"
	"github.com/gobackio/goback/storage/pack"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewClient connects to a store server over TLS, presenting creds
// on every call. A nil tlsConfig trusts the system roots; see ClientTLS
// for a pinned certificate authority.
func NewClient(addr string, creds auth.Credentials, tlsConfig *tls.Config) (*Client, error) {
	return dialStore(addr, creds, credentials.NewTLS(tlsConfig), true)
}

// NewPlaintextClient connects to a store server without TLS, putting the
// credentials and every object on the wire in the clear. It is for a
// store on the same machine as the agent; anywhere else, use NewClient.
// The credentials must allow it.
func NewPlaintextClient(addr string, creds auth.Credentials) (*Client, error) {
	if !creds.Plaintext {
		return nil, errors.New("plaintext credentials are required to dial a store without TLS")
	}

	return dialStore(addr, creds, insecure.NewCredentials(), false)
}

// downloadWindows let a stream have a long link's worth of parts in
// flight; the windows grpc estimates on its own stay far smaller over a
// distant store.
var downloadWindows = []grpc.DialOption{
	grpc.WithInitialWindowSize(16 << 20),
	grpc.WithInitialConnWindowSize(64 << 20),
}

func dialStore(addr string, creds auth.Credentials, transport credentials.TransportCredentials, secure bool) (*Client, error) {
	jar := newCookies(addr, secure)
	received := &downloads{}
	con, err := grpc.NewClient(addr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(creds),
		grpc.WithUnaryInterceptor(jar.unary()),
		grpc.WithStreamInterceptor(jar.stream()),
		grpc.WithStatsHandler(received),
	}, downloadWindows...)...)
	if err != nil {
		return nil, err
	}

	return &Client{
		store:     proto.NewStoreClient(con),
		admin:     adminpb.NewAdminClient(con),
		downloads: received,
		fetching:  semaphore.NewWeighted(runFetchers),
	}, nil
}

// ClientTLS returns a client TLS configuration that trusts only the PEM
// certificates in caFile, or the system roots when caFile is empty.
func ClientTLS(caFile string) (*tls.Config, error) {
	if caFile == "" {
		return nil, nil
	}

	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", caFile)
	}

	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

// ServerTLS returns what a store server listens with: TLS from the
// certificate and key files, or plaintext when the operator declared a
// TLS-terminating proxy in front and gave neither. The TLS configuration
// is nil for plaintext.
func ServerTLS(certFile, keyFile string, plaintextBehindProxy bool) (credentials.TransportCredentials, *tls.Config, error) {
	switch {
	case certFile != "" && keyFile != "":
		if plaintextBehindProxy {
			return nil, nil, errors.New("--plaintext-behind-proxy and --tls-cert are exclusive")
		}

		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, err
		}

		config := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}

		return credentials.NewTLS(config), config, nil
	case certFile != "" || keyFile != "":
		return nil, nil, errors.New("--tls-cert and --tls-key go together")
	case plaintextBehindProxy:
		return insecure.NewCredentials(), nil, nil
	default:
		return nil, nil, errors.New("--tls-cert and --tls-key are required unless --plaintext-behind-proxy is set")
	}
}

var m = gen.MapperImpl{}

var (
	_ backup.KeyEscrow   = (*Client)(nil)
	_ backup.Index       = (*Client)(nil)
	_ backup.Retention   = (*Client)(nil)
	_ backup.PartReader  = (*Client)(nil)
	_ backup.FilesReader = (*Client)(nil)
	_ backup.DirLister   = (*Client)(nil)
	_ backup.CommitGate  = (*Client)(nil)
)

// Client is a backup.Index over a store server's gRPC API.
type Client struct {
	store proto.StoreClient
	admin adminpb.AdminClient

	// HTTP fetches the locations a server answers with; nil uses the
	// default client.
	HTTP *http.Client

	downloads *downloads
	// fetching bounds the runs the client fetches at once, across calls
	fetching *semaphore.Weighted
}

// Open is a no-op; the connection is dialed by NewClient.
func (r *Client) Open() error { return nil }

// Close is a no-op; close the connection instead.
func (r *Client) Close() error { return nil }

// FileInfo implements backup.Index.
func (r *Client) FileInfo(ctx context.Context, set string, name string, notAfter time.Time, count int) ([]*proto.TreeNode, error) {
	ctx = r.outgoing(ctx)

	na := timestamppb.New(notAfter)

	response, err := r.store.FileInfo(ctx, &proto.FileInfoRequest{
		BackupSet: set,
		Path:      name,
		NotAfter:  na,
		Count:     int32(count),
	})

	if err != nil {
		return nil, err
	}

	return response.Files, nil
}

// ReadDir implements backup.DirLister.
func (r *Client) ReadDir(ctx context.Context, set string, dir string, notAfter time.Time) ([]*proto.TreeNode, error) {
	ctx = r.outgoing(ctx)

	response, err := r.store.ReadDir(ctx, &proto.ReadDirRequest{
		BackupSet: set,
		Path:      dir,
		NotAfter:  timestamppb.New(notAfter),
	})

	if err != nil {
		return nil, err
	}

	return response.Entries, nil
}

// CommitInfo implements backup.Index.
func (r *Client) CommitInfo(ctx context.Context, set string, notAfter time.Time, count int) ([]*proto.Commit, error) {
	commits, _, err := r.CommitSizes(ctx, set, notAfter, count)

	return commits, err
}

// CommitSizes implements backup.CommitSizer. A store that measures nothing
// answers each commit with an empty size.
func (r *Client) CommitSizes(ctx context.Context, set string, notAfter time.Time, count int) ([]*proto.Commit, []*proto.CommitSize, error) {
	ctx = r.outgoing(ctx)

	response, err := r.store.CommitInfo(ctx, &proto.CommitInfoRequest{
		BackupSet: set,
		NotAfter:  timestamppb.New(notAfter),
		Count:     int32(count),
	})
	if err != nil {
		return nil, nil, err
	}

	sizes := response.Sizes
	if len(sizes) != len(response.Commits) {
		sizes = make([]*proto.CommitSize, len(response.Commits))
		for i := range sizes {
			sizes[i] = &proto.CommitSize{}
		}
	}

	return response.Commits, sizes, nil
}

// ReIndex is not offered by the server.
func (r *Client) ReIndex(context.Context) (backup.ReIndexReport, error) {
	return backup.ReIndexReport{}, backup.ErrNotImplemented
}

// LatestCommit implements backup.Index.
func (r *Client) LatestCommit(ctx context.Context, set string) (*proto.Ref, error) {
	ctx = r.outgoing(ctx)

	response, err := r.store.LatestCommit(ctx, &proto.LatestCommitRequest{BackupSet: set})
	if err != nil {
		return nil, err
	}

	if response.Ref == nil {
		return nil, backup.ErrNotFound
	}

	return response.Ref, nil
}

// GetTree implements backup.TreeFetcher over the streaming RPC. It returns
// only trees reached from ref through the refs of trees it returns, and
// fails with proto.ErrRefMismatch on any other object the server sends.
func (r *Client) GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error) {
	ctx, cancel := context.WithCancel(r.outgoing(ctx))
	defer cancel()

	stream, err := r.store.GetTree(ctx, &proto.GetTreeRequest{Ref: ref, MaxDepth: maxDepth})
	if err != nil {
		return nil, err
	}

	var (
		mtx      sync.Mutex
		received = map[string]*proto.Object{}
	)

	keep := func(_ *proto.LocatedRecord, obj *proto.Object) error {
		if err := obj.Validate(); err != nil {
			return fmt.Errorf("tree below %x: %w", ref.GetHash(), err)
		}

		hash := string(obj.Ref().GetHash())

		mtx.Lock()
		defer mtx.Unlock()

		received[hash] = obj

		return nil
	}

	runs, rctx := errgroup.WithContext(ctx)

	err = receive(stream, func(resp *proto.GetTreeResponse) error {
		if len(resp.GetRuns()) == 0 {
			return keep(nil, resp.Object)
		}

		for _, run := range resp.GetRuns() {
			runs.Go(func() error { return r.fetchRun(rctx, run, keep) })
		}

		return rctx.Err()
	})
	if err != nil {
		cancel()
	}

	if waited := runs.Wait(); waited != nil && (err == nil || errors.Is(err, context.Canceled)) {
		err = waited
	}

	if err != nil {
		return nil, err
	}

	return reachedTrees(ref, received)
}

// reachedTrees returns the trees of received that ref reaches, root first,
// failing on any received object it does not reach.
func reachedTrees(ref *proto.Ref, received map[string]*proto.Object) ([]*proto.Object, error) {
	objects := make([]*proto.Object, 0, len(received))
	queue := []*proto.Ref{ref}

	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]

		obj, ok := received[string(next.GetHash())]
		if !ok {
			continue
		}

		delete(received, string(next.GetHash()))

		tree := obj.GetTree()
		if tree == nil {
			return nil, fmt.Errorf("%w: object %x below tree %x is a %s", proto.ErrRefMismatch, next.GetHash(), ref.GetHash(), obj.Type())
		}

		objects = append(objects, obj)
		queue = append(queue, tree.Splits...)

		for _, node := range tree.Nodes {
			if node.GetStat().IsDir() {
				queue = append(queue, node.Ref)
			}
		}
	}

	for hash := range received {
		return nil, fmt.Errorf("%w: tree %x does not reach object %x the server sent with it", proto.ErrRefMismatch, ref.GetHash(), []byte(hash))
	}

	return objects, nil
}

// Put implements backup.ObjectStore.
func (r *Client) Put(ctx context.Context, object *proto.Object) error {
	_, err := r.put(ctx, object, nil)

	return err
}

// PutFile implements backup.Confirmer.
func (r *Client) PutFile(ctx context.Context, object *proto.Object, assumed []*proto.Ref) ([]*proto.Ref, error) {
	return r.put(ctx, object, assumed)
}

func (r *Client) put(ctx context.Context, object *proto.Object, assumed []*proto.Ref) ([]*proto.Ref, error) {
	ctx = r.outgoing(ctx)

	err := object.Validate()
	if err != nil {
		return nil, err
	}

	request := &proto.PutRequest{Object: object, AssumedRefs: assumed}
	stamped := object.GetCommit() != nil || object.GetPin() != nil
	if !stamped {
		request.Ref = object.Ref()
	}

	resp, err := r.store.Put(ctx, request)
	if err == nil && stamped && resp.GetObject() != nil {
		// the server assigned the receipt time and set id; the caller's
		// object must hash to the stored ref
		pb.Reset(object)
		pb.Merge(object, resp.Object)
	}

	if status.Code(err) == codes.FailedPrecondition {
		msg := status.Convert(err).Message()
		if strings.Contains(msg, backup.ErrNoSession.Error()) {
			return nil, fmt.Errorf("%w: %s", backup.ErrNoSession, msg)
		}

		return nil, fmt.Errorf("%w: %s", backup.ErrDanglingRef, msg)
	}

	if status.Code(err) == codes.Aborted {
		return nil, fmt.Errorf("%w: %s", backup.ErrSessionLost, status.Convert(err).Message())
	}

	if err != nil {
		return nil, err
	}

	return resp.Missing, nil
}

// Presence implements backup.PresenceSource.
func (r *Client) Presence(ctx context.Context, set string) (presence.Set, error) {
	ctx = r.outgoing(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := r.store.GetPresence(ctx, &proto.GetPresenceRequest{BackupSet: set})
	if err != nil {
		return nil, err
	}

	var (
		order   []uint32
		filters = map[uint32]*proto.PresenceFilter{}
	)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if f, ok := filters[resp.Index]; ok {
			f.Data = append(f.Data, resp.GetFilter().GetData()...)
			continue
		}

		filters[resp.Index] = resp.GetFilter()
		order = append(order, resp.Index)
	}

	result := make(presence.Set, 0, len(order))
	for _, index := range order {
		f, err := presence.FromProto(filters[index])
		if err != nil {
			return nil, err
		}

		result = append(result, f)
	}

	return result, nil
}

// Get implements backup.ObjectStore. A server that answers with a
// location is followed, so where the bytes came from stays its business.
func (r *Client) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	resp, err := r.store.Get(r.outgoing(ctx), &proto.GetRequest{Ref: ref})
	if err != nil {
		return nil, fromStatus(err)
	}

	if location := resp.GetLocation(); location != nil {
		return r.fetch(ctx, ref, location)
	}

	if resp.GetObject() == nil {
		return nil, backup.ErrNotFound
	}

	if err := resp.GetObject().Verify(ref); err != nil {
		return nil, err
	}

	return resp.GetObject(), nil
}

// fetch follows a location and decodes the record it yields, refusing an
// object that is not the one asked for.
func (r *Client) fetch(ctx context.Context, ref *proto.Ref, location *proto.Location) (*proto.Object, error) {
	record, err := r.follow(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("fetching object %x from its location: %w", ref.GetHash(), err)
	}

	object, err := pack.DecodeRecord(record, location.GetAtRestKey())
	if err != nil {
		return nil, fmt.Errorf("decoding object %x from its location: %w", ref.GetHash(), err)
	}

	if err := object.Verify(ref); err != nil {
		return nil, fmt.Errorf("location for %x: %w", ref.GetHash(), err)
	}

	return object, nil
}

// open requests the body of a location.
func (r *Client) open(ctx context.Context, location *proto.Location) (io.ReadCloser, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, location.GetUrl(), nil)
	if err != nil {
		return nil, err
	}

	for name, value := range location.GetHeader() {
		request.Header.Set(name, value)
	}

	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		_ = response.Body.Close()

		return nil, errors.New(response.Status)
	}

	return counted{ReadCloser: response.Body, n: &r.counter().n}, nil
}

// counted adds what is read through it to n.
type counted struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c counted) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n.Add(int64(n))

	return n, err
}

// downloads counts the bytes a client receives: the payloads of the
// server's messages and the bodies of the locations it follows.
type downloads struct {
	n atomic.Int64
}

func (d *downloads) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (d *downloads) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (d *downloads) HandleConn(context.Context, stats.ConnStats)                       {}

func (d *downloads) HandleRPC(_ context.Context, s stats.RPCStats) {
	if in, ok := s.(*stats.InPayload); ok {
		d.n.Add(int64(in.WireLength))
	}
}

func (r *Client) counter() *downloads {
	if r.downloads == nil {
		return &downloads{}
	}

	return r.downloads
}

// Downloaded returns how many bytes the client has received so far, from
// the server and from the locations it followed, as they came over the
// wire.
func (r *Client) Downloaded() int64 {
	return r.counter().n.Load()
}

// follow reads the body of a location, which must be as long as it says.
func (r *Client) follow(ctx context.Context, location *proto.Location) ([]byte, error) {
	body, err := r.open(ctx, location)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	record := make([]byte, location.GetLength())
	if _, err := io.ReadFull(body, record); err != nil {
		return nil, err
	}

	return record, nil
}

// fetchRun follows a run once the client fetches fewer than runFetchers,
// and hands fn each object its body holds with its record, reading the
// body one record at a time.
func (r *Client) fetchRun(ctx context.Context, run *proto.LocatedRun, fn func(*proto.LocatedRecord, *proto.Object) error) error {
	if r.fetching != nil {
		if err := r.fetching.Acquire(ctx, 1); err != nil {
			return err
		}
		defer r.fetching.Release(1)
	}

	location := run.GetLocation()

	body, err := r.open(ctx, location)
	if err != nil {
		return fmt.Errorf("fetching a run of %d records: %w", len(run.GetRecords()), err)
	}
	defer body.Close()

	var (
		at       int64
		previous *proto.LocatedRecord
		record   []byte
	)

	for _, next := range run.GetRecords() {
		// the same record twice is read once
		if previous == nil || next.Offset != previous.Offset || next.Length != previous.Length {
			if next.Offset < at || next.Length <= 0 || next.Offset+next.Length > location.GetLength() {
				return fmt.Errorf("record %d lies outside its run or overlaps the one before", next.Index)
			}

			if _, err := io.CopyN(io.Discard, body, next.Offset-at); err != nil {
				return fmt.Errorf("fetching a run of %d records: %w", len(run.GetRecords()), err)
			}

			record = make([]byte, next.Length)
			if _, err := io.ReadFull(body, record); err != nil {
				return fmt.Errorf("fetching a run of %d records: %w", len(run.GetRecords()), err)
			}

			at = next.Offset + next.Length
		}

		previous = next

		object, err := pack.DecodeRecord(record, location.GetAtRestKey())
		if err != nil {
			return fmt.Errorf("decoding record %d from its run: %w", next.Index, err)
		}

		if err := fn(next, object); err != nil {
			return err
		}
	}

	return nil
}

// EscrowedKeys returns every escrowed copy of the store key the server
// keeps.
func (r *Client) EscrowedKeys(ctx context.Context) ([]backup.EscrowedKey, error) {
	response, err := r.store.EscrowedKeys(r.outgoing(ctx), &proto.EscrowedKeysRequest{})
	if err != nil {
		return nil, err
	}

	kept := make([]backup.EscrowedKey, 0, len(response.GetKeys()))
	for _, k := range response.GetKeys() {
		kept = append(kept, backup.EscrowedKey{KeyID: k.GetKeyId(), Escrowed: k.GetEscrowed()})
	}

	return kept, nil
}

// PutEscrowedKey implements backup.KeyEscrow through the server's Admin
// service, which takes the same credentials as the store.
func (r *Client) PutEscrowedKey(ctx context.Context, key backup.EscrowedKey) error {
	_, err := r.admin.PutEscrowedKey(r.outgoing(ctx), &adminpb.PutEscrowedKeyRequest{KeyId: key.KeyID, Escrowed: string(key.Escrowed)})

	return err
}

// Delete is not offered by the server; retention deletes commits and sets.
func (r *Client) Delete(context.Context, *proto.Ref) error { return backup.ErrNotImplemented }

// Walk is not offered by the server; an agent reads through Get and ReadParts.
func (r *Client) Walk(context.Context, bool, proto.ObjectType, backup.ObjectReceiver) error {
	return backup.ErrNotImplemented
}

// BeginCommit implements backup.CommitGate; a refusal is ErrCommitDenied
// with the server's reason.
func (r *Client) BeginCommit(ctx context.Context, set string) (*backup.CommitGrant, error) {
	resp, err := r.store.BeginCommit(r.outgoing(ctx), &proto.BeginCommitRequest{BackupSet: set})
	if err != nil {
		return nil, err
	}

	if !resp.Allowed {
		return nil, fmt.Errorf("%w: %s", backup.ErrCommitDenied, resp.Reason)
	}

	return &backup.CommitGrant{
		Policy:  m.FromStorePolicy(resp.Policy),
		Rescan:  resp.Rescan,
		Damaged: resp.DamagedPaths,
	}, nil
}

// runFetchers bounds the runs a client fetches at once, across all its
// calls: a range read from a bucket takes most of 100ms whatever its size,
// so a link is filled only with hundreds in flight.
const runFetchers = 256

// ReadParts implements backup.PartReader through the ReadFile stream.
func (r *Client) ReadParts(ctx context.Context, file *proto.Ref, skip []int, fn func(int, *proto.Object) error) error {
	ctx, cancel := context.WithCancel(r.outgoing(ctx))
	defer cancel()

	request := &proto.ReadFileRequest{Ref: file, SkipParts: make([]uint32, 0, len(skip))}
	for _, i := range skip {
		request.SkipParts = append(request.SkipParts, uint32(i))
	}

	stream, err := r.store.ReadFile(ctx, request)
	if err != nil {
		return err
	}

	runs, rctx := errgroup.WithContext(ctx)

	err = receive(stream, func(resp *proto.ReadFileResponse) error {
		if len(resp.GetRuns()) == 0 {
			return fn(int(resp.Index), resp.Object)
		}

		for _, run := range resp.GetRuns() {
			runs.Go(func() error {
				return r.fetchRun(rctx, run, func(record *proto.LocatedRecord, obj *proto.Object) error {
					return fn(int(record.Index), obj)
				})
			})
		}

		return rctx.Err()
	})
	if err != nil {
		cancel()
	}

	if waited := runs.Wait(); waited != nil && (err == nil || errors.Is(err, context.Canceled)) {
		err = waited
	}

	return err
}

// ReadFiles implements backup.FilesReader through the ReadFiles stream.
func (r *Client) ReadFiles(ctx context.Context, files []backup.FileRead, objectsOnly bool, object func(int, *proto.Object) error, part func(int, int, *proto.Object) error) error {
	ctx, cancel := context.WithCancel(r.outgoing(ctx))
	defer cancel()

	request := &proto.ReadFilesRequest{Files: make([]*proto.ReadFilesEntry, len(files)), ObjectsOnly: objectsOnly}
	for i, file := range files {
		entry := &proto.ReadFilesEntry{Ref: file.Ref, SkipParts: make([]uint32, len(file.Skip))}
		for j, skip := range file.Skip {
			entry.SkipParts[j] = uint32(skip)
		}

		request.Files[i] = entry
	}

	stream, err := r.store.ReadFiles(ctx, request)
	if err != nil {
		return err
	}

	runs, rctx := errgroup.WithContext(ctx)

	err = receive(stream, func(resp *proto.ReadFilesResponse) error {
		switch {
		case len(resp.GetRuns()) > 0:
			for _, run := range resp.GetRuns() {
				runs.Go(func() error {
					return r.fetchRun(rctx, run, func(record *proto.LocatedRecord, obj *proto.Object) error {
						return part(int(record.File), int(record.Index), obj)
					})
				})
			}

			return rctx.Err()
		case resp.GetPart():
			return part(int(resp.File), int(resp.Index), resp.Object)
		}

		if int(resp.File) >= len(files) {
			return fmt.Errorf("%w: ReadFiles answered file %d of %d", proto.ErrInvalidObject, resp.File, len(files))
		}

		if err := resp.GetObject().Verify(files[resp.File].Ref); err != nil {
			return err
		}

		return object(int(resp.File), resp.Object)
	})
	if err != nil {
		cancel()
	}

	if waited := runs.Wait(); waited != nil && (err == nil || errors.Is(err, context.Canceled)) {
		err = waited
	}

	return err
}

func receive[T any](stream grpc.ServerStreamingClient[T], fn func(*T) error) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return fromStatus(err)
		}

		if err := fn(resp); err != nil {
			return err
		}
	}
}

// DeleteCommit implements backup.Retention.
func (r *Client) DeleteCommit(ctx context.Context, ref *proto.Ref) error {
	_, err := r.store.DeleteCommit(r.outgoing(ctx), &proto.DeleteCommitRequest{Ref: ref})
	return err
}

// UndeleteCommit implements backup.Retention.
func (r *Client) UndeleteCommit(ctx context.Context, ref *proto.Ref) error {
	_, err := r.store.UndeleteCommit(r.outgoing(ctx), &proto.UndeleteCommitRequest{Ref: ref})
	return err
}

// TrashedCommits implements backup.Retention.
func (r *Client) TrashedCommits(ctx context.Context, set string, before time.Time, limit int) ([]*proto.TrashedCommit, error) {
	request := &proto.ListTrashRequest{BackupSet: set, Limit: int32(limit)}
	if !before.IsZero() {
		request.BeforeNs = before.UnixNano()
	}

	resp, err := r.store.ListTrash(r.outgoing(ctx), request)
	if err != nil {
		return nil, err
	}

	return resp.GetCommits(), nil
}

// GetPolicy is the named set's retention.
func (r *Client) GetPolicy(ctx context.Context, set string) (index.SetRetention, error) {
	resp, err := r.store.GetRetention(r.outgoing(ctx), &proto.GetRetentionRequest{BackupSet: set})
	if err != nil {
		return index.SetRetention{}, err
	}

	return m.FromSetRetention(resp), nil
}

// SetPolicy replaces the named set's own policy, nil leaving it to the
// store's default, and reports whether the server holds the change until
// someone confirms it.
func (r *Client) SetPolicy(ctx context.Context, set string, p *retention.Policy) (pending bool, err error) {
	resp, err := r.store.SetRetention(r.outgoing(ctx), &proto.SetRetentionRequest{BackupSet: set, Policy: m.RetentionPolicy(p)})
	if err != nil {
		return false, err
	}

	return resp.GetPending(), nil
}

// CountCommits implements backup.Retention.
func (r *Client) CountCommits(ctx context.Context, set string, period proto.Period, from, to time.Time, zone string, deleted bool) ([]*proto.CommitCount, error) {
	resp, err := r.store.CountCommits(r.outgoing(ctx), &proto.CountCommitsRequest{
		BackupSet: set, Period: period, FromNs: unixNanos(from), ToNs: unixNanos(to), TimeZone: zone, Deleted: deleted,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetCounts(), nil
}

// DeleteSet implements backup.Retention.
func (r *Client) DeleteSet(ctx context.Context, set string, erase bool) error {
	_, err := r.store.DeleteSet(r.outgoing(ctx), &proto.DeleteSetRequest{BackupSet: set, Erase: erase})
	return err
}

// UndeleteSet implements backup.Retention.
func (r *Client) UndeleteSet(ctx context.Context, set string) error {
	_, err := r.store.UndeleteSet(r.outgoing(ctx), &proto.UndeleteSetRequest{BackupSet: set})
	return err
}

// Unpin implements backup.Retention.
func (r *Client) Unpin(ctx context.Context, pin *proto.Ref) error {
	_, err := r.store.Unpin(r.outgoing(ctx), &proto.UnpinRequest{Pin: pin})
	return err
}

// Pins implements backup.Retention.
func (r *Client) Pins(ctx context.Context) ([]*proto.PinInfo, error) {
	resp, err := r.store.ListPins(r.outgoing(ctx), &proto.ListPinsRequest{})
	if err != nil {
		return nil, err
	}

	return resp.GetPins(), nil
}

// Has is not offered by the server; presence filters answer it.
func (r *Client) Has(context.Context, *proto.Ref) (bool, error) {
	return false, backup.ErrNotImplemented
}

// NewServer serves store over gRPC.
func NewServer(store *Store) *Server {
	return &Server{store: store}
}

var _ proto.StoreServer = (*Server)(nil)

// Server is the gRPC adapter of a Store: every RPC is one Store
// operation and a translation of its errors to status codes.
type Server struct {
	proto.UnsafeStoreServer
	store *Store
}

// BeginCommit implements proto.StoreServer.
func (r *Server) BeginCommit(ctx context.Context, request *proto.BeginCommitRequest) (*proto.BeginCommitResponse, error) {
	grant, err := r.store.BeginCommit(ctx, request.BackupSet)
	if errors.Is(err, backup.ErrCommitDenied) {
		return &proto.BeginCommitResponse{Allowed: false, Reason: err.Error()}, nil
	}

	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.BeginCommitResponse{
		Allowed:      true,
		Policy:       m.StorePolicy(grant.Policy),
		Rescan:       grant.Rescan,
		DamagedPaths: grant.Damaged,
	}, nil
}

// FileInfo implements proto.StoreServer.
func (r *Server) FileInfo(ctx context.Context, request *proto.FileInfoRequest) (*proto.FileInfoResponse, error) {
	err := request.NotAfter.CheckValid()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	files, err := r.store.Index.FileInfo(ctx, request.BackupSet, request.Path, request.NotAfter.AsTime(), int(request.Count))
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.FileInfoResponse{Files: files}, nil
}

// ReadDir implements proto.StoreServer.
func (r *Server) ReadDir(ctx context.Context, request *proto.ReadDirRequest) (*proto.ReadDirResponse, error) {
	err := request.NotAfter.CheckValid()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	entries, err := r.store.ReadDir(ctx, request.GetBackupSet(), request.GetPath(), request.NotAfter.AsTime())
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.ReadDirResponse{Entries: entries}, nil
}

// CommitInfo implements proto.StoreServer.
func (r *Server) CommitInfo(ctx context.Context, request *proto.CommitInfoRequest) (*proto.CommitInfoResponse, error) {
	err := request.NotAfter.CheckValid()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if sizer, ok := r.store.Index.(backup.CommitSizer); ok {
		commits, sizes, err := sizer.CommitSizes(ctx, request.BackupSet, request.NotAfter.AsTime(), int(request.Count))
		if err != nil {
			return nil, ToStatus(err)
		}

		return &proto.CommitInfoResponse{Commits: commits, Sizes: sizes}, nil
	}

	commits, err := r.store.Index.CommitInfo(ctx, request.BackupSet, request.NotAfter.AsTime(), int(request.Count))
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.CommitInfoResponse{Commits: commits}, nil
}

// Put implements proto.StoreServer.
func (r *Server) Put(ctx context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	receipt, err := r.store.Put(ctx, Upload{Object: request.GetObject(), Ref: request.Ref, Assumed: request.AssumedRefs})
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.PutResponse{Ref: receipt.Ref, Object: receipt.Object, Missing: receipt.Missing}, nil
}

// LatestCommit answers with an empty ref when the set has no commit.
func (r *Server) LatestCommit(ctx context.Context, request *proto.LatestCommitRequest) (*proto.LatestCommitResponse, error) {
	ref, err := r.store.Index.LatestCommit(ctx, request.BackupSet)
	if errors.Is(err, backup.ErrNotFound) {
		return &proto.LatestCommitResponse{}, nil
	}

	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.LatestCommitResponse{Ref: ref}, nil
}

// GetTree implements proto.StoreServer.
func (r *Server) GetTree(request *proto.GetTreeRequest, stream proto.Store_GetTreeServer) error {
	return ToStatus(r.store.Tree(stream.Context(), request.Ref, request.MaxDepth, stream.Send))
}

// Get implements proto.StoreServer.
func (r *Server) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	object, err := r.store.Get(ctx, request.Ref)
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.GetResponse{Body: &proto.GetResponse_Object{Object: object}}, nil
}

// ReadFile streams a file's parts, as runs to fetch or as stored objects,
// running the loop server-side so the client authorises once per file.
func (r *Server) ReadFile(request *proto.ReadFileRequest, stream proto.Store_ReadFileServer) error {
	return ToStatus(r.store.ReadFile(stream.Context(), request.Ref, request.SkipParts, func(resp *proto.ReadFileResponse) error {
		return stream.Send(resp)
	}))
}

// ReadFiles streams many files' objects and then their parts, as runs to
// fetch or as stored objects.
func (r *Server) ReadFiles(request *proto.ReadFilesRequest, stream proto.Store_ReadFilesServer) error {
	return ToStatus(r.store.ReadFiles(stream.Context(), request.GetFiles(), request.GetObjectsOnly(), stream.Send))
}

// ToStatus maps the store's sentinel errors to gRPC codes; an error that
// already is a status passes through.
func ToStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, backup.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, auth.ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, auth.ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, proto.ErrRefMismatch), errors.Is(err, proto.ErrInvalidObject),
		errors.Is(err, backup.ErrInvalidEscrow), errors.Is(err, backup.ErrSetName), errors.Is(err, backup.ErrInvalidCount),
		errors.Is(err, retention.ErrInvalidPolicy):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, backup.ErrDanglingRef), errors.Is(err, backup.ErrSetClosed),
		errors.Is(err, backup.ErrTombstoned), errors.Is(err, backup.ErrNewestCommit),
		errors.Is(err, backup.ErrPinned), errors.Is(err, backup.ErrNoSession), errors.Is(err, backup.ErrCommitDenied),
		errors.Is(err, backup.ErrOtherKeyEscrowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, backup.ErrNotImplemented):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, backup.ErrSessionLost):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, backup.ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	}

	return err
}

// fromStatus maps the codes a read answers with back to the store's
// sentinel errors, keeping the server's message.
func fromStatus(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return backup.ErrNotFound
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: %s", backup.ErrQuotaExceeded, status.Convert(err).Message())
	}

	return err
}

// DeleteCommit implements proto.StoreServer.
func (r *Server) DeleteCommit(ctx context.Context, request *proto.DeleteCommitRequest) (*proto.DeleteCommitResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.DeleteCommitResponse{}, ToStatus(ret.DeleteCommit(ctx, request.GetRef()))
}

// UndeleteCommit implements proto.StoreServer.
func (r *Server) UndeleteCommit(ctx context.Context, request *proto.UndeleteCommitRequest) (*proto.UndeleteCommitResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.UndeleteCommitResponse{}, ToStatus(ret.UndeleteCommit(ctx, request.GetRef()))
}

// ListTrash implements proto.StoreServer.
func (r *Server) ListTrash(ctx context.Context, request *proto.ListTrashRequest) (*proto.ListTrashResponse, error) {
	if request.GetLimit() < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must not be negative")
	}

	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	var before time.Time
	if request.GetBeforeNs() != 0 {
		before = time.Unix(0, request.GetBeforeNs())
	}

	commits, err := ret.TrashedCommits(ctx, request.GetBackupSet(), before, int(request.GetLimit()))
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.ListTrashResponse{Commits: commits}, nil
}

// GetRetention implements proto.StoreServer.
func (r *Server) GetRetention(ctx context.Context, request *proto.GetRetentionRequest) (*proto.GetRetentionResponse, error) {
	policies, err := r.store.Policies()
	if err != nil {
		return nil, ToStatus(err)
	}

	got, err := policies.GetPolicy(ctx, request.GetBackupSet())
	if err != nil {
		return nil, ToStatus(err)
	}

	return m.SetRetention(got), nil
}

// SetRetention implements proto.StoreServer; the change applies at once.
func (r *Server) SetRetention(ctx context.Context, request *proto.SetRetentionRequest) (*proto.SetRetentionResponse, error) {
	policies, err := r.store.Policies()
	if err != nil {
		return nil, ToStatus(err)
	}

	if err := policies.SetPolicy(ctx, request.GetBackupSet(), m.FromRetentionPolicy(request.GetPolicy())); err != nil {
		return nil, ToStatus(err)
	}

	return &proto.SetRetentionResponse{}, nil
}

// CountCommits implements proto.StoreServer.
func (r *Server) CountCommits(ctx context.Context, request *proto.CountCommitsRequest) (*proto.CountCommitsResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	counts, err := ret.CountCommits(ctx, request.GetBackupSet(), request.GetPeriod(),
		fromUnixNanos(request.GetFromNs()), fromUnixNanos(request.GetToNs()), request.GetTimeZone(), request.GetDeleted())
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.CountCommitsResponse{Counts: counts}, nil
}

// unixNanos is t on the wire, 0 for the zero time.
func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.UnixNano()
}

// fromUnixNanos reads what unixNanos wrote.
func fromUnixNanos(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}

	return time.Unix(0, ns)
}

// DeleteSet implements proto.StoreServer.
func (r *Server) DeleteSet(ctx context.Context, request *proto.DeleteSetRequest) (*proto.DeleteSetResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.DeleteSetResponse{}, ToStatus(ret.DeleteSet(ctx, request.GetBackupSet(), request.GetErase()))
}

// UndeleteSet implements proto.StoreServer.
func (r *Server) UndeleteSet(ctx context.Context, request *proto.UndeleteSetRequest) (*proto.UndeleteSetResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.UndeleteSetResponse{}, ToStatus(ret.UndeleteSet(ctx, request.GetBackupSet()))
}

// Unpin implements proto.StoreServer.
func (r *Server) Unpin(ctx context.Context, request *proto.UnpinRequest) (*proto.UnpinResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.UnpinResponse{}, ToStatus(ret.Unpin(ctx, request.GetPin()))
}

// ListPins implements proto.StoreServer.
func (r *Server) ListPins(ctx context.Context, _ *proto.ListPinsRequest) (*proto.ListPinsResponse, error) {
	ret, err := r.store.Retention()
	if err != nil {
		return nil, ToStatus(err)
	}

	pins, err := ret.Pins(ctx)
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.ListPinsResponse{Pins: pins}, nil
}

// EscrowedKeys implements proto.StoreServer.
func (r *Server) EscrowedKeys(ctx context.Context, _ *proto.EscrowedKeysRequest) (*proto.EscrowedKeysResponse, error) {
	escrow, err := r.store.Escrow()
	if err != nil {
		return nil, ToStatus(err)
	}

	kept, err := escrow.EscrowedKeys(ctx)
	if err != nil {
		return nil, ToStatus(err)
	}

	response := &proto.EscrowedKeysResponse{}
	for _, k := range kept {
		response.Keys = append(response.Keys, &proto.EscrowedKey{KeyId: k.KeyID, Escrowed: k.Escrowed})
	}

	return response, nil
}

// presencePiece is the largest data slice one GetPresence message carries.
const presencePiece = 1 << 20

// GetPresence streams the head filters of the caller's scope, each split
// into pieces that share an index.
func (r *Server) GetPresence(request *proto.GetPresenceRequest, stream proto.Store_GetPresenceServer) error {
	filters, err := r.store.Presence(stream.Context(), request.BackupSet)
	if err != nil {
		return ToStatus(err)
	}

	for i, filter := range filters {
		data := filter.Data
		header := pb.Clone(filter).(*proto.PresenceFilter)

		for first := true; first || len(data) > 0; first = false {
			n := len(data)
			if n > presencePiece {
				n = presencePiece
			}

			piece := &proto.PresenceFilter{}
			if first {
				piece = header
			}

			piece.Data = data[:n]

			err = stream.Send(&proto.GetPresenceResponse{Index: uint32(i), Filter: piece})
			if err != nil {
				return err
			}

			data = data[n:]
		}
	}

	return nil
}
