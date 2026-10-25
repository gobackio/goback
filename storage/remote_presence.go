package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/proto"

	"google.golang.org/protobuf/encoding/protodelim"
)

// WithPresenceCache has the client keep the presence filters it fetched in
// dir as well as in memory, so a new process asks only whether they
// changed. dir is the store's own.
func WithPresenceCache(dir string) ClientOption {
	return func(o *clientOptions) { o.presenceDir = dir }
}

// presenceCache holds the filters a client fetched for each set, with the
// digest the server named them by.
type presenceCache struct {
	// dir, when set, keeps them across processes
	dir string

	mtx  sync.Mutex
	held map[string]heldPresence
}

type heldPresence struct {
	digest  []byte
	filters presence.Set
}

// load returns what the cache holds for set, nothing when it holds none.
func (c *presenceCache) load(set string) heldPresence {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if held, ok := c.held[set]; ok {
		return held
	}

	if c.dir == "" {
		return heldPresence{}
	}

	held, err := c.read(set)
	if err != nil {
		return heldPresence{}
	}

	c.remember(set, held)

	return held
}

// keep holds filters, encoded as protos, under digest for set. A cache
// directory that cannot take them leaves them in memory alone.
func (c *presenceCache) keep(set string, digest []byte, filters presence.Set, protos []*proto.PresenceFilter) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.remember(set, heldPresence{digest: digest, filters: filters})

	if c.dir != "" {
		_ = c.write(set, digest, protos)
	}
}

func (c *presenceCache) remember(set string, held heldPresence) {
	if c.held == nil {
		c.held = map[string]heldPresence{}
	}

	c.held[set] = held
}

func (c *presenceCache) path(set string) string {
	sum := sha256.Sum256([]byte(set))

	return filepath.Join(c.dir, hex.EncodeToString(sum[:16]))
}

// read decodes what write wrote: a message naming the digest, then one per
// filter.
func (c *presenceCache) read(set string) (heldPresence, error) {
	file, err := os.Open(c.path(set))
	if err != nil {
		return heldPresence{}, err
	}
	defer file.Close()

	r := bufio.NewReader(file)

	first := &proto.GetPresenceResponse{}
	if err := protodelim.UnmarshalFrom(r, first); err != nil {
		return heldPresence{}, err
	}

	if len(first.GetDigest()) == 0 {
		return heldPresence{}, errors.New("cached presence filters name no digest")
	}

	held := heldPresence{digest: first.GetDigest(), filters: presence.Set{}}

	for {
		msg := &proto.GetPresenceResponse{}
		err := protodelim.UnmarshalFrom(r, msg)
		if errors.Is(err, io.EOF) {
			return held, nil
		}

		if err != nil {
			return heldPresence{}, err
		}

		filter, err := presence.FromProto(msg.GetFilter())
		if err != nil {
			return heldPresence{}, err
		}

		held.filters = append(held.filters, filter)
	}
}

func (c *presenceCache) write(set string, digest []byte, protos []*proto.PresenceFilter) error {
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return err
	}

	file, err := os.CreateTemp(c.dir, "presence-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	w := bufio.NewWriter(file)

	_, err = protodelim.MarshalTo(w, &proto.GetPresenceResponse{Digest: digest})
	for i, filter := range protos {
		if err != nil {
			break
		}

		_, err = protodelim.MarshalTo(w, &proto.GetPresenceResponse{Index: uint32(i), Filter: filter})
	}

	if err == nil {
		err = w.Flush()
	}

	if closeErr := file.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		return err
	}

	return os.Rename(file.Name(), c.path(set))
}
