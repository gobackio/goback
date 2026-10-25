package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// presenceCounting is a Server that counts the filter pieces GetPresence
// sends.
type presenceCounting struct {
	*Server
	pieces atomic.Int64
}

func (p *presenceCounting) GetPresence(request *proto.GetPresenceRequest, stream proto.Store_GetPresenceServer) error {
	return p.Server.GetPresence(request, &countedPresence{Store_GetPresenceServer: stream, pieces: &p.pieces})
}

type countedPresence struct {
	proto.Store_GetPresenceServer
	pieces *atomic.Int64
}

func (c *countedPresence) Send(resp *proto.GetPresenceResponse) error {
	if resp.GetFilter() != nil {
		c.pieces.Add(1)
	}

	return c.Store_GetPresenceServer.Send(resp)
}

func TestPresenceFiltersAreFetchedOnlyOnceTheyChange(t *testing.T) {
	index := newMemIndex()
	remote := NewServer(NewStore(index, nil))
	counting := &presenceCounting{Server: remote}
	dial := serveRemote(t, remote, counting)
	ctx := context.Background()
	dir := t.TempDir()

	one := presence.New(10)
	one.Add(testHash("one"))
	index.filters = []*proto.PresenceFilter{one.Proto()}

	client := dial(testSecret, "node-1")
	client.presence.dir = dir

	for range 2 {
		filters, err := client.Presence(ctx, "world")
		require.NoError(t, err)
		require.True(t, filters.Test(testHash("one")))
		require.EqualValues(t, 1, counting.pieces.Load(), "unchanged filters are fetched once")
	}

	two := presence.New(10)
	two.Add(testHash("two"))

	index.mtx.Lock()
	index.filters = []*proto.PresenceFilter{one.Proto(), two.Proto()}
	index.mtx.Unlock()

	filters, err := client.Presence(ctx, "world")
	require.NoError(t, err)
	require.Len(t, filters, 2)
	require.True(t, filters.Test(testHash("two")))
	require.EqualValues(t, 3, counting.pieces.Load(), "rebuilt filters are fetched again")

	restarted := dial(testSecret, "node-1")
	restarted.presence.dir = dir

	filters, err = restarted.Presence(ctx, "world")
	require.NoError(t, err)
	require.Len(t, filters, 2)
	require.True(t, filters.Test(testHash("one")))
	require.True(t, filters.Test(testHash("two")))
	require.EqualValues(t, 3, counting.pieces.Load(), "a new client reads the filters its cache directory holds")

	filters, err = restarted.Presence(ctx, "elsewhere")
	require.NoError(t, err)
	require.Len(t, filters, 2)
	require.EqualValues(t, 5, counting.pieces.Load(), "each set's filters are held apart")
}

func TestPresenceIsAnsweredWhenTheScopeHasNoFilters(t *testing.T) {
	_, dial := startServer(t)
	client := dial("node-1")

	for range 2 {
		filters, err := client.Presence(context.Background(), "world")
		require.NoError(t, err)
		require.Empty(t, filters)
	}
}
