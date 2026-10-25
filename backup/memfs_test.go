package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFileReadsAFileIntoMemory(t *testing.T) {
	ctx := context.Background()

	plain := newMemStore()
	data, ref := distinctParts(t, plain, 40)

	got, err := (&Restorer{Store: plain}).ReadFile(ctx, statFor(data), ref)
	require.NoError(t, err)
	require.Equal(t, data, got)

	streaming := &streamingStore{memStore: newMemStore()}
	data, ref = distinctParts(t, streaming.memStore, 40)

	got, err = (&Restorer{Store: streaming}).ReadFile(ctx, statFor(data), ref)
	require.NoError(t, err, "from a store that serves parts only through ReadParts")
	require.Equal(t, data, got)
}

func TestReadFileFailsForAFileLeftOut(t *testing.T) {
	store := newMemStore()
	data := randomData(100, 1)
	ref := putFile(t, store, nil, data)

	lost := statFor(data)
	lost.Lost = true

	_, err := (&Restorer{Store: store}).ReadFile(context.Background(), lost, ref)
	require.ErrorIs(t, err, ErrLost)

	cloud := statFor(data)
	cloud.Attributes = recallAttributes

	_, err = (&Restorer{Store: store}).ReadFile(context.Background(), cloud, ref)
	require.Error(t, err)
}
