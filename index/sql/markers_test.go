package sql

import (
	"testing"

	"github.com/gobackio/goback/storage/pack/packtest"
	"github.com/gobackio/goback/testing/testpg"

	"github.com/stretchr/testify/require"
)

func TestSessionMarkers(t *testing.T) {
	packtest.TestSessionMarkers(t, openIndex(t, newMemStore()))
}

func TestSessionMarkersOnPostgres(t *testing.T) {
	x := New(testpg.Start(t), newMemStore())
	require.NoError(t, x.Open())
	t.Cleanup(func() { _ = x.Close() })

	packtest.TestSessionMarkers(t, x)
}
