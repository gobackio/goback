package pack

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/require"
)

func TestAMarkResultWithLengthsBeyondItsBytesIsRefused(t *testing.T) {
	g := &gcFile{Generation: 3, Snapshot: time.Now(), Current: bitset.New(100).Set(7), Dead: []uint64{1, 2}}
	g.Death.set(3, 100, 1)

	var buf bytes.Buffer
	_, err := g.WriteTo(&buf)
	require.NoError(t, err)

	read := &gcFile{}
	_, err = read.ReadFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.True(t, read.Current.Test(7))
	require.Equal(t, g.Dead, read.Dead)
	require.Equal(t, g.Death, read.Death)

	deadAt := len(gcMagic) + 5*8 + 2
	currentAt := deadAt + 4
	deathAt := buf.Len() - len(g.Death) - 4

	for name, patch := range map[string]func([]byte){
		"bitset":  func(b []byte) { binary.BigEndian.PutUint64(b[currentAt:], 1<<62) },
		"dead":    func(b []byte) { binary.BigEndian.PutUint32(b[deadAt:], 0xffffffff) },
		"deaths":  func(b []byte) { binary.BigEndian.PutUint32(b[deathAt:], 0xffffffff) },
		"bitset+": func(b []byte) { binary.BigEndian.PutUint64(b[currentAt:], 1<<64-1) },
	} {
		corrupt := bytes.Clone(buf.Bytes())
		patch(corrupt)

		_, err := (&gcFile{}).ReadFrom(bytes.NewReader(corrupt))
		require.ErrorIs(t, err, errGCFileLength, name)
	}
}
