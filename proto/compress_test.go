package proto

import (
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAFrameClaimingTooMuchContentFailsToDecode(t *testing.T) {
	// a zstd frame with a 1 KiB window, an eight-byte content size and a
	// single empty raw block
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xc0, 0x00}
	frame = binary.LittleEndian.AppendUint64(frame, 1<<30)
	frame = append(frame, 0x01, 0x00, 0x00)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	_, err := Decode(frame, Compression_ZSTD)
	require.Error(t, err)

	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20), "decoding allocated what the frame claims")
}
