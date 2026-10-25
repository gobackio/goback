package proto

import (
	"github.com/klauspost/compress/zstd"
)

// maxDecoded is the most one stored payload decompresses to; a payload
// that claims or yields more fails to decode.
const maxDecoded = 256 << 20

var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecoded))
)

// Encode compresses a payload for storage and reports the codec used; it
// stores the payload as is when compression does not make it smaller.
func Encode(payload []byte) ([]byte, Compression) {
	compressed := zstdEncoder.EncodeAll(payload, make([]byte, 0, len(payload)))
	if len(compressed) >= len(payload) {
		return payload, Compression_NONE
	}

	return compressed, Compression_ZSTD
}

// Decode reverses Encode for any supported codec.
func Decode(stored []byte, compression Compression) ([]byte, error) {
	return decode(stored, compression)
}

func decodeZstd(stored []byte) ([]byte, error) {
	return zstdDecoder.DecodeAll(stored, nil)
}
