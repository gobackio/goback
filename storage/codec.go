package storage

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	gproto "google.golang.org/protobuf/proto"
)

// exactCodec is grpc's proto codec, except that a request goes into a
// buffer of its own size: grpc's pools round a 64 KiB chunk up to 1 MiB,
// which each upload in flight holds until the link takes it.
type exactCodec struct {
	encoding.CodecV2
}

func (c exactCodec) Marshal(v any) (mem.BufferSlice, error) {
	m, ok := v.(gproto.Message)
	if !ok {
		return c.CodecV2.Marshal(v)
	}

	data, err := gproto.Marshal(m)
	if err != nil {
		return nil, err
	}

	return mem.BufferSlice{mem.SliceBuffer(data)}, nil
}

var exactRequests = grpc.WithDefaultCallOptions(grpc.ForceCodecV2(exactCodec{encoding.GetCodecV2(grpcproto.Name)}))
