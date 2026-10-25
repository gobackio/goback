package pack

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func makeIndex() IndexFile {
	idx := make(IndexFile, 1000)

	for i := range idx {
		idx[i].Offset = rand.Uint32()
		idx[i].Length = rand.Uint32()
		idx[i].Type = rand.Uint32()
		for j := range idx[i].Sum {
			idx[i].Sum[j] = byte(rand.Intn(256))
		}
	}

	return idx
}

func TestIndex(t *testing.T) {
	buf := new(bytes.Buffer)
	idx := makeIndex()

	_, err := idx.WriteTo(buf)
	assert.Nil(t, err)

	var newIdx IndexFile

	_, err = (&newIdx).ReadFrom(bytes.NewReader(buf.Bytes()))
	assert.Nil(t, err)

	assert.Equal(t, idx, newIdx)
}

func TestAnIndexWhoseCountExceedsItsRecordsFailsToRead(t *testing.T) {
	buf := new(bytes.Buffer)
	_, err := makeIndex().WriteTo(buf)
	assert.Nil(t, err)

	corrupt := buf.Bytes()
	binary.BigEndian.PutUint32(corrupt[len(indexFileMagicBytes):], 0xffffffff)

	var idx IndexFile
	_, err = (&idx).ReadFrom(bytes.NewReader(corrupt))
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
