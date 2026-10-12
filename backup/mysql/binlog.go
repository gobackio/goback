package mysql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var binlogMagic = []byte{0xfe, 'b', 'i', 'n'}

const (
	eventHeaderLen = 19

	formatDescriptionEvent = 15
)

// readBinlogServerID reads the server_id of the format description event
// every binlog opens with.
func readBinlogServerID(r io.Reader) (uint32, error) {
	magic := make([]byte, len(binlogMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return 0, fmt.Errorf("reading the binlog magic: %w", err)
	}

	if !bytes.Equal(magic, binlogMagic) {
		return 0, errors.New("no binlog magic: not a binlog, or an encrypted one")
	}

	event := make([]byte, eventHeaderLen)
	if _, err := io.ReadFull(r, event); err != nil {
		return 0, fmt.Errorf("reading an event header: %w", err)
	}

	if kind := event[4]; kind != formatDescriptionEvent {
		return 0, fmt.Errorf("the binlog opens with event type %d, not a format description", kind)
	}

	return binary.LittleEndian.Uint32(event[5:]), nil
}

func readBinlogFileServerID(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return readBinlogServerID(bufio.NewReader(f))
}
