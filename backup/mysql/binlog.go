package mysql

import (
	"bufio"
	"bytes"
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
	h, err := readFormatDescription(r)
	return h.serverID, err
}

// readFormatDescription reads the header of the format description event
// every binlog opens with.
func readFormatDescription(r io.Reader) (eventHeader, error) {
	magic := make([]byte, len(binlogMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return eventHeader{}, fmt.Errorf("reading the binlog magic: %w", err)
	}

	if !bytes.Equal(magic, binlogMagic) {
		return eventHeader{}, errors.New("no binlog magic: not a binlog, or an encrypted one")
	}

	event := make([]byte, eventHeaderLen)
	if _, err := io.ReadFull(r, event); err != nil {
		return eventHeader{}, fmt.Errorf("reading an event header: %w", err)
	}

	if kind := event[4]; kind != formatDescriptionEvent {
		return eventHeader{}, fmt.Errorf("the binlog opens with event type %d, not a format description", kind)
	}

	return parseEventHeader(event)
}

func readBinlogFileStart(path string) (eventHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return eventHeader{}, err
	}
	defer f.Close()

	return readFormatDescription(bufio.NewReader(f))
}
