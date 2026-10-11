package mysql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

var binlogMagic = []byte{0xfe, 'b', 'i', 'n'}

const (
	eventHeaderLen = 19

	formatDescriptionEvent = 15
	previousGTIDsEvent     = 35

	// LOG_EVENT_BINLOG_IN_USE_F, which the server clears when it closes
	// the file
	binlogInUse = 0x1
)

// binlogHeader is what the events every binlog opens with say about it.
type binlogHeader struct {
	// created is when the server opened the binlog
	created  time.Time
	serverID uint32
	inUse    bool
	// end is where the first event past the opening ones starts: a binlog
	// whose position is end holds nothing yet.
	end int64
}

// readBinlogHeader reads a binlog's magic, its format description event
// and the previous GTIDs event MySQL 5.6 and later write after it.
func readBinlogHeader(r io.Reader) (binlogHeader, error) {
	var hdr binlogHeader

	magic := make([]byte, len(binlogMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return hdr, fmt.Errorf("reading the binlog magic: %w", err)
	}

	if !bytes.Equal(magic, binlogMagic) {
		return hdr, errors.New("no binlog magic: not a binlog, or an encrypted one")
	}

	hdr.end = int64(len(binlogMagic))

	for first := true; ; first = false {
		event := make([]byte, eventHeaderLen)

		_, err := io.ReadFull(r, event)
		// an active binlog can end anywhere past its opening events
		if !first && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
			return hdr, nil
		}

		if err != nil {
			return hdr, fmt.Errorf("reading an event header: %w", err)
		}

		kind := event[4]
		size := int64(binary.LittleEndian.Uint32(event[9:]))

		if first {
			if kind != formatDescriptionEvent {
				return hdr, fmt.Errorf("the binlog opens with event type %d, not a format description", kind)
			}

			hdr.created = time.Unix(int64(binary.LittleEndian.Uint32(event)), 0)
			hdr.serverID = binary.LittleEndian.Uint32(event[5:])
			hdr.inUse = binary.LittleEndian.Uint16(event[17:])&binlogInUse != 0
		} else if kind != previousGTIDsEvent {
			return hdr, nil
		}

		if size < eventHeaderLen {
			return hdr, fmt.Errorf("an event of %d bytes", size)
		}

		if _, err := io.CopyN(io.Discard, r, size-eventHeaderLen); err != nil {
			return hdr, fmt.Errorf("reading an event: %w", err)
		}

		hdr.end += size
	}
}

func readBinlogFile(path string) (binlogHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return binlogHeader{}, err
	}
	defer f.Close()

	return readBinlogHeader(bufio.NewReader(f))
}
