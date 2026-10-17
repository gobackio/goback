package postgres

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

const segSize = 16 << 20

func header(timeline uint32, lsn, systemID uint64) []byte {
	buf := make([]byte, 40)
	binary.LittleEndian.PutUint16(buf[0:], 0xD116)
	binary.LittleEndian.PutUint32(buf[4:], timeline)
	binary.LittleEndian.PutUint64(buf[8:], lsn)
	binary.LittleEndian.PutUint64(buf[24:], systemID)
	binary.LittleEndian.PutUint32(buf[32:], segSize)
	binary.LittleEndian.PutUint32(buf[36:], 8192)

	return buf
}

func base(file string, received time.Time) *proto.Commit {
	return &proto.Commit{ReceivedAtNs: received.UnixNano(), Metadata: map[string]string{MetaStartWALFile: file}}
}

func TestTheCutoffIsTheNewestBaseBeforeTheWindowStarts(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	newestFirst := []*proto.Commit{
		base("00000001000000000000000C", now.Add(-day)),
		base("000000010000000000000008", now.Add(-5*day)),
		base("000000010000000000000004", now.Add(-20*day)),
	}

	cutoff, ok := Cutoff(newestFirst, 2*day, now)
	require.True(t, ok)
	require.Equal(t, "000000010000000000000008", cutoff)

	cutoff, ok = Cutoff(newestFirst, 5*day, now)
	require.True(t, ok)
	require.Equal(t, "000000010000000000000008", cutoff, "a base right at the window's start covers it")

	cutoff, ok = Cutoff(newestFirst, day/2, now)
	require.True(t, ok)
	require.Equal(t, "00000001000000000000000C", cutoff)

	cutoff, ok = Cutoff(newestFirst, 30*day, now)
	require.True(t, ok)
	require.Equal(t, "000000010000000000000004", cutoff, "with every base inside the window the oldest one bounds it")

	_, ok = Cutoff(nil, day, now)
	require.False(t, ok)
}

func TestCarryKeepsHistoriesAndWhatFollowsTheCutoff(t *testing.T) {
	carry := Carry("000000020000000000000008")

	require.True(t, carry("00000002.history"))
	require.True(t, carry("000000010000000000000008"), "a lower timeline at the cutoff's position")
	require.True(t, carry("000000020000000000000009.partial"))
	require.False(t, carry("000000020000000000000007"))
	require.False(t, carry("000000010000000000000007.00000028.backup"))
}

func TestASegmentFromAnotherClusterIsRefused(t *testing.T) {
	name := "000000010000000100000002"
	lsn := uint64(1)<<32 | 2*segSize

	id, err := checkSegment(name, bytes.NewReader(header(1, lsn, 42)), 0)
	require.NoError(t, err)
	require.EqualValues(t, 42, id)

	_, err = checkSegment(name, bytes.NewReader(header(1, lsn, 7)), 42)
	require.ErrorIs(t, err, ErrForeignSegment)

	_, err = checkSegment("000000010000000100000003", bytes.NewReader(header(1, lsn, 42)), 42)
	require.Error(t, err, "a segment whose header places it elsewhere")

	_, err = checkSegment("000000020000000100000002", bytes.NewReader(header(1, lsn, 42)), 42)
	require.NoError(t, err, "a new timeline's first segment, begun on the old one")

	_, err = checkSegment(name, bytes.NewReader(header(2, lsn, 42)), 42)
	require.Error(t, err, "a header of a later timeline than the name's")
}

func TestAGapInATimelineIsFound(t *testing.T) {
	require.NoError(t, checkContiguous([]string{
		"0000000100000000000000FE",
		"0000000100000000000000FF",
		"000000010000000100000000",
		"00000002.history",
		"000000020000000100000000.partial",
		"000000020000000100000001",
	}, segSize, nil))

	require.ErrorIs(t, checkContiguous([]string{
		"0000000100000000000000FE",
		"000000010000000100000000",
	}, segSize, nil), ErrGap)
}

func TestAGapIsAcceptedWhereABaseBackupStarts(t *testing.T) {
	names := []string{"0000000100000000000000FE", "000000010000000100000002", "000000010000000100000003"}

	require.ErrorIs(t, checkContiguous(names, segSize, nil), ErrGap)
	require.NoError(t, checkContiguous(names, segSize, map[string]bool{"000000010000000100000002": true}))
}

func TestATimelineHoldsOnePartialSegmentAfterItsLastWholeOne(t *testing.T) {
	require.NoError(t, checkContiguous([]string{
		"000000010000000000000001",
		"000000010000000000000002",
		"000000010000000000000003.partial",
		"000000020000000000000003.partial",
	}, segSize, nil))

	require.ErrorIs(t, checkContiguous([]string{
		"000000010000000000000001",
		"000000010000000000000003.partial",
	}, segSize, nil), ErrGap, "a partial that skips a segment")

	require.ErrorIs(t, checkContiguous([]string{
		"000000010000000000000001",
		"000000010000000000000002.partial",
		"000000010000000000000003.partial",
	}, segSize, nil), ErrGap, "two partials on one timeline")
}

func TestSegmentNamesAndNumbers(t *testing.T) {
	name := segmentName(2, uint64(1)<<32|3*segSize+100, segSize)
	require.Equal(t, "000000020000000100000003", name)
	require.Equal(t, uint32(2), segmentTimeline(name))
	require.Equal(t, (uint64(1)<<32|3*segSize)/segSize, segmentNumber(name, segSize))
}

func TestLSNsReadAsPostgresPrintsThem(t *testing.T) {
	lsn, err := ParseLSN("16/B374D848")
	require.NoError(t, err)
	require.Equal(t, uint64(0x16B374D848), lsn)
	require.Equal(t, "16/B374D848", FormatLSN(lsn))
}
