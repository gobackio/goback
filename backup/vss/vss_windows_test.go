//go:build windows

package vss

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestAPathMapsIntoTheShadowCopyOfTheInnermostVolume(t *testing.T) {
	s := &Snapshot{shadows: []shadow{
		{mount: `C:\Mounted\Data\`, device: `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy8`},
		{mount: `C:\`, device: `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7`},
	}}

	for path, want := range map[string]string{
		`C:\`:                      `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7\`,
		`c:\Users\a\file.txt`:      `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7\Users\a\file.txt`,
		`C:\Mounted\Data`:          `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy8\`,
		`C:\mounted\data\x\y.bin`:  `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy8\x\y.bin`,
		`C:\Mounted\DataElsewhere`: `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7\Mounted\DataElsewhere`,
	} {
		got, err := s.Path(path)
		require.NoError(t, err, path)
		require.Equal(t, want, got, path)
	}

	_, err := s.Path(`D:\other`)
	require.Error(t, err)
}

func TestTakingAShadowCopyUnelevatedFailsAsAPrivilegeError(t *testing.T) {
	if windows.GetCurrentProcessToken().IsElevated() {
		t.Skip("elevated")
	}

	_, err := Take(context.Background(), t.TempDir())
	require.ErrorIs(t, err, ErrPrivilege)
}

// TestAShadowCopyReadsTheVolumeAsItWas needs an elevated shell and
// GOBACK_TEST_VSS=1.
func TestAShadowCopyReadsTheVolumeAsItWas(t *testing.T) {
	if os.Getenv("GOBACK_TEST_VSS") != "1" {
		t.Skip("set GOBACK_TEST_VSS=1 in an elevated shell")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
	require.NoError(t, os.WriteFile(path+":extra", []byte("stream"), 0o644))

	snapshot, err := Take(context.Background(), dir)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("after, and longer"), 0o644))

	fsys := snapshot.FS()
	file, err := fsys.Open(context.Background(), path)
	require.NoError(t, err)
	content, err := io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.Equal(t, "before", string(content))

	meta := fsys.(backup.MetadataFS)
	sd, err := meta.Security(context.Background(), path)
	require.NoError(t, err)
	require.NotEmpty(t, sd)

	streams, err := meta.Streams(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, []backup.StreamInfo{{Name: "extra", Size: 6}}, streams)

	stream, err := meta.OpenStream(context.Background(), path, "extra")
	require.NoError(t, err)
	content, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())
	require.Equal(t, "stream", string(content))

	shadowed, err := snapshot.Path(path)
	require.NoError(t, err)
	require.NoError(t, snapshot.Release())

	_, err = os.Stat(shadowed)
	require.Error(t, err, "the shadow copy is gone")
}
