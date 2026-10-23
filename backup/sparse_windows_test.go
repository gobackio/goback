package backup

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func setSparse(t *testing.T, file *os.File) {
	t.Helper()

	var returned uint32
	err := windows.DeviceIoControl(windows.Handle(file.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &returned, nil)
	require.NoError(t, err)
}
