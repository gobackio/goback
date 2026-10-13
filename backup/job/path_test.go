package job

import (
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/stretchr/testify/require"
)

func TestAPathCommitIsOfThePathKindUnlessItsMetaNamesOne(t *testing.T) {
	require.Equal(t, map[string]string{backup.MetaKind: backup.KindPath}, pathMeta(nil))

	meta := map[string]string{"host": "a"}
	require.Equal(t, map[string]string{"host": "a", backup.MetaKind: backup.KindPath}, pathMeta(meta))
	require.Equal(t, map[string]string{"host": "a"}, meta, "the caller's map is left as it was")

	named := map[string]string{backup.MetaKind: "other"}
	require.Equal(t, named, pathMeta(named))
}
