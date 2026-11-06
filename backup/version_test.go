package backup

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModuleVersionIsGobacksWhetherItIsTheProgramOrALibrary(t *testing.T) {
	cases := []struct {
		name string
		info debug.BuildInfo
		want string
	}{
		{
			name: "the program",
			info: debug.BuildInfo{Main: debug.Module{Path: module, Version: "v1.2.3"}},
			want: "v1.2.3",
		},
		{
			name: "a library",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "v9.0.0"},
				Deps: []*debug.Module{{Path: "example.com/other", Version: "v8.0.0"}, {Path: module, Version: "v1.2.3"}},
			},
			want: "v1.2.3",
		},
		{
			name: "a replaced library",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{Path: module, Version: "v1.2.3", Replace: &debug.Module{Path: "example.com/fork", Version: "v1.2.4"}}},
			},
			want: "v1.2.4",
		},
		{
			name: "not linked",
			info: debug.BuildInfo{Main: debug.Module{Path: "example.com/app", Version: "v9.0.0"}},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, moduleVersion(&c.info))
		})
	}
}

func TestACommitCarriesTheVersionThatWroteIt(t *testing.T) {
	built := buildVersion
	buildVersion = "v1.2.3"
	t.Cleanup(func() { buildVersion = built })

	f := newWalkerFixture(t)
	f.write("a.txt", []byte("hello"))

	require.Equal(t, "v1.2.3", f.run().Commit.GobackVersion)
}
