package backup

import (
	"maps"
	"runtime/debug"
)

// MetaVersion is the commit metadata key holding the Version that wrote the
// commit.
const MetaVersion = "goback.version"

const module = "github.com/gobackio/goback"

var buildVersion = readVersion()

// Version returns the goback module version the running program was built
// with, whether goback is the program or a library it links, and is empty
// when the build records none.
func Version() string { return buildVersion }

func readVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	return moduleVersion(info)
}

func moduleVersion(info *debug.BuildInfo) string {
	if info.Main.Path == module {
		return info.Main.Version
	}

	for _, dep := range info.Deps {
		if dep.Path != module {
			continue
		}

		if dep.Replace != nil {
			return dep.Replace.Version
		}

		return dep.Version
	}

	return ""
}

// commitMetadata is what a commit of this run records: the walker's Metadata
// and the Version writing it.
func (w *Walker) commitMetadata() map[string]string {
	version := Version()
	if version == "" {
		return w.Metadata
	}

	metadata := maps.Clone(w.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}

	metadata[MetaVersion] = version

	return metadata
}
