package backup

import "runtime/debug"

const module = "github.com/gobackio/goback"

var buildVersion = readVersion()

// Version returns the goback module version the running program was built
// with, whether goback is the program or a library it links. It is empty when
// the build records none, and every commit a Walker writes carries it.
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
