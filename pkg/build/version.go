package build

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"

	"github.com/filecoin-project/lassie/pkg/internal/revision"
)

var (
	defaultVersion string = "v0.0.0"
	// version is the built version.
	// Set with ldflags in .goreleaser.yaml via -ldflags="-X github.com/filecoin-project/lassie/pkg/build.version=v{{.Version}}".
	version string
	// Version returns the current version of the Lassie application
	Version   string
	UserAgent string
)

func init() {
	if version == "" {
		// This is being ran in development, try to grab the latest known version from the version.json file
		var err error
		version, err = readVersionFromFile()
		if err != nil {
			// Use the default version
			version = defaultVersion
		}
	}

	Version = fmt.Sprintf("%s-%s", version, revision.Revision)
	UserAgent = fmt.Sprintf("lassie/%s", Version)
}

// versionJson is used to read the local version.json file
type versionJson struct {
	Version string `json:"version"`
}

// readVersionFromFile reads the version from the version.json file.
// Reading this should be fine in development since the version.json file
// should be present in the project, I hope :)
func readVersionFromFile() (string, error) {
	// Open file
	file, err := os.Open("version.json")
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Decode json into struct
	decoder := json.NewDecoder(file)
	var vJson versionJson
	err = decoder.Decode(&vJson)
	if err != nil {
		return "", err
	}

	// Read version from json
	return vJson.Version, nil
}
