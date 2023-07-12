package fixtures

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/warpfork/go-testmark"
)

var Unixfs20mVarietyRoot = cid.MustParse("bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq")

const file = "internal/testdata/unixfs_20m_variety."

func filepath(typ string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// convert wd to absolute path, normalizing to / separators
	wd = strings.ReplaceAll(wd, "\\", "/")
	rootInd := strings.LastIndex(wd, "/lassie/pkg/")
	if rootInd == -1 {
		return "", fmt.Errorf("could not find root of lassie package")
	}
	filename := wd[:rootInd] + "/lassie/pkg/" + file + typ
	return filename, nil
}

func Unixfs20mVarietyReadableStorage() (storage.ReadableStorage, io.Closer, error) {
	file, err := filepath("car")
	if err != nil {
		return nil, nil, err
	}
	carFile, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	reader, err := carstorage.OpenReadable(carFile)
	if err != nil {
		carFile.Close()
		return nil, nil, err
	}
	return reader, carFile, nil
}

func Unixfs20mVarietyCases() ([]TestCase, error) {
	file, err := filepath("md")
	if err != nil {
		return nil, err
	}
	doc, err := testmark.ReadFile(file)
	if err != nil {
		return nil, err
	}
	doc.BuildDirIndex()
	testCases := make([]TestCase, 0)
	for _, test := range doc.DirEnt.Children["test"].ChildrenList {
		for _, scope := range test.ChildrenList {
			tc, err := ParseCase(test.Name+"/"+scope.Name, dstr(scope, "query"), dstr(scope, "execution"))
			if err != nil {
				return nil, err
			}
			testCases = append(testCases, tc)
		}
	}
	return testCases, nil
}

func dstr(dir *testmark.DirEnt, ch string) string {
	return string(dir.Children[ch].Hunk.Body)
}
