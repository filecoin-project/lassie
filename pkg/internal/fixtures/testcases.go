package fixtures

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

type TestCase struct {
	Name         string
	Root         cid.Cid
	Path         string
	Scope        types.DagScope
	Duplicates   bool
	ByteRange    *types.ByteRange
	ExpectedCids []cid.Cid
}

func (tc TestCase) AsQuery() string {
	pp := ipld.ParsePath(tc.Path).String()
	if pp != "" {
		pp = "/" + pp
	}
	br := ""
	if tc.ByteRange != nil && !tc.ByteRange.IsDefault() {
		br = fmt.Sprintf("&entity-bytes=%s", tc.ByteRange.String())
	}
	dup := ""
	if tc.Duplicates {
		dup = "&dups=y"
	}
	return fmt.Sprintf("/ipfs/%s%s?dag-scope=%s%s%s", tc.Root, pp, tc.Scope, br, dup)
}

func ParseCase(name, spec, exec string) (TestCase, error) {
	lines := strings.Split(exec, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	specParts := strings.Split(strings.TrimSpace(spec), "?")
	if len(specParts) != 2 {
		return TestCase{}, errors.New("invalid spec")
	}
	spec = specParts[0]
	query, err := url.ParseQuery(specParts[1])
	if err != nil {
		return TestCase{}, err
	}
	specParts = strings.Split(spec, "/")
	if specParts[0] != "" && specParts[1] != "ipfs" {
		return TestCase{}, errors.New("invalid spec")
	}
	root, err := cid.Parse(specParts[2])
	if err != nil {
		return TestCase{}, err
	}
	path := "/" + ipld.ParsePath(strings.Join(specParts[3:], "/")).String()
	scope, err := types.ParseDagScope(query.Get("dag-scope")) // required
	if err != nil {
		return TestCase{}, err
	}
	duplicates := query.Get("dups") == "y"
	var byteRange *types.ByteRange
	if query.Get("byte-range") != "" {
		if br, err := types.ParseByteRange(query.Get("byte-range")); err != nil {
			return TestCase{}, err
		} else {
			byteRange = &br
		}
	}
	expectedCids := make([]cid.Cid, 0, len(lines))
	for _, line := range lines {
		la := strings.Split(line, "|")
		c := cid.MustParse(strings.TrimSpace(la[0]))
		expectedCids = append(expectedCids, c)
	}
	return TestCase{
		Name:         name,
		Root:         root,
		Path:         path,
		Scope:        scope,
		Duplicates:   duplicates,
		ByteRange:    byteRange,
		ExpectedCids: expectedCids,
	}, nil
}
