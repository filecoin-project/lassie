package selectorutils_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/filecoin-project/lassie/pkg/retriever/selectorutils"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

var exploreAllJson = mustDagJson(selectorparse.CommonSelector_ExploreAllRecursively)

// explore interpret-as (~), next (>), match (.), interpreted as unixfs-preload
var matchShallowJson = `{"~":{">":{".":{}},"as":"unixfs-preload"}}`

func TestPathToSelector(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		expextedSelector string
		full             bool
	}{
		{
			name:             "empty path",
			path:             "",
			expextedSelector: exploreAllJson,
			full:             true,
		},
		{
			name:             "empty path shallow",
			path:             "",
			expextedSelector: matchShallowJson,
			full:             false,
		},
		{
			name:             "single field",
			path:             "/foo",
			expextedSelector: manualJsonFieldStart("foo") + exploreAllJson + manualJsonFieldEnd(1),
			full:             true,
		},
		{
			name:             "single field shallow",
			path:             "/foo",
			expextedSelector: manualJsonFieldStart("foo") + matchShallowJson + manualJsonFieldEnd(1),
			full:             false,
		},
		{
			name:             "multiple fields",
			path:             "/foo/bar",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + exploreAllJson + manualJsonFieldEnd(2),
			full:             true,
		},
		{
			name:             "multiple fields shallow",
			path:             "/foo/bar",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + matchShallowJson + manualJsonFieldEnd(2),
			full:             false,
		},
		{
			name:             "leading slash optional",
			path:             "foo/bar",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + exploreAllJson + manualJsonFieldEnd(2),
			full:             true,
		},
		{
			name:             "trailing slash optional",
			path:             "/foo/bar/",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + exploreAllJson + manualJsonFieldEnd(2),
			full:             true,
		},
		// a go-ipld-prime specific thing, not clearly specified by path spec (?)
		{
			name:             ".. is a field named ..",
			path:             "/foo/../bar/",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("..") + manualJsonFieldStart("bar") + exploreAllJson + manualJsonFieldEnd(3),
			full:             true,
		},
		{
			// a go-ipld-prime specific thing, not clearly specified by path spec
			name:             "redundant slashes ignored",
			path:             "foo///bar",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + exploreAllJson + manualJsonFieldEnd(2),
			full:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel := selectorutils.PathToUnixfsExploreSelector(tc.path, tc.full)
			require.Equal(t, tc.expextedSelector, mustDagJson(sel))
		})
	}
}

func manualJsonFieldStart(name string) string {
	// explore interpret-as (~) next (>), explore field (f) + specific field (f>), with field name
	return fmt.Sprintf(`{"~":{">":{"f":{"f>":{"%s":`, name)
}

func manualJsonFieldEnd(fieldCount int) string {
	// close all of the above and specify "unixfs" for interpret-as
	return strings.Repeat(`}}},"as":"unixfs"}}`, fieldCount)
}

func mustDagJson(n ipld.Node) string {
	byts, err := ipld.Encode(n, dagjson.Encode)
	if err != nil {
		panic(err)
	}
	return string(byts)
}
