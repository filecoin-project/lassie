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

// explore interpret-as (~), next (>), recursive (R), explore (a), next (>),
// recursive edge (@), recursion limit depth 0, interpreted as unixfs-preload
var exploreShallowJson = `{"~":{">":{"R":{":>":{"a":{">":{"@":{}}}},"l":{"depth":0}}},"as":"unixfs-preload"}}`

func TestPathToSelector(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		expectedErr      string
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
			expextedSelector: exploreShallowJson,
			full:             false,
		},
		{
			name:        "no leading slash",
			path:        "nope",
			expectedErr: "path must start with /",
			full:        true,
		},
		{
			name:        "no leading slash shallow",
			path:        "nope",
			expectedErr: "path must start with /",
			full:        false,
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
			expextedSelector: manualJsonFieldStart("foo") + exploreShallowJson + manualJsonFieldEnd(1),
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
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + exploreShallowJson + manualJsonFieldEnd(2),
			full:             false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel, err := selectorutils.UnixfsPathToSelector(tc.path, tc.full)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
				return
			}
			require.NoError(t, err)
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
