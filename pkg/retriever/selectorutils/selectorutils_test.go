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

var matchAllJson = mustDagJson(selectorparse.CommonSelector_ExploreAllRecursively)

func TestPathToSelector(t *testing.T) {
	testCases := []struct {
		name             string
		path             string
		expectedErr      string
		expextedSelector string
	}{
		{
			name:             "empty path",
			path:             "",
			expextedSelector: matchAllJson,
		},
		{
			name:        "no leading slash",
			path:        "nope",
			expectedErr: "path must start with /",
		},
		{
			name:             "single field",
			path:             "/foo",
			expextedSelector: manualJsonFieldStart("foo") + matchAllJson + manualJsonFieldEnd(1),
		},
		{
			name:             "multiple fields",
			path:             "/foo/bar",
			expextedSelector: manualJsonFieldStart("foo") + manualJsonFieldStart("bar") + matchAllJson + manualJsonFieldEnd(2),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel, err := selectorutils.PathToSelector(tc.path)
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
	// 1. union (|) of match current;
	// 2. and explore field (f);
	// 3. + specific field (f>);
	// 4 with field name
	return fmt.Sprintf(`{"|":[{".":{}},{"f":{"f>":{"%s":`, name)
}

func manualJsonFieldEnd(fieldCount int) string {
	// close all of the above
	return strings.Repeat("}}}]}", fieldCount)
}

func mustDagJson(n ipld.Node) string {
	byts, err := ipld.Encode(n, dagjson.Encode)
	if err != nil {
		panic(err)
	}
	return string(byts)
}
