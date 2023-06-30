package types_test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

func TestEtag(t *testing.T) {
	// To generate independent fixtures using Node.js, `npm install xxhash` then
	// in a REPL:
	//
	//   xx = (s) => require('xxhash').hash64(Buffer.from(s), 0).readBigUInt64LE(0).toString(32)
	//
	// then generate the suffix with the expected construction:
	//
	//   xx('/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.dfs')

	testCases := []struct {
		cid      cid.Cid
		path     string
		scope    types.DagScope
		bytes    *types.ByteRange
		dups     bool
		expected string
	}{
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.58mf8vcmd2eo8"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeEntity,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.3t6g88g8u04i6"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeBlock,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.1fe71ua3km0b5"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.4mglp6etuagob"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeEntity,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.fqhsp0g4l66m1"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeBlock,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.8u1ga109k62pp"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeAll,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.8q5lna3r43lgj"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeEntity,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.e4hni8qqgeove"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeBlock,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.7pdc786smhd1n"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeAll,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.bdfv1q76a1oem"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeEntity,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.790m13mh0recp"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeBlock,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.972jmjvd3o3"`,
		},
		// path variations should be normalised
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeAll,
			path:     "some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.8q5lna3r43lgj"`,
		},
		{
			cid:      cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"),
			scope:    types.DagScopeAll,
			path:     "///some//path//to/thing/",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.8q5lna3r43lgj"`,
		},
		{
			cid:      cid.MustParse("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk"),
			scope:    types.DagScopeAll,
			expected: `"bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk.car.9lumqv26cg30t"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			bytes:    &types.ByteRange{From: 0}, // default, not included
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.58mf8vcmd2eo8"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			bytes:    &types.ByteRange{From: 10},
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.560ditjelh0u2"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			bytes:    &types.ByteRange{From: 0, To: ptr(200)},
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.faqf14andvfmb"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeAll,
			bytes:    &types.ByteRange{From: 100, To: ptr(200)},
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.bvebrb14stt94"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeEntity,
			bytes:    &types.ByteRange{From: 100, To: ptr(200)},
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.bq3u6t9t877t3"`,
		},
		{
			cid:      cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK"),
			scope:    types.DagScopeEntity,
			dups:     true,
			bytes:    &types.ByteRange{From: 100, To: ptr(200)},
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.fhf498an52uqb"`,
		},
	}

	for _, tc := range testCases {
		br := ""
		if tc.bytes != nil {
			br = ":" + tc.bytes.String()
		}
		t.Run(fmt.Sprintf("%s:%s:%s:%v%s", tc.cid.String(), tc.path, tc.scope, tc.dups, br), func(t *testing.T) {
			rr := types.RetrievalRequest{
				Cid:        tc.cid,
				Path:       tc.path,
				Scope:      tc.scope,
				Bytes:      tc.bytes,
				Duplicates: tc.dups,
			}
			actual := rr.Etag()
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func ptr(i int64) *int64 {
	return &i
}
