package types_test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

var testCidV1 = cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
var testCidV0 = cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK")

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
			cid:      testCidV0,
			scope:    types.DagScopeAll,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.58mf8vcmd2eo8"`,
		},
		{
			cid:      testCidV0,
			scope:    types.DagScopeEntity,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.3t6g88g8u04i6"`,
		},
		{
			cid:      testCidV0,
			scope:    types.DagScopeBlock,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.1fe71ua3km0b5"`,
		},
		{
			cid:      testCidV0,
			scope:    types.DagScopeAll,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.4mglp6etuagob"`,
		},
		{
			cid:      testCidV0,
			scope:    types.DagScopeEntity,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.fqhsp0g4l66m1"`,
		},
		{
			cid:      testCidV0,
			scope:    types.DagScopeBlock,
			dups:     true,
			expected: `"QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK.car.8u1ga109k62pp"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeAll,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.8q5lna3r43lgj"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeEntity,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.e4hni8qqgeove"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeBlock,
			path:     "/some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.7pdc786smhd1n"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeAll,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.bdfv1q76a1oem"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeEntity,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.790m13mh0recp"`,
		},
		{
			cid:      testCidV1,
			scope:    types.DagScopeBlock,
			path:     "/some/path/to/thing",
			dups:     true,
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.972jmjvd3o3"`,
		},
		// path variations should be normalised
		{
			cid:      testCidV1,
			scope:    types.DagScopeAll,
			path:     "some/path/to/thing",
			expected: `"bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.car.8q5lna3r43lgj"`,
		},
		{
			cid:      testCidV1,
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

func TestRequestStringRepresentations(t *testing.T) {
	testCases := []struct {
		name               string
		request            types.RetrievalRequest
		expectedUrlPath    string
		expectedDescriptor string
	}{
		{
			name: "plain",
			request: types.RetrievalRequest{
				Cid: testCidV1,
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n",
		},
		{
			name: "path",
			request: types.RetrievalRequest{
				Cid:  testCidV1,
				Path: "/some/path/to/thing",
			},
			expectedUrlPath:    "/some/path/to/thing?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/some/path/to/thing?dag-scope=all&dups=n",
		},
		{
			name: "escaped path",
			request: types.RetrievalRequest{
				Cid:  testCidV1,
				Path: "/?/#/;/&/ /!",
			},
			expectedUrlPath:    "/%3F/%23/%3B/&/%20/%21?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/%3F/%23/%3B/&/%20/%21?dag-scope=all&dups=n",
		},
		{
			name: "entity",
			request: types.RetrievalRequest{
				Cid:   testCidV1,
				Scope: types.DagScopeEntity,
			},
			expectedUrlPath:    "?dag-scope=entity&car-scope=file",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity&dups=n",
		},
		{
			name: "block",
			request: types.RetrievalRequest{
				Cid:   testCidV1,
				Scope: types.DagScopeBlock,
			},
			expectedUrlPath:    "?dag-scope=block&car-scope=block",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=block&dups=n",
		},
		{
			name: "protocol",
			request: types.RetrievalRequest{
				Cid:       testCidV0,
				Protocols: []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=n&protocols=transport-graphsync-filecoinv1",
		},
		{
			name: "protocols",
			request: types.RetrievalRequest{
				Cid:       testCidV1,
				Protocols: []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&protocols=transport-bitswap,transport-ipfs-gateway-http",
		},
		{
			name: "duplicates",
			request: types.RetrievalRequest{
				Cid:        testCidV0,
				Duplicates: true,
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=y",
		},
		{
			name: "block limit",
			request: types.RetrievalRequest{
				Cid:       testCidV1,
				MaxBlocks: 100,
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&blockLimit=100",
		},
		{
			name: "fixed peer",
			request: types.RetrievalRequest{
				Cid:        testCidV1,
				FixedPeers: must(types.ParseProviderStrings("/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "fixed peers",
			request: types.RetrievalRequest{
				Cid:        testCidV1,
				FixedPeers: must(types.ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all&car-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "all the things",
			request: types.RetrievalRequest{
				Cid:        testCidV0,
				Path:       "/some/path/to/thing",
				Scope:      types.DagScopeEntity,
				Duplicates: true,
				MaxBlocks:  222,
				Protocols:  []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
				FixedPeers: must(types.ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "/some/path/to/thing?dag-scope=entity&car-scope=file",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK/some/path/to/thing?dag-scope=entity&dups=y&blockLimit=222&protocols=transport-bitswap,transport-ipfs-gateway-http&providers=/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.request.GetUrlPath()
			require.NoError(t, err)
			require.Equal(t, tc.expectedUrlPath, actual)
			actual, err = tc.request.GetDescriptorString()
			require.NoError(t, err)
			require.Equal(t, tc.expectedDescriptor, actual)
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ptr(i int64) *int64 {
	return &i
}
