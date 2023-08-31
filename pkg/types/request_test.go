package types_test

import (
	"testing"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

var testCidV1 = cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
var testCidV0 = cid.MustParse("QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK")

func TestRequestStringRepresentations(t *testing.T) {
	// some of the parts of this test are duplicated in go-trustless-utils/tyeps_test.go

	testCases := []struct {
		name               string
		request            types.RetrievalRequest
		expectedUrlPath    string
		expectedDescriptor string
	}{
		{
			name: "plain",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n",
		},
		{
			name: "path",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Path: "/some/path/to/thing"},
			},
			expectedUrlPath:    "/some/path/to/thing?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/some/path/to/thing?dag-scope=all&dups=n",
		},
		{
			name: "escaped path",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Path: "/?/#/;/&/ /!"},
			},
			expectedUrlPath:    "/%3F/%23/%3B/&/%20/%21?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/%3F/%23/%3B/&/%20/%21?dag-scope=all&dups=n",
		},
		{
			name: "entity",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Scope: trustlessutils.DagScopeEntity},
			},
			expectedUrlPath:    "?dag-scope=entity",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity&dups=n",
		},
		{
			name: "block",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Scope: trustlessutils.DagScopeBlock},
			},
			expectedUrlPath:    "?dag-scope=block",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=block&dups=n",
		},
		{
			name: "protocol",
			request: types.RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV0},
				Protocols: []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=n&protocols=transport-graphsync-filecoinv1",
		},
		{
			name: "protocols",
			request: types.RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV1},
				Protocols: []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&protocols=transport-bitswap,transport-ipfs-gateway-http",
		},
		{
			name: "duplicates",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV0, Duplicates: true},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=y",
		},
		{
			name: "block limit",
			request: types.RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV1},
				MaxBlocks: 100,
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&blockLimit=100",
		},
		{
			name: "fixed peer",
			request: types.RetrievalRequest{
				Request:    trustlessutils.Request{Root: testCidV1},
				FixedPeers: must(types.ParseProviderStrings("/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "fixed peers",
			request: types.RetrievalRequest{
				Request:    trustlessutils.Request{Root: testCidV1},
				FixedPeers: must(types.ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "byte range",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{
					Root:  testCidV1,
					Bytes: &trustlessutils.ByteRange{From: 100, To: ptr(200)},
				},
			},
			expectedUrlPath:    "?dag-scope=all&entity-bytes=100:200",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&entity-bytes=100:200&dups=n",
		},
		{
			name: "byte range -ve",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{
					Root:  testCidV1,
					Bytes: &trustlessutils.ByteRange{From: -100},
				},
			},
			expectedUrlPath:    "?dag-scope=all&entity-bytes=-100:*",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&entity-bytes=-100:*&dups=n",
		},
		{
			name: "all the things",
			request: types.RetrievalRequest{
				Request: trustlessutils.Request{
					Root:       testCidV0,
					Path:       "/some/path/to/thing",
					Scope:      trustlessutils.DagScopeEntity,
					Duplicates: true,
					Bytes:      &trustlessutils.ByteRange{From: 100, To: ptr(-200)},
				},
				MaxBlocks:  222,
				Protocols:  []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
				FixedPeers: must(types.ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "/some/path/to/thing?dag-scope=entity&entity-bytes=100:-200",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK/some/path/to/thing?dag-scope=entity&entity-bytes=100:-200&dups=y&blockLimit=222&protocols=transport-bitswap,transport-ipfs-gateway-http&providers=/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.request.Request.UrlPath()
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
