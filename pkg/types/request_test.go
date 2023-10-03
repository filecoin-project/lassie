package types

import (
	"testing"

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
		request            RetrievalRequest
		expectedUrlPath    string
		expectedDescriptor string
	}{
		{
			name: "plain",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n",
		},
		{
			name: "path",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Path: "/some/path/to/thing"},
			},
			expectedUrlPath:    "/some/path/to/thing?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/some/path/to/thing?dag-scope=all&dups=n",
		},
		{
			name: "escaped path",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Path: "/?/#/;/&/ /!"},
			},
			expectedUrlPath:    "/%3F/%23/%3B/&/%20/%21?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/%3F/%23/%3B/&/%20/%21?dag-scope=all&dups=n",
		},
		{
			name: "entity",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Scope: trustlessutils.DagScopeEntity},
			},
			expectedUrlPath:    "?dag-scope=entity",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=entity&dups=n",
		},
		{
			name: "block",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV1, Scope: trustlessutils.DagScopeBlock},
			},
			expectedUrlPath:    "?dag-scope=block",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=block&dups=n",
		},
		{
			name: "protocol",
			request: RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV0},
				Protocols: []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=n&protocols=transport-graphsync-filecoinv1",
		},
		{
			name: "protocols",
			request: RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV1},
				Protocols: []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&protocols=transport-bitswap,transport-ipfs-gateway-http",
		},
		{
			name: "duplicates",
			request: RetrievalRequest{
				Request: trustlessutils.Request{Root: testCidV0, Duplicates: true},
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/QmVXsSVjwxMsCwKRCUxEkGb4f4B98gXVy3ih3v4otvcURK?dag-scope=all&dups=y",
		},
		{
			name: "block limit",
			request: RetrievalRequest{
				Request:   trustlessutils.Request{Root: testCidV1},
				MaxBlocks: 100,
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&blockLimit=100",
		},
		{
			name: "fixed peer",
			request: RetrievalRequest{
				Request:    trustlessutils.Request{Root: testCidV1},
				FixedPeers: must(ParseProviderStrings("/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "fixed peers",
			request: RetrievalRequest{
				Request:    trustlessutils.Request{Root: testCidV1},
				FixedPeers: must(ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
			},
			expectedUrlPath:    "?dag-scope=all",
			expectedDescriptor: "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
		},
		{
			name: "byte range",
			request: RetrievalRequest{
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
			request: RetrievalRequest{
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
			request: RetrievalRequest{
				Request: trustlessutils.Request{
					Root:       testCidV0,
					Path:       "/some/path/to/thing",
					Scope:      trustlessutils.DagScopeEntity,
					Duplicates: true,
					Bytes:      &trustlessutils.ByteRange{From: 100, To: ptr(-200)},
				},
				MaxBlocks:  222,
				Protocols:  []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportIpfsGatewayHttp},
				FixedPeers: must(ParseProviderStrings("/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")),
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

	t.Run("fixed peer, no peer ID", func(t *testing.T) {
		pps, err := ParseProviderStrings("/ip4/127.0.0.1/tcp/5000/http")
		require.NoError(t, err)
		request := RetrievalRequest{
			Request:    trustlessutils.Request{Root: testCidV1},
			FixedPeers: pps,
		}
		ds, err := request.GetDescriptorString()
		require.NoError(t, err)
		expectedStart := "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/ip4/127.0.0.1/tcp/5000/http/p2p/1TunknownX"
		require.Equal(t, expectedStart, ds[0:len(expectedStart)])
	})

	t.Run("fixed peer, http:// URL", func(t *testing.T) {
		for _, p := range []string{"", "/", "///"} {
			t.Run("w/ path=["+p+"]", func(t *testing.T) {
				pps, err := ParseProviderStrings("http://127.0.0.1:5000" + p)
				require.NoError(t, err)
				request := RetrievalRequest{
					Request:    trustlessutils.Request{Root: testCidV1},
					FixedPeers: pps,
				}
				ds, err := request.GetDescriptorString()
				require.NoError(t, err)
				expectedStart := "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=all&dups=n&providers=/ip4/127.0.0.1/tcp/5000/http/p2p/1TunknownX"
				require.Equal(t, expectedStart, ds[0:len(expectedStart)])
			})
		}
	})

	t.Run("fixed peer, http:// URL with path err", func(t *testing.T) {
		_, err := ParseProviderStrings("http://127.0.0.1:5000/nope")
		require.ErrorContains(t, err, "paths not supported")
	})
}

func TestProviderStrings(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expectMatch string // regex
		expectErr   string
	}{
		{
			name:      "empty",
			input:     "",
			expectErr: "failed to parse multiaddr",
		},
		{
			name:        "single",
			input:       "/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg",
			expectMatch: "^/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg$",
		},
		{
			name:        "multi",
			input:       "/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/dns4/dag.w3s.link/tcp/443/https/p2p/QmUA9D3H7HeCYsirB3KmPSvZh3dNXMZas6Lwgr4fv1HTTp",
			expectMatch: "^/dns/beep.boop.com/tcp/3747/p2p/12D3KooWDXAVxjSTKbHKpNk8mFVQzHdBDvR4kybu582Xd4Zrvagg,/dns4/dag.w3s.link/tcp/443/https/p2p/QmUA9D3H7HeCYsirB3KmPSvZh3dNXMZas6Lwgr4fv1HTTp$",
		},
		{
			name:        "no id",
			input:       "/ip4/127.0.0.1/tcp/5000/http",
			expectMatch: "^/ip4/127.0.0.1/tcp/5000/http/p2p/1TunknownX",
		},
		{
			name:        "http:// form",
			input:       "http://127.0.0.1:5000",
			expectMatch: "^/ip4/127.0.0.1/tcp/5000/http/p2p/1TunknownX",
		},
		{
			name:      "http:// form with path err",
			input:     "http://127.0.0.1:5000/boop",
			expectErr: "invalid provider URL, paths not supported:",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseProviderStrings(tc.input)
			if tc.expectErr != "" {
				require.Contains(t, err.Error(), tc.expectErr)
				return
			} else {
				require.NoError(t, err)
			}

			actual, err := ToProviderString(parsed)
			require.NoError(t, err)

			require.Regexp(t, tc.expectMatch, actual)
		})
	}
}

func TestUnknownPeerID(t *testing.T) {
	for i := 0; i < 1000; i++ {
		p := nextUnknownPeerID()
		require.Equal(t, "1TunknownX", p.String()[0:10])
		require.True(t, IsUnknownPeerID(p))
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
