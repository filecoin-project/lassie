package metadata_test

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/lassie/pkg/httputil/metadata"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

var testCid = cid.MustParse("bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4")

func TestCarMetadataRoundtrip(t *testing.T) {
	path := "/birb.mp4"
	orig := metadata.CarMetadata{
		Metadata: &metadata.Metadata{
			Request: metadata.Request{
				Root:       testCid,
				Path:       &path,
				Scope:      types.DagScopeAll,
				Duplicates: true,
			},
			Properties: &types.CarProperties{
				CarBytes:          202020,
				DataBytes:         101010,
				BlockCount:        303,
				ChecksumMultihash: testCid.Hash(),
			},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, orig.Serialize(&buf))

	t.Log("metadata dag-json:", buf.String())

	var roundtrip metadata.CarMetadata
	require.NoError(t, roundtrip.Deserialize(&buf))
	require.Equal(t, orig, roundtrip)
	require.NotNil(t, roundtrip.Metadata)
	require.Equal(t, testCid, roundtrip.Metadata.Request.Root)
	require.NotNil(t, roundtrip.Metadata.Request.Path)
	require.Equal(t, "/birb.mp4", *roundtrip.Metadata.Request.Path)
	require.Equal(t, types.DagScopeAll, roundtrip.Metadata.Request.Scope)
	require.True(t, roundtrip.Metadata.Request.Duplicates)
	require.NotNil(t, roundtrip.Metadata.Properties)
	require.Nil(t, roundtrip.Metadata.Error)
	require.Equal(t, int64(202020), roundtrip.Metadata.Properties.CarBytes)
	require.Equal(t, int64(101010), roundtrip.Metadata.Properties.DataBytes)
	require.Equal(t, int64(303), roundtrip.Metadata.Properties.BlockCount)
	require.Equal(t, []byte(testCid.Hash()), roundtrip.Metadata.Properties.ChecksumMultihash)
}

func TestCarMetadataErrorRoundtrip(t *testing.T) {
	path := "/birb.mp4"
	msg := "something bad happened"
	orig := metadata.CarMetadata{
		Metadata: &metadata.Metadata{
			Request: metadata.Request{
				Root:       testCid,
				Path:       &path,
				Scope:      types.DagScopeAll,
				Duplicates: true,
			},
			Error: &msg,
		},
	}
	var buf bytes.Buffer
	require.NoError(t, orig.Serialize(&buf))

	t.Log("metadata dag-json:", buf.String())

	var roundtrip metadata.CarMetadata
	require.NoError(t, roundtrip.Deserialize(&buf))
	require.Equal(t, orig, roundtrip)
	require.NotNil(t, roundtrip.Metadata)
	require.Equal(t, testCid, roundtrip.Metadata.Request.Root)
	require.NotNil(t, roundtrip.Metadata.Request.Path)
	require.Equal(t, "/birb.mp4", *roundtrip.Metadata.Request.Path)
	require.Equal(t, types.DagScopeAll, roundtrip.Metadata.Request.Scope)
	require.True(t, roundtrip.Metadata.Request.Duplicates)
	require.Nil(t, roundtrip.Metadata.Properties)
	require.NotNil(t, roundtrip.Metadata.Error)
	require.Equal(t, "something bad happened", *roundtrip.Metadata.Error)
}

func TestBadMetadata(t *testing.T) {
	testCases := []struct {
		name string
		byts string
		err  string
	}{
		{"empty", `{}`, `union structure constraints for CarMetadata caused rejection: a union must have exactly one entry`},
		{"bad key", `{"not metadata":true}`, `union structure constraints for CarMetadata caused rejection: no member named "not metadata"`},
		{
			"bad multihash",
			`{"car-metadata/v1":{"properties":{"block_count":303,"car_bytes":202020,"checksum":{"/":{"bytes":"bm90IGEgbXVsdGloYXNo"}},"data_bytes":101010},"request":{"dups":true,"path":"/birb.mp4","root":{"/":"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4"},"scope":"all"}}}`,
			`invalid CarMetadata: checksum multihash:`,
		},
		{
			"no properties or error",
			`{"car-metadata/v1":{"request":{"dups":true,"path":"/birb.mp4","root":{"/":"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4"},"scope":"all"}}}`,
			`invalid CarMetadata: must contain either properties or error fields`,
		},
		{
			"both properties and error",
			`{"car-metadata/v1":{"error":"something bad happened","properties":{"block_count":303,"car_bytes":202020,"checksum":{"/":{"bytes":"EiBd9neBCasGxUmysJN7nGza4ylHikmbsP2+nXs6BlIpvw"}},"data_bytes":101010},"request":{"dups":true,"path":"/birb.mp4","root":{"/":"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4"},"scope":"all"}}}`,
			`invalid CarMetadata: must contain either properties or error fields, not both`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var roundtrip metadata.CarMetadata
			require.ErrorContains(t, roundtrip.Deserialize(bytes.NewBuffer([]byte(tc.byts))), tc.err)
		})
	}
}
