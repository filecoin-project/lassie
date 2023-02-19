package unixfs

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func GenerateFile(t *testing.T, linkSys *linking.LinkSystem, size int) (cid.Cid, []byte) {
	// a file of `size` random bytes, packaged into unixfs DAGs, stored in the remote blockstore
	delimited := io.LimitReader(rand.Reader, int64(size))
	var buf bytes.Buffer
	buf.Grow(size)
	delimited = io.TeeReader(delimited, &buf)
	// "size-256144" sets the chunker, splitting bytes at 256144b boundaries
	root, _, err := builder.BuildUnixFSFile(delimited, "size-256144", linkSys)
	require.NoError(t, err)
	srcData := buf.Bytes()
	rootCid := root.(cidlink.Link).Cid
	return rootCid, srcData
}
