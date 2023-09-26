package itest

import (
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

// CarBytesLinkSystem returns a LinkSystem based on a CAR body, it is a simple
// utility wrapper around the go-car storage Readable that is UnixFS aware.
func CarBytesLinkSystem(t *testing.T, carReader io.ReaderAt) ipld.LinkSystem {
	reader, err := storage.OpenReadable(carReader)
	require.NoError(t, err)
	linkSys := cidlink.DefaultLinkSystem()
	linkSys.SetReadStorage(reader)
	linkSys.NodeReifier = unixfsnode.Reify
	linkSys.TrustedStorage = true
	return linkSys
}

// carToDirEntry is ToDirEntry but around a CAR body as the source of data.
func carToDirEntry(t *testing.T, carReader io.ReaderAt, root cid.Cid, rootPath string, expectFull bool) unixfs.DirEntry {
	return unixfs.ToDirEntryFrom(t, CarBytesLinkSystem(t, carReader), root, rootPath, expectFull)
}
