package unixfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/stretchr/testify/require"
)

// DirEntry represents a flattened directory entry, where Path is from the
// root of the directory and Content is the file contents. It is intended
// that a DirEntry slice can be used to represent a full-depth directory without
// needing nesting.
type DirEntry struct {
	Path     string
	Content  []byte
	Root     cid.Cid
	SelfCids []cid.Cid
	TSize    uint64
	Children []DirEntry
}

func (de DirEntry) Size() (int64, error) {
	return int64(de.TSize), nil
}

func (de DirEntry) Link() ipld.Link {
	return cidlink.Link{Cid: de.Root}
}

func ToDirEntry(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid, expectFull bool) DirEntry {
	de := toDirEntryRecursive(t, linkSys, rootCid, "", expectFull)
	return *de
}

func CarBytesLinkSystem(t *testing.T, carReader io.ReaderAt) ipld.LinkSystem {
	reader, err := storage.OpenReadable(carReader)
	require.NoError(t, err)
	linkSys := cidlink.DefaultLinkSystem()
	linkSys.SetReadStorage(reader)
	linkSys.NodeReifier = unixfsnode.Reify
	linkSys.TrustedStorage = true
	return linkSys
}

func CarToDirEntry(t *testing.T, carReader io.ReaderAt, root cid.Cid, expectFull bool) DirEntry {
	return ToDirEntry(t, CarBytesLinkSystem(t, carReader), root, expectFull)
}

func toDirEntryRecursive(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid, name string, expectFull bool) *DirEntry {
	var proto datamodel.NodePrototype = dagpb.Type.PBNode
	if rootCid.Prefix().Codec == cid.Raw {
		proto = basicnode.Prototype.Any
	}
	node, err := linkSys.Load(linking.LinkContext{Ctx: context.TODO()}, cidlink.Link{Cid: rootCid}, proto)
	if expectFull {
		require.NoError(t, err)
	} else if err != nil {
		if e, ok := err.(interface{ NotFound() bool }); ok && e.NotFound() {
			return nil
		}
		require.NoError(t, err)
	}

	if node.Kind() == ipld.Kind_Bytes { // is a file
		byts, err := node.AsBytes()
		require.NoError(t, err)
		return &DirEntry{
			Path:    name,
			Content: byts,
			Root:    rootCid,
		}
	}
	// else is a directory
	children := make([]DirEntry, 0)
	for itr := node.MapIterator(); !itr.Done(); {
		k, v, err := itr.Next()
		require.NoError(t, err)
		childName, err := k.AsString()
		require.NoError(t, err)
		childLink, err := v.AsLink()
		require.NoError(t, err)
		child := toDirEntryRecursive(t, linkSys, childLink.(cidlink.Link).Cid, name+"/"+childName, expectFull)
		children = append(children, *child)
	}
	return &DirEntry{
		Path:     name,
		Root:     rootCid,
		Children: children,
	}
}

func CompareDirEntries(t *testing.T, a, b DirEntry) {
	// t.Log("CompareDirEntries", a.Path, b.Path) // TODO: remove this
	require.Equal(t, a.Path, b.Path)
	require.Equal(t, a.Root.String(), b.Root.String(), a.Path+" root mismatch")
	hashA := sha256.Sum256(a.Content)
	hashB := sha256.Sum256(b.Content)
	require.Equal(t, hex.EncodeToString(hashA[:]), hex.EncodeToString(hashB[:]), a.Path+"content hash mismatch")
	require.Equal(t, len(a.Children), len(b.Children), fmt.Sprintf("%s child length mismatch %d <> %d", a.Path, len(a.Children), len(b.Children)))
	for i := range a.Children {
		// not necessarily in order
		var found bool
		for j := range b.Children {
			if a.Children[i].Path == b.Children[j].Path {
				found = true
				CompareDirEntries(t, a.Children[i], b.Children[j])
			}
		}
		require.True(t, found, fmt.Sprintf("%s child %s not found in b", a.Path, a.Children[i].Path))
	}
}

const WrapPath = "/want2/want1/want0"

// WrapContent embeds the content we want in some random nested content such
// that it's fetchable under the path "/want2/want1/want0" but also contains
// extraneous files in those nested directories.
func WrapContent(t *testing.T, rndReader io.Reader, lsys *ipld.LinkSystem, content DirEntry) DirEntry {
	before := GenerateDirectory(t, lsys, rndReader, 4<<10, false)
	before.Path = "!before"
	// target content goes here
	want := content
	want.Path = "want0"
	after := GenerateFile(t, lsys, rndReader, 4<<11)
	after.Path = "~after"
	want = BuildDirectory(t, lsys, []DirEntry{before, want, after}, false)

	before = GenerateFile(t, lsys, rndReader, 4<<10)
	before.Path = "!before"
	want.Path = "want1"
	after = GenerateDirectory(t, lsys, rndReader, 4<<11, true)
	after.Path = "~after"
	want = BuildDirectory(t, lsys, []DirEntry{before, want, after}, false)

	before = GenerateFile(t, lsys, rndReader, 4<<10)
	before.Path = "!before"
	want.Path = "want2"
	after = GenerateFile(t, lsys, rndReader, 4<<11)
	after.Path = "~after"
	want = BuildDirectory(t, lsys, []DirEntry{before, want, after}, false)

	return want
}

// WrapContentExclusive is the same as WrapContent but doesn't
// include the extraneous files.
func WrapContentExclusive(t *testing.T, rndReader io.Reader, lsys *ipld.LinkSystem, content DirEntry) DirEntry {
	want := content
	want.Path = "want0"
	want = BuildDirectory(t, lsys, []DirEntry{want}, false)

	want.Path = "want1"
	want = BuildDirectory(t, lsys, []DirEntry{want}, false)

	want.Path = "want2"
	want = BuildDirectory(t, lsys, []DirEntry{want}, false)

	return want
}
