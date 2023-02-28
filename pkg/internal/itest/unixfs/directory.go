package unixfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
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
	require.Equal(t, a.Path, b.Path)
	require.Equal(t, a.Content, b.Content, a.Path+" content mismatch")
	require.Equal(t, a.Root, b.Root, a.Path+" root mismatch")
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
