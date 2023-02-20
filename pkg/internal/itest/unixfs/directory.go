package unixfs

import (
	"context"
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
	Path    string
	Content []byte
	Cid     cid.Cid
}

func ToDirEntry(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid) []DirEntry {
	return toDirEntryRecursive(t, linkSys, rootCid, "")
}

func toDirEntryRecursive(t *testing.T, linkSys linking.LinkSystem, rootCid cid.Cid, name string) []DirEntry {
	var proto datamodel.NodePrototype = dagpb.Type.PBNode
	if rootCid.Prefix().Codec == cid.Raw {
		proto = basicnode.Prototype.Any
	}
	node, err := linkSys.Load(linking.LinkContext{Ctx: context.TODO()}, cidlink.Link{Cid: rootCid}, proto)
	require.NoError(t, err)

	if node.Kind() == ipld.Kind_Bytes { // is a file
		byts, err := node.AsBytes()
		require.NoError(t, err)
		return []DirEntry{
			{
				Path:    name,
				Content: byts,
				Cid:     rootCid,
			},
		}
	}
	// else is a directory
	entries := make([]DirEntry, 0)
	for itr := node.MapIterator(); !itr.Done(); {
		k, v, err := itr.Next()
		require.NoError(t, err)
		childName, err := k.AsString()
		require.NoError(t, err)
		childLink, err := v.AsLink()
		require.NoError(t, err)
		childEntries := toDirEntryRecursive(t, linkSys, childLink.(cidlink.Link).Cid, name+"/"+childName)
		entries = append(entries, childEntries...)
	}
	return entries
}

func CompareDirEntries(t *testing.T, a, b []DirEntry) {
	require.Equal(t, len(a), len(b))
	for _, aEntry := range a {
		found := false
		for _, bEntry := range b {
			if aEntry.Path == bEntry.Path {
				require.Equal(t, aEntry.Content, bEntry.Content)
				require.Equal(t, aEntry.Cid, bEntry.Cid)
				found = true
				break
			}
		}
		require.True(t, found)
	}
}
