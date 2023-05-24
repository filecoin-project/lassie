package testutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/stretchr/testify/require"
)

// ToBlocks makes a block array from ordered blocks in a traversal
func ToBlocks(t *testing.T, lsys linking.LinkSystem, root cid.Cid, selNode datamodel.Node) []blocks.Block {
	sel, err := selector.CompileSelector(selNode)
	require.NoError(t, err)
	traversedBlocks := make([]blocks.Block, 0)
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	osro := lsys.StorageReadOpener
	lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		r, err := osro(lc, l)
		if err != nil {
			return nil, err
		}
		byts, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		blk, err := blocks.NewBlockWithCid(byts, l.(cidlink.Link).Cid)
		traversedBlocks = append(traversedBlocks, blk)
		return bytes.NewReader(byts), nil
	}
	var proto datamodel.NodePrototype = basicnode.Prototype.Any
	if root.Prefix().Codec == cid.DagProtobuf {
		proto = dagpb.Type.PBNode
	}
	rootNode, err := lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: root}, proto)
	require.NoError(t, err)
	prog := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: dagpb.AddSupportToChooser(basicnode.Chooser),
		},
	}
	vf := func(p traversal.Progress, n datamodel.Node, vr traversal.VisitReason) error { return nil }
	err = prog.WalkAdv(rootNode, sel, vf)
	require.NoError(t, err)

	return traversedBlocks
}
