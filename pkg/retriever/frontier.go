package retriever

import (
	"bytes"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// Frontier tracks pending and seen CIDs during streaming DFS traversal.
type Frontier struct {
	pending []cid.Cid
	seen    map[cid.Cid]struct{}
}

func NewFrontier(root cid.Cid) *Frontier {
	return &Frontier{
		pending: []cid.Cid{root},
		seen:    make(map[cid.Cid]struct{}),
	}
}

func (f *Frontier) Empty() bool {
	return len(f.pending) == 0
}

func (f *Frontier) Pop() cid.Cid {
	if len(f.pending) == 0 {
		return cid.Undef
	}
	n := len(f.pending) - 1
	c := f.pending[n]
	f.pending = f.pending[:n]
	return c
}

func (f *Frontier) Seen(c cid.Cid) bool {
	_, ok := f.seen[c]
	return ok
}

func (f *Frontier) MarkSeen(c cid.Cid) {
	f.seen[c] = struct{}{}
}

// PushAll adds CIDs in reverse order so first child is popped first (DFS).
func (f *Frontier) PushAll(cids []cid.Cid) {
	for i := len(cids) - 1; i >= 0; i-- {
		c := cids[i]
		if _, ok := f.seen[c]; !ok {
			f.pending = append(f.pending, c)
		}
	}
}

func (f *Frontier) Size() int      { return len(f.pending) }
func (f *Frontier) SeenCount() int { return len(f.seen) }

// ExtractLinks returns all CID links from a block based on its codec.
func ExtractLinks(block blocks.Block) ([]cid.Cid, error) {
	c := block.Cid()
	codec := c.Prefix().Codec

	switch codec {
	case cid.Raw:
		return nil, nil
	case cid.DagProtobuf:
		return extractDagPBLinks(block.RawData())
	case cid.DagCBOR:
		return extractDagCBORLinks(block.RawData())
	case cid.DagJSON:
		return extractDagJSONLinks(block.RawData())
	default:
		// unknown codec — try CBOR, fall back to no links
		links, err := extractDagCBORLinks(block.RawData())
		if err != nil {
			return nil, nil
		}
		return links, nil
	}
}

func extractDagPBLinks(data []byte) ([]cid.Cid, error) {
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, data); err != nil {
		return nil, err
	}
	node := nb.Build().(dagpb.PBNode)

	var cids []cid.Cid
	iter := node.Links.ListIterator()
	for !iter.Done() {
		_, linkNode, err := iter.Next()
		if err != nil {
			return nil, err
		}
		link := linkNode.(dagpb.PBLink)
		cids = append(cids, link.Hash.Link().(cidlink.Link).Cid)
	}
	return cids, nil
}

func extractDagCBORLinks(data []byte) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return collectLinks(nb.Build()), nil
}

func extractDagJSONLinks(data []byte) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return collectLinks(nb.Build()), nil
}

func collectLinks(node ipld.Node) []cid.Cid {
	var links []cid.Cid

	switch node.Kind() {
	case ipld.Kind_Link:
		if link, err := node.AsLink(); err == nil {
			if cl, ok := link.(cidlink.Link); ok {
				links = append(links, cl.Cid)
			}
		}
	case ipld.Kind_Map:
		iter := node.MapIterator()
		for !iter.Done() {
			_, v, err := iter.Next()
			if err != nil {
				break
			}
			links = append(links, collectLinks(v)...)
		}
	case ipld.Kind_List:
		iter := node.ListIterator()
		for !iter.Done() {
			_, v, err := iter.Next()
			if err != nil {
				break
			}
			links = append(links, collectLinks(v)...)
		}
	}

	return links
}
