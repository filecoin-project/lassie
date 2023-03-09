package selectorutils

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// PathToUnixfsExploreSelector converts an IPLD path to a selector. The path is
// interpreted according to github.com/ipld/go-ipld-prime/datamodel/Path rules,
// i.e.
//   - leading and trailing slashes are ignored
//   - redundant slashes are ignored
//   - the segment `..` is a field named `..`, same with `.`
//
// When "full" is true, the selector will explore all nodes recursively at the
// termination of the path. When "full" is false, the selector will only match
// the node at the termination of the path, however this match is done using
// the "unixfs-preload" ADL, which will load sharded files and sharded
// directories, but not go further.
//
// This selector is intended to explore, not match, so it's useful for
// traversals where block loads are important, not where the matcher visitor
// callback is important. (Caveat: it will match the final node when "full" is
// false, but this is a special case for ease of use with "unixfs-preload")
func PathToUnixfsExploreSelector(path string, full bool) ipld.Node {
	segments := ipld.ParsePath(path)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	var ss builder.SelectorSpec
	if full {
		// ExploreAllRecursively
		ss = ssb.ExploreRecursive(
			selector.RecursionLimitNone(),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		)
	} else {
		// Match only this node, interpreted as unixfs-preload, which will
		// load sharded files and sharded directories, and not go further.
		ss = ssb.ExploreInterpretAs("unixfs-preload", ssb.Matcher())
	}

	for segments.Len() > 0 {
		// Wrap selector in ExploreFields as we walk back up through the path.
		// We can assume each segment to be a unixfs path section, so we
		// InterpretAs to make sure the node is reified through go-unixfsnode
		// (if possible) and we can traverse through according to unixfs pathing
		// rather than bare IPLD pathing - which also gives us the ability to
		// traverse through HAMT shards.
		ss = ssb.ExploreInterpretAs("unixfs", ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments.Last().String(), ss)
			},
		))
		segments = segments.Pop()
	}

	return ss.Node()
}
