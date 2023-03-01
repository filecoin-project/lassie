package selectorutils

import (
	"fmt"
	"strings"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// Kind of a combination of these two, with some additional magic sprinkles:
// https://github.com/ipld/go-ipld-selector-text-lite/blob/master/parser.go
// https://github.com/ipfs/go-unixfsnode/blob/9cc15a4574f13f434f2da2cd1afb9de7d0bb3979/signaling.go#L23
// TODO: upstream this, probably to go-unixfsnode

// PathToSelector converts a standard IPLD path to a selector that explores the
// whole UnixFS path (inclusive), and if 'full' is true, the complete DAG at its
// termination, or if not true, only the UnixFS node at its termination (which
// may be an entire sharded directory or file).
//
// Path is optional, but if supplied it must start with a '/'.
//
// This selector does _not_ match, only explore, so it's useful for traversals
// where block loads are important, not where the matcher visitor callback is
// important.
func UnixfsPathToSelector(path string, full bool) (ipld.Node, error) {
	if len(path) > 0 && path[0] != '/' {
		return nil, fmt.Errorf("path must start with /")
	}

	segments, err := pathSegments(path)
	if err != nil {
		return nil, err
	}

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

	for i := len(segments) - 1; i >= 0; i-- {
		// Wrap selector in ExploreFields as we walk back up through the path.
		// We can assume each segment to be a unixfs path section, so we
		// InterpretAs to make sure the node is reified through go-unixfsnode
		// (if possible) and we can traverse through according to unixfs pathing
		// rather than bare IPLD pathing - which also gives us the ability to
		// traverse through HAMT shards.
		ss = ssb.ExploreInterpretAs("unixfs", ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments[i], ss)
			},
		))
	}

	return ss.Node(), nil
}

func pathSegments(path string) ([]string, error) {
	segments := strings.Split(path, "/")
	filtered := make([]string, 0, len(segments))
	for i := 0; i < len(segments); i++ {
		if segments[i] == "" {
			// Allow one leading and one trailing '/' at most
			if i == 0 || i == len(segments)-1 {
				continue
			}
			return nil, fmt.Errorf("invalid empty path segment at position %d", i)
		}
		if segments[i] == "." || segments[i] == ".." {
			return nil, fmt.Errorf("'%s' is unsupported in paths", segments[i])
		}
		filtered = append(filtered, segments[i])
	}
	return filtered, nil
}
