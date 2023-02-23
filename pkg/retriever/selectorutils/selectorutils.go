package selectorutils

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
)

// PathToSelector converts a standard IPLD path to a selector that matches the
// whole path (inclusive) and the complete DAG at its termination.
//
// Paths must start with a '/'.
//
// The empty path "" is treated as a special case and will match the entire
// DAG.
func PathToSelector(path string) (ipld.Node, error) {
	if len(path) == 0 {
		return selectorparse.CommonSelector_ExploreAllRecursively, nil
	}

	if path[0] != '/' {
		return nil, fmt.Errorf("path must start with /")
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selspec, err := textselector.SelectorSpecFromPath(
		textselector.Expression(path),
		true, // match path inclusive
		// match everything below the path:
		ssb.ExploreRecursive(
			selector.RecursionLimitNone(),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path as a selector '%s': %w", path, err)
	}
	return selspec.Node(), nil
}
