/*
Package coordinators contains retrievers that coordinate multiple child retrievers
*/
package coordinators

import (
	"errors"

	"github.com/filecoin-project/lassie/pkg/types"
)

func Coordinator(kind types.CoordinationKind) (types.RetrievalCoordinator, error) {
	switch kind {
	case types.RaceCoordination:
		return Race, nil
	case types.SequentialCoordination:
		return Sequence, nil
	case types.AsyncCoordination:
		return Async, nil
	default:
		return nil, errors.New("unrecognized retriever kind")
	}
}
