/*
Package coordinators contains retrievers that coordinate multiple child retrievers
*/
package coordinators

import (
	"errors"

	"github.com/filecoin-project/lassie/pkg/types"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("lassie/retriever/coordinators")

func Coordinator(kind types.CoordinationKind) (types.RetrievalCoordinator, error) {
	switch kind {
	case types.RaceCoordination:
		return Race, nil
	case types.SequentialCoordination:
		return Sequence, nil
	default:
		return nil, errors.New("unrecognized retriever kind")
	}
}
