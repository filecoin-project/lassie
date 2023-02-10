package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

type ProtocolSplitter struct {
	protocols []multicodec.Code
}

var _ types.CandidateSplitter = (*ProtocolSplitter)(nil)

func NewProtocolSplitter(protocols []multicodec.Code) *ProtocolSplitter {
	return &ProtocolSplitter{protocols: protocols}
}

func (ps *ProtocolSplitter) SplitRetrieval(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.RetrievalSplitter {
	return &retrievalProtocolSplitter{ps}
}

type retrievalProtocolSplitter struct {
	*ProtocolSplitter
}

func (rps *retrievalProtocolSplitter) SplitCandidates(candidates []types.RetrievalCandidate) ([][]types.RetrievalCandidate, error) {
	protocolCandidates := make([][]types.RetrievalCandidate, len(rps.protocols))
	for _, candidate := range candidates {
		candidateProtocolsArr := candidate.Metadata.Protocols()
		candidateProtocolsSet := make(map[multicodec.Code]struct{})
		for _, candidateProtocol := range candidateProtocolsArr {
			candidateProtocolsSet[candidateProtocol] = struct{}{}
		}
		for i, protocol := range rps.protocols {
			if _, ok := candidateProtocolsSet[protocol]; ok {
				protocolCandidates[i] = append(protocolCandidates[i], candidate)
			}
		}
	}
	return protocolCandidates, nil
}
