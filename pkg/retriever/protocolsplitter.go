package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/multiformats/go-multicodec"
)

type ProtocolSplitter struct {
	protocols []multicodec.Code
}

func NewProtocolSplitter(protocols []multicodec.Code) *ProtocolSplitter {
	return &ProtocolSplitter{protocols: protocols}
}

func (ps *ProtocolSplitter) SplitCandidates(ctx context.Context, request types.RetrievalRequest, candidates []types.RetrievalCandidate, events func(types.RetrievalEvent)) ([][]types.RetrievalCandidate, error) {
	protocolCandidates := make([][]types.RetrievalCandidate, len(ps.protocols))
	for _, candidate := range candidates {
		candidateProtocolsArr := candidate.Metadata.Protocols()
		candidateProtocolsSet := make(map[multicodec.Code]struct{})
		for _, candidateProtocol := range candidateProtocolsArr {
			candidateProtocolsSet[candidateProtocol] = struct{}{}
		}
		for i, protocol := range ps.protocols {
			if _, ok := candidateProtocolsSet[protocol]; ok {
				protocolCandidates[i] = append(protocolCandidates[i], candidate)
			}
		}
	}
	return protocolCandidates, nil
}
