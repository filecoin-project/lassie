package retriever

import (
	"context"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type ProtocolSplitter struct {
	protocols []multicodec.Code
}

var _ types.CandidateSplitter[multicodec.Code] = (*ProtocolSplitter)(nil)

func NewProtocolSplitter(protocols []multicodec.Code) types.CandidateSplitter[multicodec.Code] {
	return &ProtocolSplitter{protocols: protocols}
}

func (ps *ProtocolSplitter) SplitRetrievalRequest(ctx context.Context, request types.RetrievalRequest, events func(types.RetrievalEvent)) types.RetrievalSplitter[multicodec.Code] {
	filteredPeers := make(map[peer.ID]peerFilter, len(request.FilteredPeers))
	for _, filteredPeer := range request.FilteredPeers {
		existing := filteredPeers[filteredPeer.Peer]
		excludeAll := existing.excludeAll || filteredPeer.ExcludeAll
		protocolSet := existing.protocolsSet
		if protocolSet == nil {
			protocolSet = make(map[multicodec.Code]struct{})
		}
		for _, protocol := range filteredPeer.ExcludedProtocols {
			protocolSet[protocol] = struct{}{}
		}
		filteredPeers[filteredPeer.Peer] = peerFilter{excludeAll, protocolSet}
	}
	return &retrievalProtocolSplitter{ps, request.GetSupportedProtocols(ps.protocols), filteredPeers}
}

type peerFilter struct {
	excludeAll   bool
	protocolsSet map[multicodec.Code]struct{}
}

type retrievalProtocolSplitter struct {
	*ProtocolSplitter
	protocols     []multicodec.Code
	filteredPeers map[peer.ID]peerFilter
}

func (rps *retrievalProtocolSplitter) SplitCandidates(candidates []types.RetrievalCandidate) (map[multicodec.Code][]types.RetrievalCandidate, error) {
	protocolCandidates := make(map[multicodec.Code][]types.RetrievalCandidate, len(rps.protocols))
	for _, candidate := range candidates {
		filter, isFiltered := rps.filteredPeers[candidate.MinerPeer.ID]
		if isFiltered && filter.excludeAll {
			continue
		}
		candidateProtocolsArr := candidate.Metadata.Protocols()
		candidateProtocolsSet := make(map[multicodec.Code]struct{})
		for _, candidateProtocol := range candidateProtocolsArr {
			if isFiltered {
				if _, ok := filter.protocolsSet[candidateProtocol]; ok {
					continue
				}
			}
			candidateProtocolsSet[candidateProtocol] = struct{}{}
		}
		for _, protocol := range rps.protocols {
			if _, ok := candidateProtocolsSet[protocol]; ok {
				protocolCandidates[protocol] = append(protocolCandidates[protocol], candidate)
			}
		}
	}
	return protocolCandidates, nil
}
