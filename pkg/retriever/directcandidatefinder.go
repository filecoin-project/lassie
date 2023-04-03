package retriever

import (
	"context"
	"sync"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/lassie/pkg/internal/lp2ptransports"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	gsnet "github.com/ipfs/go-graphsync/network"
	bsnet "github.com/ipfs/go-libipfs/bitswap/network"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var _ CandidateFinder = &DirectCandidateFinder{}

// DirectCandidateFinder finds candidate protocols from a fixed set of peers
type DirectCandidateFinder struct {
	h         host.Host
	providers []peer.AddrInfo
}

// NewDirectCandidateFinder returns a new DirectCandidateFinder for the given providers
func NewDirectCandidateFinder(h host.Host, providers []peer.AddrInfo) *DirectCandidateFinder {
	return &DirectCandidateFinder{
		h:         h,
		providers: providers,
	}
}

type candidateSender struct {
	ctx              context.Context
	cancel           context.CancelFunc
	rootCid          cid.Cid
	candidateResults chan<- types.FindCandidatesResult
}

func (cs candidateSender) sendCandidate(addr peer.AddrInfo, protocols ...metadata.Protocol) error {
	select {
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	case cs.candidateResults <- types.FindCandidatesResult{Candidate: types.RetrievalCandidate{
		MinerPeer: addr,
		RootCid:   cs.rootCid,
		Metadata:  metadata.Default.New(protocols...),
	}}:
		return nil
	}
}

func (cs candidateSender) sendError(err error) error {
	select {
	case <-cs.ctx.Done():
		return cs.ctx.Err()
	case cs.candidateResults <- types.FindCandidatesResult{Err: err}:
		cs.cancel()
		return nil
	}
}

// FindCandidatesAsync finds supported protocols for each peer
// TODO: Cache the results?
func (d *DirectCandidateFinder) FindCandidatesAsync(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	candidateResults := make(chan types.FindCandidatesResult)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cs := candidateSender{ctx, cancel, c, candidateResults}
	var wg sync.WaitGroup
	for _, provider := range d.providers {
		wg.Add(1)
		provider := provider
		go func() {
			defer wg.Done()
			err := d.h.Connect(ctx, provider)
			// don't add peers that we can't connect to
			if err != nil {
				_ = cs.sendError(err)
				return
			}
			// check for support for Boost libp2p transports protocol
			transportsClient := lp2ptransports.NewTransportsClient(d.h)
			qr, err := transportsClient.SendQuery(ctx, provider.ID)
			if err == nil {
				log.Debugw("retrieving metadata from transports protocol", "peer", provider.ID)
				// if present, construct metadata from Boost libp2p transports response
				d.retrievalCandidatesFromTransportsProtocol(ctx, qr, provider, cs)
			} else {
				log.Debugw("retrieving metadata from libp2p protocol list", "peer", provider.ID)
				// if not present, just make guesses based on list of supported libp2p
				// protocols catalogued via identify protocol
				d.retrievalCandidatesFromProtocolProbing(ctx, provider, cs)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(candidateResults)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-candidateResults:
			if !ok {
				return nil
			}
			if result.Err != nil {
				return result.Err
			}
			cb(result.Candidate)
		}
	}
}

func (d *DirectCandidateFinder) retrievalCandidatesFromProtocolProbing(ctx context.Context, provider peer.AddrInfo, cs candidateSender) {
	var protocols []metadata.Protocol
	s, err := d.h.NewStream(ctx, provider.ID,
		bsnet.ProtocolBitswap,
		bsnet.ProtocolBitswapOneOne,
		bsnet.ProtocolBitswapOneZero,
		bsnet.ProtocolBitswapNoVers,
	)
	if err == nil {
		s.Close()
		protocols = append(protocols, &metadata.Bitswap{})
	}
	// must support both graphsync & data transfer to do graphsync filecoin v1 retrieval
	s, err = d.h.NewStream(ctx, provider.ID,
		gsnet.ProtocolGraphsync_2_0_0)
	if err == nil {
		s.Close()
		s, err = d.h.NewStream(ctx, provider.ID, datatransfer.ProtocolDataTransfer1_2)
		if err == nil {
			s.Close()
			protocols = append(protocols, &metadata.GraphsyncFilecoinV1{})

		}
	}
	_ = cs.sendCandidate(provider, protocols...)
}

func (d *DirectCandidateFinder) retrievalCandidatesFromTransportsProtocol(ctx context.Context, qr *lp2ptransports.QueryResponse, provider peer.AddrInfo, cs candidateSender) {
	for _, protocol := range qr.Protocols {
		// try to parse addr infos directly
		addrs, err := peer.AddrInfosFromP2pAddrs(protocol.Addresses...)
		// if no peer id is present, use provider's id
		if err != nil {
			addrs = []peer.AddrInfo{{
				ID:    provider.ID,
				Addrs: protocol.Addresses,
			}}
		}
		switch protocol.Name {
		case "libp2p":
			for _, addr := range addrs {
				if err := cs.sendCandidate(addr, &metadata.GraphsyncFilecoinV1{}); err != nil {
					return
				}
			}
		case "bitswap":
			for _, addr := range addrs {
				if err := cs.sendCandidate(addr, &metadata.Bitswap{}); err != nil {
					return
				}
			}
		default:
		}
	}
}

func (d *DirectCandidateFinder) FindCandidates(ctx context.Context, c cid.Cid) ([]types.RetrievalCandidate, error) {
	var candidates []types.RetrievalCandidate
	err := d.FindCandidatesAsync(ctx, c, func(nextCandidate types.RetrievalCandidate) {
		candidates = append(candidates, nextCandidate)
	})
	if err != nil {
		return nil, err
	}
	return candidates, nil
}
