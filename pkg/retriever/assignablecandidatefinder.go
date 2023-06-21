package retriever

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/internal/candidatebuffer"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
)

type FilterIndexerCandidate func(types.RetrievalCandidate) (bool, types.RetrievalCandidate)

// AssignableCandidateFinder finds and filters candidates for a given retrieval
type AssignableCandidateFinder struct {
	filterIndexerCandidate FilterIndexerCandidate
	candidateFinder        CandidateFinder
	clock                  clock.Clock
}

const BufferWindow = 5 * time.Millisecond

func NewAssignableCandidateFinder(candidateFinder CandidateFinder, filterIndexerCandidate FilterIndexerCandidate) AssignableCandidateFinder {
	return NewAssignableCandidateFinderWithClock(candidateFinder, filterIndexerCandidate, clock.New())
}
func NewAssignableCandidateFinderWithClock(candidateFinder CandidateFinder, filterIndexerCandidate FilterIndexerCandidate, clock clock.Clock) AssignableCandidateFinder {
	return AssignableCandidateFinder{candidateFinder: candidateFinder, filterIndexerCandidate: filterIndexerCandidate, clock: clock}
}
func (acf AssignableCandidateFinder) FindCandidates(ctx context.Context, request types.RetrievalRequest, eventsCallback func(types.RetrievalEvent), onCandidates func([]types.RetrievalCandidate)) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	eventsCallback(events.StartedFindingCandidates(acf.clock.Now(), request.RetrievalID, request.Cid))

	var totalCandidates atomic.Uint64
	candidateBuffer := candidatebuffer.NewCandidateBuffer(func(candidates []types.RetrievalCandidate) {
		eventsCallback(events.CandidatesFound(acf.clock.Now(), request.RetrievalID, request.Cid, candidates))

		acceptableCandidates := make([]types.RetrievalCandidate, 0)
		for _, candidate := range candidates {
			hasFilterCandidateFn := acf.filterIndexerCandidate != nil
			keepCandidate := true
			if hasFilterCandidateFn {
				keepCandidate, candidate = acf.filterIndexerCandidate(candidate)
			}
			if keepCandidate {
				acceptableCandidates = append(acceptableCandidates, candidate)
			}
		}

		if len(acceptableCandidates) == 0 {
			return
		}

		eventsCallback(events.CandidatesFiltered(acf.clock.Now(), request.RetrievalID, request.Cid, acceptableCandidates))
		totalCandidates.Add(uint64(len(acceptableCandidates)))
		onCandidates(acceptableCandidates)
	}, acf.clock)

	err := candidateBuffer.BufferStream(ctx, func(ctx context.Context, onNextCandidate candidatebuffer.OnNextCandidate) error {
		if len(request.FixedPeers) > 0 {
			return sendFixedPeers(request.Cid, request.FixedPeers, onNextCandidate)
		}
		return acf.candidateFinder.FindCandidatesAsync(ctx, request.Cid, onNextCandidate)
	}, BufferWindow)

	if err != nil {
		eventsCallback(events.Failed(acf.clock.Now(), request.RetrievalID, types.RetrievalCandidate{RootCid: request.Cid}, err.Error()))
		return fmt.Errorf("could not get retrieval candidates for %s: %w", request.Cid, err)
	}

	if totalCandidates.Load() == 0 {
		eventsCallback(events.Failed(acf.clock.Now(), request.RetrievalID, types.RetrievalCandidate{RootCid: request.Cid}, ErrNoCandidates.Error()))
		return ErrNoCandidates
	}
	return nil
}

func sendFixedPeers(requestCid cid.Cid, fixedPeers []peer.AddrInfo, onNextCandidate candidatebuffer.OnNextCandidate) error {
	md := metadata.Default.New(&metadata.GraphsyncFilecoinV1{}, &metadata.Bitswap{}, &metadata.IpfsGatewayHttp{})
	for _, fixedPeer := range fixedPeers {
		onNextCandidate(types.RetrievalCandidate{
			MinerPeer: fixedPeer,
			RootCid:   requestCid,
			Metadata:  md,
		})
	}
	return nil
}
