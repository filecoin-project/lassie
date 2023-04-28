package retriever

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	retrievaltypes "github.com/filecoin-project/go-retrieval-types"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lassie/pkg/events"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"go.uber.org/multierr"
)

type GraphsyncClient interface {
	Connect(ctx context.Context, peerAddr peer.AddrInfo) error
	RetrieveFromPeer(
		ctx context.Context,
		linkSystem ipld.LinkSystem,
		peerID peer.ID,
		proposal *retrievaltypes.DealProposal,
		selector ipld.Node,
		maxLinks uint64,
		eventsCallback datatransfer.Subscriber,
		gracefulShutdownRequested <-chan struct{},
	) (*types.RetrievalStats, error)
}

var _ TransportProtocol = &ProtocolGraphsync{}

type ProtocolGraphsync struct {
	Client GraphsyncClient
}

// NewGraphsyncRetriever makes a new CandidateRetriever for Graphsync retrievals
// (transport-graphsync-filecoinv1).
func NewGraphsyncRetriever(getStorageProviderTimeout GetStorageProviderTimeout, client GraphsyncClient) types.CandidateRetriever {
	return &parallelPeerRetriever{
		Protocol: &ProtocolGraphsync{
			Client: client,
		},
		GetStorageProviderTimeout: getStorageProviderTimeout,
		Clock:                     clock.New(),
		QueueInitialPause:         2 * time.Millisecond,
	}
}

func (pg ProtocolGraphsync) Code() multicodec.Code {
	return multicodec.TransportGraphsyncFilecoinv1
}

func (pg ProtocolGraphsync) GetMergedMetadata(cid cid.Cid, currentMetadata, newMetadata metadata.Protocol) metadata.Protocol {
	gsNewMetadata, ok := newMetadata.(*metadata.GraphsyncFilecoinV1)
	if !ok {
		gsNewMetadata = &metadata.GraphsyncFilecoinV1{PieceCID: cid}
	}
	if currentMetadata != nil { // seen this candidate before
		if !ok {
			return currentMetadata
		}
		gsCurrentMetadata := currentMetadata.(*metadata.GraphsyncFilecoinV1)
		if !graphsyncMetadataCompare(gsNewMetadata, gsCurrentMetadata, false) {
			return currentMetadata // old one is better
		}
	}
	return gsNewMetadata
}

// graphsyncMetadataCompare compares two metadata.GraphsyncFilecoinV1s and
// returns true if the first is preferable to the second.
func graphsyncMetadataCompare(a, b *metadata.GraphsyncFilecoinV1, defaultValue bool) bool {
	// prioritize verified deals over not verified deals
	if a.VerifiedDeal != b.VerifiedDeal {
		return a.VerifiedDeal
	}

	// prioritize fast retrievel over not fast retrieval
	if a.FastRetrieval != b.FastRetrieval {
		return a.FastRetrieval
	}

	return defaultValue
}

func (pg ProtocolGraphsync) CompareCandidates(a, b connectCandidate, mda, mdb metadata.Protocol) bool {
	gsmda := mda.(*metadata.GraphsyncFilecoinV1)
	gsmdb := mdb.(*metadata.GraphsyncFilecoinV1)
	return graphsyncMetadataCompare(gsmda, gsmdb, a.Duration < b.Duration)
}

func (pg *ProtocolGraphsync) Connect(ctx context.Context, retrieval *retrieval, candidate types.RetrievalCandidate) error {
	return pg.Client.Connect(ctx, candidate.MinerPeer)
}

func (pg *ProtocolGraphsync) Retrieve(
	ctx context.Context,
	retrieval *retrieval,
	session *retrievalSession,
	phaseStartTime time.Time,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
) (*types.RetrievalStats, error) {

	eventsCallback := makeEventsCallback(
		session,
		retrieval.parallelPeerRetriever.Clock,
		retrieval.request.RetrievalID,
		phaseStartTime,
		candidate,
	)
	return pg.retrievalPhase(
		ctx,
		retrieval,
		timeout,
		candidate,
		eventsCallback,
	)
}

func makeEventsCallback(
	session *retrievalSession,
	clock clock.Clock,
	retrievalId types.RetrievalID,
	phaseStartTime time.Time,
	candidate types.RetrievalCandidate) datatransfer.Subscriber {

	var receivedFirstByte bool
	return func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch event.Code {
		case datatransfer.Open:
			session.sendEvent(events.Proposed(clock.Now(), retrievalId, phaseStartTime, candidate))
		case datatransfer.NewVoucherResult:
			lastVoucher := channelState.LastVoucherResult()
			resType, err := retrievaltypes.DealResponseFromNode(lastVoucher.Voucher)
			if err != nil {
				return
			}
			if resType.Status == retrievaltypes.DealStatusAccepted {
				session.sendEvent(events.Accepted(clock.Now(), retrievalId, phaseStartTime, candidate))
			}
		case datatransfer.DataReceivedProgress:
			if !receivedFirstByte {
				receivedFirstByte = true
				session.sendEvent(events.FirstByte(clock.Now(), retrievalId, phaseStartTime, candidate))
			}
		}
	}
}

func (pg *ProtocolGraphsync) retrievalPhase(
	ctx context.Context,
	retrieval *retrieval,
	timeout time.Duration,
	candidate types.RetrievalCandidate,
	eventsCallback datatransfer.Subscriber,
) (*types.RetrievalStats, error) {

	ss := "*"
	selector := retrieval.request.GetSelector()
	if !ipld.DeepEqual(selector, selectorparse.CommonSelector_ExploreAllRecursively) {
		byts, err := ipld.Encode(selector, dagjson.Encode)
		if err != nil {
			return nil, err
		}
		ss = string(byts)
	}

	log.Infof(
		"Attempting retrieval from SP %s for %s (with selector: [%s])",
		candidate.MinerPeer.ID,
		candidate.RootCid,
		ss,
	)

	params, err := retrievaltypes.NewParamsV1(big.Zero(), 0, 0, selector, nil, big.Zero())
	if err != nil {
		return nil, multierr.Append(multierr.Append(ErrRetrievalFailed, ErrProposalCreationFailed), err)
	}
	proposal := &retrievaltypes.DealProposal{
		PayloadCID: candidate.RootCid,
		ID:         retrievaltypes.DealID(dealIdGen.Next()),
		Params:     params,
	}

	retrieveCtx, retrieveCancel := context.WithCancel(ctx)
	defer retrieveCancel()

	var lastBytesReceived uint64
	var doneLk sync.Mutex
	var done, timedOut bool
	var lastBytesReceivedTimer, gracefulShutdownTimer *clock.Timer

	gracefulShutdownChan := make(chan struct{})

	// Start the timeout tracker only if retrieval timeout isn't 0
	if timeout != 0 {
		lastBytesReceivedTimer = retrieval.parallelPeerRetriever.Clock.AfterFunc(timeout, func() {
			doneLk.Lock()
			done = true
			timedOut = true
			doneLk.Unlock()

			gracefulShutdownChan <- struct{}{}
			gracefulShutdownTimer = retrieval.parallelPeerRetriever.Clock.AfterFunc(1*time.Minute, retrieveCancel)
		})
	}

	eventsSubscriber := func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.DataReceivedProgress {
			if lastBytesReceivedTimer != nil {
				doneLk.Lock()
				if !done {
					if lastBytesReceived != channelState.Received() {
						lastBytesReceivedTimer.Reset(timeout)
						lastBytesReceived = channelState.Received()
					}
				}
				doneLk.Unlock()
			}
		}
		eventsCallback(event, channelState)
	}

	stats, err := pg.Client.RetrieveFromPeer(
		retrieveCtx,
		retrieval.request.LinkSystem,
		candidate.MinerPeer.ID,
		proposal,
		selector,
		uint64(retrieval.request.MaxBlocks),
		eventsSubscriber,
		gracefulShutdownChan,
	)

	if timedOut {
		return nil, multierr.Append(ErrRetrievalFailed,
			fmt.Errorf(
				"%w after %s",
				ErrRetrievalTimedOut,
				timeout,
			),
		)
	}

	if lastBytesReceivedTimer != nil {
		lastBytesReceivedTimer.Stop()
	}
	if gracefulShutdownTimer != nil {
		gracefulShutdownTimer.Stop()
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRetrievalFailed, err)
	}
	return stats, nil
}
