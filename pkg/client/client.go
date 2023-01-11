package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtchannelmonitor "github.com/filecoin-project/go-data-transfer/v2/channelmonitor"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/v2/network"
	dttransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/hannahhoward/go-pubsub/ready"

	"github.com/filecoin-project/go-address"

	cborutil "github.com/filecoin-project/go-cbor-util"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/requestvalidation"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/paych"

	"github.com/filecoin-project/lassie/pkg/eventpublisher"
	"github.com/filecoin-project/lassie/pkg/retriever"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"

	blockstore "github.com/ipfs/go-ipfs-blockstore"

	graphsync "github.com/ipfs/go-graphsync/impl"
	gsnetwork "github.com/ipfs/go-graphsync/network"
	gsutil "github.com/ipfs/go-graphsync/storeutil"

	logging "github.com/ipfs/go-log/v2"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Logging
var log = logging.Logger("client")
var retrievalLog = logging.Logger("lassie-retrieval")
var tracer trace.Tracer = otel.Tracer("lassie")

const RetrievalQueryProtocol = "/fil/retrieval/qry/1.0.0"

// ChannelMonitorConfig defaults
const acceptTimeout = 24 * time.Hour
const completeTimeout = 40 * time.Minute
const maxConsecutiveRestarts = 15
const restartBackoff = 20 * time.Second
const restartDebounce = 10 * time.Second

// GraphSyncOpt defaults
const maxInProgressIncomingRequests = 200
const maxInProgressIncomingRequestsPerPeer = 20
const maxInProgressOutgoingRequests = 200
const maxMemoryPerPeerResponder = 32 << 20
const maxMemoryResponder = 8 << 30
const maxTraversalLinks = 32 * (1 << 20)
const messageSendRetries = 2
const sendMessageTimeout = 2 * time.Minute

type RetrievalClient struct {
	blockstore              blockstore.Blockstore
	dataTransfer            datatransfer.Manager
	host                    host.Host
	payChanMgr              PayChannelManager
	ready                   *ready.ReadyManager
	retrievalEventPublisher *eventpublisher.RetrievalEventPublisher
}

type Config struct {
	Blockstore           blockstore.Blockstore
	ChannelMonitorConfig dtchannelmonitor.Config
	Datastore            datastore.Batching
	GraphsyncOpts        []graphsync.Option
	Host                 host.Host
	PayChannelManager    PayChannelManager
	RetrievalConfigurer  datatransfer.TransportConfigurer
}

// Creates a new RetrievalClient
func NewClient(blockstore blockstore.Blockstore, datastore datastore.Batching, host host.Host, payChanMgr PayChannelManager, opts ...func(*Config)) (*RetrievalClient, error) {
	cfg := &Config{
		Blockstore: blockstore,
		ChannelMonitorConfig: dtchannelmonitor.Config{
			AcceptTimeout:          acceptTimeout,
			RestartDebounce:        restartDebounce,
			RestartBackoff:         restartBackoff,
			MaxConsecutiveRestarts: maxConsecutiveRestarts,
			CompleteTimeout:        completeTimeout,
			// RestartAckTimeout:      30 * time.Second,

			// Called when a restart completes successfully
			//OnRestartComplete func(id datatransfer.ChannelID)
		},
		Datastore: datastore,
		GraphsyncOpts: []graphsync.Option{
			graphsync.MaxInProgressIncomingRequests(maxInProgressIncomingRequests),
			graphsync.MaxInProgressOutgoingRequests(maxInProgressOutgoingRequests),
			graphsync.MaxMemoryResponder(maxMemoryResponder),
			graphsync.MaxMemoryPerPeerResponder(maxMemoryPerPeerResponder),
			graphsync.MaxInProgressIncomingRequestsPerPeer(maxInProgressIncomingRequestsPerPeer),
			graphsync.MessageSendRetries(messageSendRetries),
			graphsync.SendMessageTimeout(sendMessageTimeout),
			graphsync.MaxLinksPerIncomingRequests(maxTraversalLinks),
			graphsync.MaxLinksPerOutgoingRequests(maxTraversalLinks),
		},
		Host:              host,
		PayChannelManager: payChanMgr,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return NewClientWithConfig(cfg)
}

// Creates a new RetrievalClient with the given Config
func NewClientWithConfig(cfg *Config) (*RetrievalClient, error) {
	ctx := context.Background()

	if cfg.PayChannelManager != nil {
		if err := cfg.PayChannelManager.Start(); err != nil {
			return nil, err
		}
	}

	graphSync := graphsync.New(context.Background(),
		gsnetwork.NewFromLibp2pHost(cfg.Host),
		gsutil.LinkSystemForBlockstore(cfg.Blockstore),
		cfg.GraphsyncOpts...,
	).(*graphsync.GraphSync)

	dtNetwork := dtnetwork.NewFromLibp2pHost(cfg.Host)
	dtTransport := dttransport.NewTransport(cfg.Host.ID(), graphSync)

	dtRestartConfig := dtimpl.ChannelRestartConfig(cfg.ChannelMonitorConfig)

	dataTransfer, err := dtimpl.NewDataTransfer(cfg.Datastore, dtNetwork, dtTransport, dtRestartConfig)
	if err != nil {
		return nil, err
	}

	err = dataTransfer.RegisterVoucherType(requestvalidation.StorageDataTransferVoucherType, nil)
	if err != nil {
		return nil, err
	}

	err = dataTransfer.RegisterVoucherType(retrievalmarket.DealProposalType, nil)
	if err != nil {
		return nil, err
	}

	err = dataTransfer.RegisterVoucherType(retrievalmarket.DealPaymentType, nil)
	if err != nil {
		return nil, err
	}

	if cfg.RetrievalConfigurer != nil {
		if err := dataTransfer.RegisterTransportConfigurer(retrievalmarket.DealProposalType, cfg.RetrievalConfigurer); err != nil {
			return nil, err
		}
	}

	ready := ready.NewReadyManager()
	dataTransfer.OnReady(func(err error) {
		ready.FireReady(err)
	})

	if err := dataTransfer.Start(ctx); err != nil {
		return nil, err
	}

	retrievalEventPublisher := eventpublisher.NewEventPublisher(ctx)

	client := &RetrievalClient{
		blockstore:              cfg.Blockstore,
		dataTransfer:            dataTransfer,
		host:                    cfg.Host,
		payChanMgr:              cfg.PayChannelManager,
		retrievalEventPublisher: retrievalEventPublisher,
		ready:                   ready,
	}

	retrievalEventPublisher.Subscribe(client)

	return client, nil
}

func (rc *RetrievalClient) AwaitReady() error {
	return rc.ready.AwaitReady()
}

func (rc *RetrievalClient) RetrieveContentFromPeerAsync(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
) (result <-chan retriever.RetrievalResult, onProgress <-chan uint64, gracefulShutdown func()) {
	gracefulShutdownChan := make(chan struct{}, 1)
	resultChan := make(chan retriever.RetrievalResult, 1)
	progressChan := make(chan uint64)
	internalCtx, internalCancel := context.WithCancel(ctx)
	go func() {
		defer internalCancel()
		result, err := rc.retrieveContentFromPeerWithProgressCallback(internalCtx, peerID, minerWallet, proposal, func(bytes uint64) {
			select {
			case <-internalCtx.Done():
			case progressChan <- bytes:
			}
		}, gracefulShutdownChan)
		resultChan <- retriever.RetrievalResult{result, err}
	}()
	return resultChan, progressChan, func() {
		gracefulShutdownChan <- struct{}{}
	}
}

func (rc *RetrievalClient) RetrieveContentFromPeerWithProgressCallback(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
	progressCallback func(bytesReceived uint64),
) (*retriever.RetrievalStats, error) {
	return rc.retrieveContentFromPeerWithProgressCallback(ctx, peerID, minerWallet, proposal, progressCallback, nil)
}

func (rc *RetrievalClient) retrieveContentFromPeerWithProgressCallback(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
	progressCallback func(bytesReceived uint64),
	gracefulShutdownRequested <-chan struct{},
) (*retriever.RetrievalStats, error) {
	if progressCallback == nil {
		progressCallback = func(bytesReceived uint64) {}
	}

	log.Infof("Starting retrieval with miner peer ID: %s", peerID)

	ctx, span := tracer.Start(ctx, "rcRetrieveContent")
	defer span.End()

	// Stats
	startTime := time.Now()
	totalPayment := abi.NewTokenAmount(0)

	rootCid := proposal.PayloadCID
	var chanid datatransfer.ChannelID
	var chanidLk sync.Mutex

	willingToPay := rc.payChanMgr != nil // The PayChannelManager can be nil if we don't want to pay for retrievals
	paymentRequired := !proposal.PricePerByte.IsZero() || !proposal.UnsealPrice.IsZero()
	var payChanAddr address.Address
	var payChanLane uint64

	if willingToPay && paymentRequired {
		// Get the payment channel and create a lane for this retrieval
		payChanAddr, err := rc.payChanMgr.GetPayChannelWithMinFunds(ctx, minerWallet)
		if err != nil {
			rc.retrievalEventPublisher.Publish(
				eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef,
					fmt.Sprintf("failed to get payment channel: %s", err.Error())))
			return nil, fmt.Errorf("failed to get payment channel: %w", err)
		}
		payChanLane, err = rc.payChanMgr.AllocateLane(ctx, payChanAddr)
		if err != nil {
			rc.retrievalEventPublisher.Publish(
				eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef,
					fmt.Sprintf("failed to allocate lane: %s", err.Error())))
			return nil, fmt.Errorf("failed to allocate lane: %w", err)
		}
	}

	// Set up incoming events handler

	// The next nonce (incrementing unique ID starting from 0) for the next voucher
	var nonce uint64 = 0

	// dtRes receives either an error (failure) or nil (success) which is waited
	// on and handled below before exiting the function
	dtRes := make(chan error, 1)

	finish := func(err error) {
		select {
		case dtRes <- err:
		default:
		}
	}

	dealID := proposal.ID
	allBytesReceived := false
	dealComplete := false
	receivedFirstByte := false

	unsubscribe := rc.dataTransfer.SubscribeToEvents(func(event datatransfer.Event, state datatransfer.ChannelState) {
		// Copy chanid so it can be used later in the callback
		chanidLk.Lock()
		chanidCopy := chanid
		chanidLk.Unlock()

		// Skip all events that aren't related to this channel
		if state.ChannelID() != chanidCopy {
			return
		}

		silenceEventCode := false
		eventCodeNotHandled := false

		switch event.Code {
		case datatransfer.Open:
		case datatransfer.Accept:
		case datatransfer.Restart:
		case datatransfer.DataReceived:
			silenceEventCode = true
		case datatransfer.DataSent:
		case datatransfer.Cancel:
		case datatransfer.Error:
			finish(fmt.Errorf("datatransfer error: %s", event.Message))
			return
		case datatransfer.CleanupComplete:
			finish(nil)
			return
		case datatransfer.NewVoucher:
		case datatransfer.NewVoucherResult:
			lastVoucher := state.LastVoucherResult()
			resType, err := retrievalmarket.DealResponseFromNode(lastVoucher.Voucher)
			if err != nil {
				log.Errorf("unexpected voucher result received: %s", err.Error())
				return
			}
			if len(resType.Message) != 0 {
				log.Debugf("Received deal response voucher result %s (%v): %s\n\t%+v", resType.Status, resType.Status, resType.Message, resType)
			} else {
				log.Debugf("Received deal response voucher result %s (%v)\n\t%+v", resType.Status, resType.Status, resType)
			}

			switch resType.Status {
			case retrievalmarket.DealStatusAccepted:
				log.Info("Deal accepted")

				// publish deal accepted event
				rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventAccepted(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef))

			// Respond with a payment voucher when funds are requested
			case retrievalmarket.DealStatusFundsNeeded, retrievalmarket.DealStatusFundsNeededLastPayment:
				if willingToPay && paymentRequired {
					log.Infof("Sending payment voucher (nonce: %v, amount: %v)", nonce, resType.PaymentOwed)

					totalPayment = big.Add(totalPayment, resType.PaymentOwed)

					voucher, shortfall, err := rc.payChanMgr.CreateVoucher(ctx, payChanAddr, paych.SignedVoucher{
						ChannelAddr: payChanAddr,
						Lane:        payChanLane,
						Nonce:       nonce,
						Amount:      totalPayment,
					})
					if err != nil {
						finish(err)
						return
					}

					if big.Cmp(shortfall, big.NewInt(0)) > 0 {
						finish(fmt.Errorf("not enough funds remaining in payment channel (shortfall = %s)", shortfall))
						return
					}

					paymentVoucher := retrievalmarket.BindnodeRegistry.TypeToNode(&retrievalmarket.DealPayment{
						ID:             proposal.ID,
						PaymentChannel: payChanAddr,
						PaymentVoucher: voucher,
					})

					if err := rc.dataTransfer.SendVoucher(ctx, chanidCopy, datatransfer.TypedVoucher{Type: retrievalmarket.DealPaymentType, Voucher: paymentVoucher}); err != nil {
						finish(fmt.Errorf("failed to send payment voucher: %w", err))
						return
					}

					nonce++
				} else {
					finish(fmt.Errorf("the miner requested payment even though this transaction was determined to be zero cost"))
					return
				}
			case retrievalmarket.DealStatusRejected:
				finish(fmt.Errorf("deal rejected: %s", resType.Message))
				return
			case retrievalmarket.DealStatusFundsNeededUnseal, retrievalmarket.DealStatusUnsealing:
				finish(fmt.Errorf("data is sealed"))
				return
			case retrievalmarket.DealStatusCancelled:
				finish(fmt.Errorf("deal cancelled: %s", resType.Message))
				return
			case retrievalmarket.DealStatusErrored:
				finish(fmt.Errorf("deal errored: %s", resType.Message))
				return
			case retrievalmarket.DealStatusCompleted:
				if allBytesReceived {
					finish(nil)
					return
				}
				dealComplete = true
			}
		case datatransfer.PauseInitiator:
		case datatransfer.ResumeInitiator:
		case datatransfer.PauseResponder:
		case datatransfer.ResumeResponder:
		case datatransfer.FinishTransfer:
			if dealComplete {
				finish(nil)
				return
			}
			allBytesReceived = true
		case datatransfer.ResponderCompletes:
		case datatransfer.ResponderBeginsFinalization:
		case datatransfer.BeginFinalizing:
		case datatransfer.Disconnected:
		case datatransfer.Complete:
		case datatransfer.CompleteCleanupOnRestart:
		case datatransfer.DataQueued:
		case datatransfer.DataQueuedProgress:
		case datatransfer.DataSentProgress:
		case datatransfer.DataReceivedProgress:
			// First byte has been received

			// publish first byte event
			if !receivedFirstByte {
				receivedFirstByte = true
				rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventFirstByte(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef))
			}

			progressCallback(state.Received())
			silenceEventCode = true
		case datatransfer.RequestTimedOut:
		case datatransfer.SendDataError:
		case datatransfer.ReceiveDataError:
		case datatransfer.TransferRequestQueued:
		case datatransfer.RequestCancelled:
		case datatransfer.Opened:
		default:
			eventCodeNotHandled = true
		}

		name := datatransfer.Events[event.Code]
		code := event.Code
		msg := event.Message
		blocksIndex := state.ReceivedCidsTotal()
		totalReceived := state.Received()
		if eventCodeNotHandled {
			log.Warnw("unhandled retrieval event", "dealID", dealID, "rootCid", rootCid, "peerID", peerID, "name", name, "code", code, "message", msg, "blocksIndex", blocksIndex, "totalReceived", totalReceived)
		} else {
			if !silenceEventCode { // || rc.logRetrievalProgressEvents {
				log.Debugw("retrieval event", "dealID", dealID, "rootCid", rootCid, "peerID", peerID, "name", name, "code", code, "message", msg, "blocksIndex", blocksIndex, "totalReceived", totalReceived)
			}
		}
	})
	defer unsubscribe()

	// Submit the retrieval deal proposal to the miner
	proposalVoucher := retrievalmarket.BindnodeRegistry.TypeToNode(proposal)
	newchid, err := rc.dataTransfer.OpenPullDataChannel(ctx, peerID, datatransfer.TypedVoucher{Type: retrievalmarket.DealProposalType, Voucher: proposalVoucher}, proposal.PayloadCID, shared.AllSelector())
	if err != nil {
		// We could fail before a successful proposal
		// publish event failure
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef,
				fmt.Sprintf("deal proposal failed: %s", err.Error())))
		return nil, err
	}

	// Deal has been proposed
	// publish deal proposed event
	rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventProposed(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef))

	chanidLk.Lock()
	chanid = newchid
	chanidLk.Unlock()

	defer rc.dataTransfer.CloseDataTransferChannel(ctx, chanid)

	// Wait for the retrieval to finish before exiting the function
awaitfinished:
	for {
		select {
		case err := <-dtRes:
			if err != nil {
				// If there is an error, publish a retrieval event failure
				rc.retrievalEventPublisher.Publish(
					eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef,
						fmt.Sprintf("data transfer failed: %s", err.Error())))
				return nil, fmt.Errorf("data transfer failed: %w", err)
			}

			log.Debugf("data transfer for retrieval complete")
			break awaitfinished
		case <-gracefulShutdownRequested:
			go func() {
				rc.dataTransfer.CloseDataTransferChannel(ctx, chanid)
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Confirm that we actually ended up with the root block we wanted, failure
	// here indicates a data transfer error that was not properly reported
	if has, err := rc.blockstore.Has(ctx, rootCid); err != nil {
		err = fmt.Errorf("could not get query blockstore: %w", err)
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef, err.Error()))
		return nil, err
	} else if !has {
		msg := "data transfer failed: unconfirmed block transfer"
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef, msg))
		return nil, errors.New(msg)
	}

	// Compile the retrieval stats

	state, err := rc.dataTransfer.ChannelState(ctx, chanid)
	if err != nil {
		err = fmt.Errorf("could not get channel state: %w", err)
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef, err.Error()))
		return nil, err
	}

	duration := time.Since(startTime)
	speed := uint64(float64(state.Received()) / duration.Seconds())

	// Otherwise publish a retrieval event success
	rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventSuccess(eventpublisher.RetrievalPhase, rootCid, peerID, address.Undef, state.Received(), state.ReceivedCidsTotal(), duration, totalPayment))

	return &retriever.RetrievalStats{
		StorageProviderId: state.OtherPeer(),
		Size:              state.Received(),
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      totalPayment,
		NumPayments:       int(nonce),
		AskPrice:          proposal.PricePerByte,
	}, nil
}

func (rc *RetrievalClient) RetrievalQueryToPeer(ctx context.Context, peerAddr peer.AddrInfo, payloadCid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	ctx, span := tracer.Start(ctx, "retrievalQueryPeer", trace.WithAttributes(
		attribute.Stringer("peerID", peerAddr.ID),
	))
	defer span.End()

	err := rc.host.Connect(ctx, peerAddr)
	if err != nil {
		// publish fail event, log the err
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.QueryPhase, payloadCid, peerAddr.ID, address.Undef,
				fmt.Sprintf("failed connecting to miner: %s", err.Error())))
		return nil, err
	}

	streamToPeer, err := rc.host.NewStream(ctx, peerAddr.ID, RetrievalQueryProtocol)
	if err != nil {
		// publish fail event, log the err
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.QueryPhase, payloadCid, peerAddr.ID, address.Undef,
				fmt.Sprintf("failed connecting to miner: %s", err.Error())))
		return nil, err
	}

	rc.host.ConnManager().Protect(streamToPeer.Conn().RemotePeer(), "RetrievalQueryToPeer")
	defer func() {
		rc.host.ConnManager().Unprotect(streamToPeer.Conn().RemotePeer(), "RetrievalQueryToPeer")
		streamToPeer.Close()
	}()

	// We have connected
	// publish connected event
	rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventConnect(eventpublisher.QueryPhase, payloadCid, peerAddr.ID, address.Address{}))

	request := &retrievalmarket.Query{
		PayloadCID: payloadCid,
	}

	var resp retrievalmarket.QueryResponse
	if err := doRpc(ctx, streamToPeer, request, &resp); err != nil {
		// publish failure event
		rc.retrievalEventPublisher.Publish(
			eventpublisher.NewRetrievalEventFailure(eventpublisher.QueryPhase, payloadCid, peerAddr.ID, address.Undef,
				fmt.Sprintf("failed retrieval query ask: %s", err.Error())))
		return nil, fmt.Errorf("failed retrieval query rpc: %w", err)
	}

	// publish query ask event
	rc.retrievalEventPublisher.Publish(eventpublisher.NewRetrievalEventQueryAsk(eventpublisher.QueryPhase, payloadCid, peerAddr.ID, address.Undef, resp))

	return &resp, nil
}

func (rc *RetrievalClient) SubscribeToRetrievalEvents(subscriber eventpublisher.RetrievalSubscriber) {
	rc.retrievalEventPublisher.Subscribe(subscriber)
}

// Implement RetrievalSubscriber
func (rc *RetrievalClient) OnRetrievalEvent(event eventpublisher.RetrievalEvent) {
	kv := make([]interface{}, 0)
	logadd := func(kva ...interface{}) {
		if len(kva)%2 != 0 {
			panic("bad number of key/value arguments")
		}
		for i := 0; i < len(kva); i += 2 {
			key, ok := kva[i].(string)
			if !ok {
				panic("expected string key")
			}
			kv = append(kv, key, kva[i+1])
		}
	}
	logadd("code", event.Code(),
		"phase", event.Phase(),
		"payloadCid", event.PayloadCid(),
		"storageProviderId", event.StorageProviderId(),
		"storageProviderAddr", event.StorageProviderAddr())
	switch tevent := event.(type) {
	case eventpublisher.RetrievalEventQueryAsk:
		logadd("queryResponse:Status", tevent.QueryResponse().Status,
			"queryResponse:PieceCIDFound", tevent.QueryResponse().PieceCIDFound,
			"queryResponse:Size", tevent.QueryResponse().Size,
			"queryResponse:PaymentAddress", tevent.QueryResponse().PaymentAddress,
			"queryResponse:MinPricePerByte", tevent.QueryResponse().MinPricePerByte,
			"queryResponse:MaxPaymentInterval", tevent.QueryResponse().MaxPaymentInterval,
			"queryResponse:MaxPaymentIntervalIncrease", tevent.QueryResponse().MaxPaymentIntervalIncrease,
			"queryResponse:Message", tevent.QueryResponse().Message,
			"queryResponse:UnsealPrice", tevent.QueryResponse().UnsealPrice)
	case eventpublisher.RetrievalEventFailure:
		logadd("errorMessage", tevent.ErrorMessage())
	case eventpublisher.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	retrievalLog.Debugw("retrieval-event", kv...)
}

func doRpc(ctx context.Context, stream network.Stream, req interface{}, resp interface{}) error {
	dline, ok := ctx.Deadline()
	if ok {
		stream.SetDeadline(dline)
		defer stream.SetDeadline(time.Time{})
	}

	if err := cborutil.WriteCborRPC(stream, req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if err := cborutil.ReadCborRPC(stream, resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}
