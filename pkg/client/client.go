package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	dtchannelmonitor "github.com/filecoin-project/go-data-transfer/v2/channelmonitor"
	dtimpl "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/v2/network"
	dttransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/hannahhoward/go-pubsub/ready"

	"github.com/filecoin-project/go-address"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	retrievaltypes "github.com/filecoin-project/go-retrieval-types"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/paych"

	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	format "github.com/ipfs/go-ipld-format"

	graphsync "github.com/ipfs/go-graphsync/impl"
	gsnetwork "github.com/ipfs/go-graphsync/network"

	logging "github.com/ipfs/go-log/v2"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Logging
var log = logging.Logger("client")
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
	dataTransfer datatransfer.Manager
	host         host.Host
	payChanMgr   PayChannelManager
	ready        *ready.ReadyManager
}

type Config struct {
	ChannelMonitorConfig dtchannelmonitor.Config
	Datastore            datastore.Batching
	GraphsyncOpts        []graphsync.Option
	Host                 host.Host
	PayChannelManager    PayChannelManager
	RetrievalConfigurer  datatransfer.TransportConfigurer
}

// Creates a new RetrievalClient
func NewClient(ctx context.Context, datastore datastore.Batching, host host.Host, payChanMgr PayChannelManager, opts ...func(*Config)) (*RetrievalClient, error) {
	cfg := &Config{
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

	return NewClientWithConfig(ctx, cfg)
}

// Creates a new RetrievalClient with the given Config
func NewClientWithConfig(ctx context.Context, cfg *Config) (*RetrievalClient, error) {
	if cfg.PayChannelManager != nil {
		if err := cfg.PayChannelManager.Start(); err != nil {
			return nil, err
		}
	}

	graphSync := graphsync.New(ctx,
		gsnetwork.NewFromLibp2pHost(cfg.Host),
		cidlink.DefaultLinkSystem(),
		cfg.GraphsyncOpts...,
	).(*graphsync.GraphSync)

	dtNetwork := dtnetwork.NewFromLibp2pHost(cfg.Host)
	dtTransport := dttransport.NewTransport(cfg.Host.ID(), graphSync)

	dtRestartConfig := dtimpl.ChannelRestartConfig(cfg.ChannelMonitorConfig)

	dataTransfer, err := dtimpl.NewDataTransfer(cfg.Datastore, dtNetwork, dtTransport, dtRestartConfig)
	if err != nil {
		return nil, err
	}

	err = dataTransfer.RegisterVoucherType(retrievaltypes.DealProposalType, nil)
	if err != nil {
		return nil, err
	}

	err = dataTransfer.RegisterVoucherType(retrievaltypes.DealPaymentType, nil)
	if err != nil {
		return nil, err
	}

	if cfg.RetrievalConfigurer != nil {
		if err := dataTransfer.RegisterTransportConfigurer(retrievaltypes.DealProposalType, cfg.RetrievalConfigurer); err != nil {
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

	client := &RetrievalClient{
		dataTransfer: dataTransfer,
		host:         cfg.Host,
		payChanMgr:   cfg.PayChannelManager,
		ready:        ready,
	}

	return client, nil
}

func (rc *RetrievalClient) AwaitReady() error {
	return rc.ready.AwaitReady()
}

func (rc *RetrievalClient) RetrieveFromPeer(
	ctx context.Context,
	linkSystem ipld.LinkSystem,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievaltypes.DealProposal,
	sel ipld.Node,
	eventsCallback datatransfer.Subscriber,
	gracefulShutdownRequested <-chan struct{},
) (*types.RetrievalStats, error) {
	log.Infof("Starting retrieval with miner peer ID: %s", peerID)

	ctx, span := tracer.Start(ctx, "rcRetrieveContent")
	defer span.End()

	if _, err := selector.CompileSelector(sel); err != nil {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}

	// Stats
	startTime := time.Now()
	var timeToFirstByte time.Duration
	totalPayment := abi.NewTokenAmount(0)

	rootCid := proposal.PayloadCID

	willingToPay := rc.payChanMgr != nil // The PayChannelManager can be nil if we don't want to pay for retrievals
	paymentRequired := !proposal.PricePerByte.IsZero() || !proposal.UnsealPrice.IsZero()
	var payChanAddr address.Address
	var payChanLane uint64

	if willingToPay && paymentRequired {
		// Get the payment channel and create a lane for this retrieval
		payChanAddr, err := rc.payChanMgr.GetPayChannelWithMinFunds(ctx, minerWallet)
		if err != nil {
			return nil, fmt.Errorf("failed to get payment channel: %w", err)
		}
		payChanLane, err = rc.payChanMgr.AllocateLane(ctx, payChanAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate lane: %w", err)
		}
	}

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

	eventsCb := func(event datatransfer.Event, state datatransfer.ChannelState) {
		if eventsCallback != nil {
			defer eventsCallback(event, state)
		}
		var receivedFirstByte bool
		var eventCodeNotHandled bool

		switch event.Code {
		case datatransfer.Open:
		case datatransfer.Accept:
		case datatransfer.Restart:
		case datatransfer.DataReceived:
			receivedFirstByte = true
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
			resType, err := retrievaltypes.DealResponseFromNode(lastVoucher.Voucher)
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
			case retrievaltypes.DealStatusAccepted:
				log.Info("Deal accepted")
			// Respond with a payment voucher when funds are requested
			case retrievaltypes.DealStatusFundsNeeded, retrievaltypes.DealStatusFundsNeededLastPayment:
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

					paymentVoucher := retrievaltypes.BindnodeRegistry.TypeToNode(&retrievaltypes.DealPayment{
						ID:             proposal.ID,
						PaymentChannel: payChanAddr,
						PaymentVoucher: voucher,
					})

					if err := rc.dataTransfer.SendVoucher(ctx, state.ChannelID(), datatransfer.TypedVoucher{Type: retrievaltypes.DealPaymentType, Voucher: paymentVoucher}); err != nil {
						finish(fmt.Errorf("failed to send payment voucher: %w", err))
						return
					}

					nonce++
				} else {
					finish(fmt.Errorf("the miner requested payment even though this transaction was determined to be zero cost"))
					return
				}
			case retrievaltypes.DealStatusRejected:
				finish(fmt.Errorf("deal rejected: %s", resType.Message))
				return
			case retrievaltypes.DealStatusFundsNeededUnseal, retrievaltypes.DealStatusUnsealing:
				finish(fmt.Errorf("data is sealed"))
				return
			case retrievaltypes.DealStatusCancelled:
				finish(fmt.Errorf("deal cancelled: %s", resType.Message))
				return
			case retrievaltypes.DealStatusErrored:
				finish(fmt.Errorf("deal errored: %s", resType.Message))
				return
			case retrievaltypes.DealStatusCompleted:
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
			timeToFirstByte = time.Since(startTime)
			receivedFirstByte = true
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
			if !receivedFirstByte { // || rc.logRetrievalProgressEvents {
				log.Debugw("retrieval event", "dealID", dealID, "rootCid", rootCid, "peerID", peerID, "name", name, "code", code, "message", msg, "blocksIndex", blocksIndex, "totalReceived", totalReceived)
			}
		}
	}

	// Submit the retrieval deal proposal to the miner
	proposalVoucher := retrievaltypes.BindnodeRegistry.TypeToNode(proposal)
	chanid, err := rc.dataTransfer.OpenPullDataChannel(
		ctx,
		peerID,
		datatransfer.TypedVoucher{Type: retrievaltypes.DealProposalType, Voucher: proposalVoucher},
		proposal.PayloadCID,
		sel,
		datatransfer.WithSubscriber(eventsCb),
		datatransfer.WithTransportOptions(dttransport.UseStore(linkSystem)),
	)
	if err != nil {
		// We could fail before a successful proposal
		return nil, fmt.Errorf("%w: %s", retriever.ErrDealProposalFailed, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
		defer cancel()
		rc.dataTransfer.CloseDataTransferChannel(ctx, chanid)
	}()

	// Wait for the retrieval to finish before exiting the function
awaitfinished:
	for {
		select {
		case err := <-dtRes:
			if err != nil {
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
	if _, err := linkSystem.StorageReadOpener(ipld.LinkContext{}, cidlink.Link{Cid: rootCid}); err != nil {
		if !errors.Is(err, format.ErrNotFound{}) { // may or may not get a go-ipld-format/ErrNotFound for not-found
			log.Errorf("could not query block store: %w", err)
		}
		return nil, errors.New("data transfer failed: unconfirmed block transfer")
	}

	// Compile the retrieval stats

	state, err := rc.dataTransfer.ChannelState(ctx, chanid)
	if err != nil {
		return nil, fmt.Errorf("could not get channel state: %w", err)
	}

	duration := time.Since(startTime)
	speed := uint64(float64(state.Received()) / duration.Seconds())

	return &types.RetrievalStats{
		RootCid:           rootCid,
		StorageProviderId: state.OtherPeer(),
		Size:              state.Received(),
		Blocks:            uint64(state.ReceivedCidsTotal()),
		Duration:          duration,
		AverageSpeed:      speed,
		TotalPayment:      totalPayment,
		NumPayments:       int(nonce),
		AskPrice:          proposal.PricePerByte,
		TimeToFirstByte:   timeToFirstByte,
	}, nil
}

func (rc *RetrievalClient) RetrievalQueryToPeer(
	ctx context.Context,
	peerAddr peer.AddrInfo,
	payloadCid cid.Cid,
	onConnected func(),
) (*retrievaltypes.QueryResponse, error) {

	ctx, span := tracer.Start(ctx, "retrievalQueryPeer", trace.WithAttributes(
		attribute.Stringer("peerID", peerAddr.ID),
	))
	defer span.End()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := rc.host.Connect(ctx, peerAddr); err != nil {
		return nil, err
	}

	stream, err := rc.host.NewStream(ctx, peerAddr.ID, RetrievalQueryProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to storage provider: %w", err)
	}

	rc.host.ConnManager().Protect(stream.Conn().RemotePeer(), "RetrievalQueryToPeer")
	defer func() {
		rc.host.ConnManager().Unprotect(stream.Conn().RemotePeer(), "RetrievalQueryToPeer")
		stream.Close()
	}()

	if onConnected != nil {
		onConnected()
	}

	if dline, ok := ctx.Deadline(); ok {
		stream.SetDeadline(dline)
		defer stream.SetDeadline(time.Time{})
	}

	// send query and read response in a goroutine so we can gracefully handle
	// context cancellation
	qrCh := make(chan *retrievaltypes.QueryResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		// send query
		if err := types.QueryToWriter(&retrievaltypes.Query{PayloadCID: payloadCid}, stream); err != nil {
			select {
			case errCh <- fmt.Errorf("failed to send query: %w", err):
			case <-ctx.Done():
			}
			return
		}
		// read query response
		resp, err := types.QueryResponseFromReader(stream)
		if err != nil {
			select {
			case errCh <- fmt.Errorf("failed to read query response: %w", err):
			case <-ctx.Done():
			}
			return
		}
		select {
		case qrCh <- resp:
		case <-ctx.Done():
		}
	}()

	select {
	case resp := <-qrCh:
		return resp, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
