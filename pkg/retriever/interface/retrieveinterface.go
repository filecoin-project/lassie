package retrieveinterface

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var (
	ErrNoCandidates                = errors.New("no candidates")
	ErrUnexpectedRetrieval         = errors.New("unexpected active retrieval")
	ErrHitRetrievalLimit           = errors.New("hit retrieval limit")
	ErrProposalCreationFailed      = errors.New("proposal creation failed")
	ErrRetrievalRegistrationFailed = errors.New("retrieval registration failed")
	ErrRetrievalFailed             = errors.New("retrieval failed")
	ErrAllRetrievalsFailed         = errors.New("all retrievals failed")
	ErrQueryFailed                 = errors.New("query failed")
	ErrAllQueriesFailed            = errors.New("all queries failed")
	ErrRetrievalTimedOut           = errors.New("retrieval timed out")
)

type ErrRetrievalAlreadyRunning struct {
	C     cid.Cid
	Extra string
}

func (e ErrRetrievalAlreadyRunning) Error() string {
	return fmt.Sprintf("retrieval already running for CID: %s (%s)", e.C, e.Extra)
}

type RetrievalCandidate struct {
	SourcePeer       peer.AddrInfo
	RootCid          cid.Cid
	Protocol         multicodec.Code
	ProtocolMetadata interface{}
}

type CounterCallback func(int) error
type CandidateCallback func(RetrievalCandidate) error
type CandidateErrorCallback func(RetrievalCandidate, error)

type GetStorageProviderTimeout func(peer peer.ID) time.Duration
type IsAcceptableStorageProvider func(peer peer.ID) bool
type IsAcceptableQueryResponse func(*retrievalmarket.QueryResponse) bool

type Instrumentation interface {
	// OnRetrievalCandidatesFound is called once after querying the indexer
	OnRetrievalCandidatesFound(foundCount int) error
	// OnRetrievalCandidatesFiltered is called once after filtering is applied to indexer candidates
	OnRetrievalCandidatesFiltered(filteredCount int) error
	// OnErrorQueryingRetrievalCandidate may be called up to once per retrieval candidate
	OnErrorQueryingRetrievalCandidate(candidate RetrievalCandidate, err error)
	// OnErrorRetrievingFromCandidate may be called up to once per retrieval candidate
	OnErrorRetrievingFromCandidate(candidate RetrievalCandidate, err error)
	// OnRetrievalQueryForCandidate may be called up to once per retrieval candidate
	OnRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	// OnFilteredRetrievalQueryForCandidate may be called up to once per retrieval candidate
	OnFilteredRetrievalQueryForCandidate(candidate RetrievalCandidate, queryResponse *retrievalmarket.QueryResponse)
	// OnRetrievingFromCandidate may be called up to once per retrieval candidate
	OnRetrievingFromCandidate(candidate RetrievalCandidate)
}

type RetrievalConfig struct {
	Instrumentation             Instrumentation
	GetStorageProviderTimeout   GetStorageProviderTimeout
	IsAcceptableStorageProvider IsAcceptableStorageProvider
	IsAcceptableQueryResponse   IsAcceptableQueryResponse

	WaitGroup sync.WaitGroup // only used internally for testing cleanup
}

// wait is used internally for testing that we do proper goroutine cleanup
func (cfg *RetrievalConfig) Wait() {
	cfg.WaitGroup.Wait()
}

type ProviderConfig struct {
	RetrievalTimeout        time.Duration
	MaxConcurrentRetrievals uint
}

// All config values should be safe to leave uninitialized
type RetrieverConfig struct {
	ProviderDenylist      map[peer.ID]bool
	ProviderAllowlist     map[peer.ID]bool
	DefaultProviderConfig ProviderConfig
	ProviderConfigs       map[peer.ID]ProviderConfig
	PaidRetrievals        bool
}

func (cfg *RetrieverConfig) GetProviderConfig(peer peer.ID) ProviderConfig {
	provCfg := cfg.DefaultProviderConfig

	if individual, ok := cfg.ProviderConfigs[peer]; ok {
		if individual.MaxConcurrentRetrievals != 0 {
			provCfg.MaxConcurrentRetrievals = individual.MaxConcurrentRetrievals
		}

		if individual.RetrievalTimeout != 0 {
			provCfg.RetrievalTimeout = individual.RetrievalTimeout
		}
	}

	return provCfg
}
