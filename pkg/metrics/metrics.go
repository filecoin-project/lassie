package metrics

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Measures
var (
	BitswapRequestCount                = stats.Int64("bitswap_request_total", "The number of bitswap requests received", stats.UnitDimensionless)
	BitswapResponseCount               = stats.Int64("bitswap_response_total", "The number of bitswap responses", stats.UnitDimensionless)
	BitswapRetrieverRequestCount       = stats.Int64("bitswap_retriever_request_total", "The number of bitswap messages that required a retriever lookup", stats.UnitDimensionless)
	BlockstoreCacheHitCount            = stats.Int64("blockstore_cache_hit_total", "The number of blocks from the local blockstore served to peers", stats.UnitDimensionless)
	BytesTransferredTotal              = stats.Int64("data_transferred_bytes_total", "The number of bytes transferred from storage providers to retrieval clients", stats.UnitBytes)
	RetrievalDealCost                  = stats.Int64("retrieval_deal_cost_fil", "The cost in FIL of a retrieval deal with a storage provider", stats.UnitDimensionless)
	RetrievalDealActiveCount           = stats.Int64("retrieval_deal_active_total", "The number of active retrieval deals that have not yet succeeded or failed", stats.UnitDimensionless)
	RetrievalDealDuration              = stats.Float64("retrieval_deal_duration_seconds", "The duration in seconds of a retrieval deal with a storage provider", stats.UnitSeconds)
	RetrievalDealFailCount             = stats.Int64("retrieval_deal_fail_total", "The number of failed retrieval deals with storage providers", stats.UnitDimensionless)
	RetrievalDealSize                  = stats.Int64("retrieval_deal_size_bytes", "The size in bytes of a retrieval deal with a storage provider", stats.UnitDimensionless)
	RetrievalDealSuccessCount          = stats.Int64("retrieval_deal_success_total", "The number of successful retrieval deals with storage providers", stats.UnitDimensionless)
	RetrievalRequestCount              = stats.Int64("retrieval_request_total", "The number of retrieval deals initiated with storage providers", stats.UnitDimensionless)
	RetrievalErrorPaychCount           = stats.Int64("retrieval_error_paych_total", "The number of retrieval errors for 'failed to get payment channel'", stats.UnitDimensionless)
	RetrievalErrorRejectedCount        = stats.Int64("retrieval_error_rejected_total", "The number of retrieval errors for 'response rejected'", stats.UnitDimensionless)
	RetrievalErrorTooManyCount         = stats.Int64("retrieval_error_toomany_total", "The number of retrieval errors for 'Too many retrieval deals received'", stats.UnitDimensionless)
	RetrievalErrorACLCount             = stats.Int64("retrieval_error_acl_total", "The number of retrieval errors for 'Access Control'", stats.UnitDimensionless)
	RetrievalErrorMaintenanceCount     = stats.Int64("retrieval_error_maintenance_total", "The number of retrieval errors for 'Under maintenance, retry later'", stats.UnitDimensionless)
	RetrievalErrorNoOnlineCount        = stats.Int64("retrieval_error_noonline_total", "The number of retrieval errors for 'miner is not accepting online retrieval deals'", stats.UnitDimensionless)
	RetrievalErrorUnconfirmedCount     = stats.Int64("retrieval_error_unconfirmed_total", "The number of retrieval errors for 'unconfirmed block transfer'", stats.UnitDimensionless)
	RetrievalErrorTimeoutCount         = stats.Int64("retrieval_error_timeout_total", "The number of retrieval errors for 'timeout after X'", stats.UnitDimensionless)
	RetrievalErrorOtherCount           = stats.Int64("retrieval_error_other_total", "The number of retrieval errors with uncategorized causes", stats.UnitDimensionless)
	QueryErrorFailedToDialCount        = stats.Int64("query_error_failed_to_dial", "The number of query errors because we coult not connect to the provider", stats.UnitDimensionless)
	QueryErrorFailedToOpenStreamCount  = stats.Int64("query_error_failed_to_open_stream", "The number of query errors where the miner did not respond on the retrieval protocol", stats.UnitDimensionless)
	QueryErrorResponseEOFCount         = stats.Int64("query_error_response_eof", "The number of query errors because where the response terminated early", stats.UnitDimensionless)
	QueryErrorResponseStreamResetCount = stats.Int64("query_error_response_stream_reset", "The number of query errors because where the response stream was reset", stats.UnitDimensionless)
	QueryErrorDAGStoreCount            = stats.Int64("query_error_dagstore", "The number of query failures cause the provider experienced a DAG Store error", stats.UnitDimensionless)
	QueryErrorDealNotFoundCount        = stats.Int64("query_error_dealstate", "The number of query failures cause the provider couldn't find the relevant deal", stats.UnitDimensionless)
	QueryErrorOtherCount               = stats.Int64("query_error_error_other_total", "The number of retrieval errors with uncategorized causes", stats.UnitDimensionless)

	// Indexer Candidates
	IndexerCandidatesPerRequestCount          = stats.Int64("indexer_candidates_per_request_total", "The number of indexer candidates received per request", stats.UnitDimensionless)
	RequestWithIndexerCandidatesCount         = stats.Int64("request_with_indexer_candidates_total", "The number of requests that result in non-zero candidates from the indexer", stats.UnitDimensionless)
	RequestWithIndexerCandidatesFilteredCount = stats.Int64("request_with_indexer_candidates_filtered_total", "The number of requests that result in non-zero candidates from the indexer after filtering", stats.UnitDimensionless)

	// Query
	RequestWithSuccessfulQueriesCount         = stats.Int64("request_with_successful_queries_total", "The number of requests that result in a non-zero number of successful queries from SPs", stats.UnitDimensionless)
	RequestWithSuccessfulQueriesFilteredCount = stats.Int64("request_with_successful_queries_filtered_total", "The number of requests that result in a non-zero number of successful queries from SPs after filtering", stats.UnitDimensionless)
	SuccessfulQueriesPerRequestCount          = stats.Int64("successful_queries_per_request_total", "The number of successful queries received per request", stats.UnitDimensionless)
	SuccessfulQueriesPerRequestFilteredCount  = stats.Int64("successful_queries_per_request_filtered_total", "The number of successful queries received per request after filtering", stats.UnitDimensionless)

	// Retrieval
	FailedRetrievalsPerRequestCount = stats.Int64("failed_retrievals_per_request_total", "The number of failed retrieval attempts per request", stats.UnitDimensionless)
)

// QueryErrorMetricMatches is a mapping of retrieval error message substrings
// during the query phase (i.e. that can be matched against error messages)
// and metrics to report for that error.
var QueryErrorMetricMatches = map[string]*stats.Int64Measure{
	"failed to dial":                        QueryErrorFailedToDialCount,
	"failed to open stream to peer":         QueryErrorFailedToOpenStreamCount,
	"failed to read response: EOF":          QueryErrorResponseEOFCount,
	"failed to read response: stream reset": QueryErrorResponseStreamResetCount,
}

// QueryResponseMetricMatches is a mapping of retrieval error message substrings
// during the query phase when a response is sent but it is a failure
// and metrics to report for that failure.
var QueryResponseMetricMatches = map[string]*stats.Int64Measure{
	"getting pieces for cid":             QueryErrorDAGStoreCount,
	"failed to fetch storage deal state": QueryErrorDealNotFoundCount,
}

// ErrorMetricMatches is a mapping of retrieval error message substrings (i.e.
// that can be matched against error messages) and metrics to report for that
// error.
var ErrorMetricMatches = map[string]*stats.Int64Measure{
	"failed to get payment channel":                 RetrievalErrorPaychCount,
	"response rejected":                             RetrievalErrorRejectedCount,
	"Too many retrieval deals received":             RetrievalErrorTooManyCount,
	"Access Control":                                RetrievalErrorACLCount,
	"Under maintenance, retry later":                RetrievalErrorMaintenanceCount,
	"miner is not accepting online retrieval deals": RetrievalErrorNoOnlineCount,
	"unconfirmed block transfer":                    RetrievalErrorUnconfirmedCount,
	"timeout after ":                                RetrievalErrorTimeoutCount,
}

// Tags
var (
	BitswapDontHaveReason, _ = tag.NewKey("bitswap_dont_have_reason")
	BitswapTopic, _          = tag.NewKey("bitswap_topic")
	EndpointURL, _           = tag.NewKey("endpoint_url")

	Error, _  = tag.NewKey("error")
	Method, _ = tag.NewKey("method")
	Status, _ = tag.NewKey("status")
)

// Views
var (
	bitswapRequestView = &view.View{
		Measure:     BitswapRequestCount,
		Aggregation: view.Count(),
	}
	bitswapResponseView = &view.View{
		Measure:     BitswapResponseCount,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{BitswapTopic, BitswapDontHaveReason},
	}
	bitswapRetreiverRequestView = &view.View{
		Measure:     BitswapRetrieverRequestCount,
		Aggregation: view.Count(),
	}
	blockstoreCacheHitView = &view.View{
		Measure:     BlockstoreCacheHitCount,
		Aggregation: view.Count(),
	}
	bytesTransferredView = &view.View{
		Measure:     BytesTransferredTotal,
		Aggregation: view.Sum(),
	}
	failedRetrievalsPerRequestView = &view.View{
		Measure:     FailedRetrievalsPerRequestCount,
		Aggregation: view.Distribution(0, 1, 2, 3, 4, 5, 10, 20, 40),
	}
	requestWithIndexerCandidatesFilteredView = &view.View{
		Measure:     RequestWithIndexerCandidatesFilteredCount,
		Aggregation: view.Count(),
	}
	requestWithIndexerCandidatesView = &view.View{
		Measure:     RequestWithIndexerCandidatesCount,
		Aggregation: view.Count(),
	}
	requestWithSuccessfulQueriesFilteredView = &view.View{
		Measure:     RequestWithSuccessfulQueriesFilteredCount,
		Aggregation: view.Count(),
	}
	requestWithSuccessfulQueriesView = &view.View{
		Measure:     RequestWithSuccessfulQueriesCount,
		Aggregation: view.Count(),
	}
	indexerCandidatesPerRequestView = &view.View{
		Measure:     IndexerCandidatesPerRequestCount,
		Aggregation: view.Distribution(0, 1, 2, 3, 4, 5, 10, 20, 40),
	}
	retrievalDealActiveView = &view.View{
		Measure:     RetrievalDealActiveCount,
		Aggregation: view.Count(),
	}
	retrievalDealCostView = &view.View{
		Measure:     RetrievalDealCost,
		Aggregation: view.Distribution(),
	}
	retrievalDealDurationView = &view.View{
		Measure:     RetrievalDealDuration,
		Aggregation: view.Distribution(0, 10, 20, 30, 40, 50, 60, 120, 240, 480, 540, 600),
	}
	retrievalDealFailView = &view.View{
		Measure:     RetrievalDealFailCount,
		Aggregation: view.Count(),
	}
	retrievalDealSuccessView = &view.View{
		Measure:     RetrievalDealSuccessCount,
		Aggregation: view.Count(),
	}
	retrievalDealSizeView = &view.View{
		Measure:     RetrievalDealSize,
		Aggregation: view.Distribution(),
	}
	retrievalRequestCountView = &view.View{
		Measure:     RetrievalRequestCount,
		Aggregation: view.Count(),
	}
	retrievalErrorPaychView = &view.View{
		Measure:     RetrievalErrorPaychCount,
		Aggregation: view.Count(),
	}
	retrievalErrorRejectedView = &view.View{
		Measure:     RetrievalErrorRejectedCount,
		Aggregation: view.Count(),
	}
	retrievalErrorTooManyView = &view.View{
		Measure:     RetrievalErrorTooManyCount,
		Aggregation: view.Count(),
	}
	retrievalErrorACLView = &view.View{
		Measure:     RetrievalErrorACLCount,
		Aggregation: view.Count(),
	}
	retrievalErrorMaintenanceView = &view.View{
		Measure:     RetrievalErrorMaintenanceCount,
		Aggregation: view.Count(),
	}
	retrievalErrorNoOnlineView = &view.View{
		Measure:     RetrievalErrorNoOnlineCount,
		Aggregation: view.Count(),
	}
	retrievalErrorUnconfirmedView = &view.View{
		Measure:     RetrievalErrorUnconfirmedCount,
		Aggregation: view.Count(),
	}
	retrievalErrorTimeoutView = &view.View{
		Measure:     RetrievalErrorTimeoutCount,
		Aggregation: view.Count(),
	}
	retrievalErrorOtherView = &view.View{
		Measure:     RetrievalErrorOtherCount,
		Aggregation: view.Count(),
	}
	successfulQueriesPerRequestFilteredView = &view.View{
		Measure:     SuccessfulQueriesPerRequestFilteredCount,
		Aggregation: view.Distribution(0, 1, 2, 3, 4, 5, 10, 20, 40),
	}
	successfulQueriesPerRequestView = &view.View{
		Measure:     SuccessfulQueriesPerRequestCount,
		Aggregation: view.Distribution(0, 1, 2, 3, 4, 5, 10, 20, 40),
	}
	queryErrorFailedToDialView = &view.View{
		Measure:     QueryErrorFailedToDialCount,
		Aggregation: view.Count(),
	}
	queryErrorFailedToOpenStreamView = &view.View{
		Measure:     QueryErrorFailedToOpenStreamCount,
		Aggregation: view.Count(),
	}
	queryErrorResponseEOFView = &view.View{
		Measure:     QueryErrorResponseEOFCount,
		Aggregation: view.Count(),
	}
	queryErrorResponseStreamResetView = &view.View{
		Measure:     QueryErrorResponseStreamResetCount,
		Aggregation: view.Count(),
	}
	queryErrorDAGStoreView = &view.View{
		Measure:     QueryErrorDAGStoreCount,
		Aggregation: view.Count(),
	}
	queryErrorDealNotFoundView = &view.View{
		Measure:     QueryErrorDealNotFoundCount,
		Aggregation: view.Count(),
	}
	queryErrorOtherView = &view.View{
		Measure:     QueryErrorOtherCount,
		Aggregation: view.Count(),
	}
)

var DefaultViews = []*view.View{
	bitswapRequestView,
	bitswapResponseView,
	bitswapRetreiverRequestView,
	blockstoreCacheHitView,
	bytesTransferredView,
	failedRetrievalsPerRequestView,
	requestWithIndexerCandidatesFilteredView,
	requestWithIndexerCandidatesView,
	requestWithSuccessfulQueriesFilteredView,
	requestWithSuccessfulQueriesView,
	indexerCandidatesPerRequestView,
	retrievalDealActiveView,
	retrievalDealCostView,
	retrievalDealDurationView,
	retrievalDealFailView,
	retrievalDealSuccessView,
	retrievalDealSizeView,
	retrievalRequestCountView,
	retrievalErrorPaychView,
	retrievalErrorRejectedView,
	retrievalErrorTooManyView,
	retrievalErrorACLView,
	retrievalErrorMaintenanceView,
	retrievalErrorNoOnlineView,
	retrievalErrorUnconfirmedView,
	retrievalErrorTimeoutView,
	retrievalErrorOtherView,
	successfulQueriesPerRequestFilteredView,
	successfulQueriesPerRequestView,
	queryErrorFailedToDialView,
	queryErrorFailedToOpenStreamView,
	queryErrorResponseEOFView,
	queryErrorResponseStreamResetView,
	queryErrorDAGStoreView,
	queryErrorDealNotFoundView,
	queryErrorOtherView,
}
