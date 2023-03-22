package indexerlookup

import (
	"net/http"
	"net/url"
	"time"
)

type (
	Option  func(*options) error
	options struct {
		asyncResultsChanBuffer int
		httpEndpoint           *url.URL
		httpClient             *http.Client
		httpClientTimeout      time.Duration
		httpUserAgent          string
		ipfsDhtCascade         bool
		legacyCascade          bool
	}
)

func newOptions(o ...Option) (*options, error) {
	const defaultEndpoint = "https://cid.contact"
	opts := options{
		asyncResultsChanBuffer: 1,
		httpClient:             http.DefaultClient,
		httpClientTimeout:      time.Minute,
		httpUserAgent:          "lassie",
		ipfsDhtCascade:         true,
		legacyCascade:          true,
	}
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	var err error
	if opts.httpEndpoint == nil {
		opts.httpEndpoint, err = url.Parse(defaultEndpoint)
		if err != nil {
			// We can also panic here; but considering we can also return error
			// let there be less panics in this world, and sanity check defaults
			// in unit tests instead.
			return nil, err
		}
	}
	opts.httpClient.Timeout = opts.httpClientTimeout
	return &opts, nil
}

// WithHttpClient sets the http.Client used to contact the indexer.
// Defaults to http.DefaultClient if unspecified.
func WithHttpClient(c *http.Client) Option {
	return func(o *options) error {
		o.httpClient = c
		return nil
	}
}

// WithHttpClientTimeout sets the timeout for the HTTP requests sent to the indexer.
// Defaults to one minute if unspecified.
func WithHttpClientTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.httpClientTimeout = t
		return nil
	}
}

// WithHttpEndpoint sets the indexer HTTP API endpoint.
// Defaults to https://cid.contact if unspecified.
func WithHttpEndpoint(e *url.URL) Option {
	return func(o *options) error {
		o.httpEndpoint = e
		return nil
	}
}

// WithHttpUserAgent sets the User-Agent header value when contacting the indexer HTTP API.
// Setting this option to empty string will disable inclusion of User-Agent header.
// Defaults to "lassie" if unspecified.
func WithHttpUserAgent(a string) Option {
	return func(o *options) error {
		o.httpUserAgent = a
		return nil
	}
}

// WithAsyncResultsChanBuffer sets the channel buffer returned by IndexerCandidateFinder.FindCandidatesAsync.
// Defaults to 1 if unspecified.
func WithAsyncResultsChanBuffer(i int) Option {
	return func(o *options) error {
		o.asyncResultsChanBuffer = i
		return nil
	}
}

// WithIpfsDhtCascade sets weather to cascade lookups onto the IPFS DHT.
// Enabled by default if unspecified.
func WithIpfsDhtCascade(b bool) Option {
	return func(o *options) error {
		o.ipfsDhtCascade = b
		return nil
	}
}

// WithLegacyCascade sets weather to cascade finds legacy providers connecting only over bitswap
// Enabled by default if unspecified.
func WithLegacyCascade(b bool) Option {
	return func(o *options) error {
		o.legacyCascade = b
		return nil
	}
}
