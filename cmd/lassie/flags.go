package main

import (
	"strings"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

// IsVerbose is a global var signaling if the CLI is running in
// verbose mode or not (default: false).
var IsVerbose bool

// FlagVerbose enables verbose mode, which shows verbose information about
// operations invoked in the CLI. It should be included as a flag on the
// top-level command (e.g. lassie -v).
var FlagVerbose = &cli.BoolFlag{
	Name:        "verbose",
	Aliases:     []string{"v"},
	Usage:       "enable verbose mode for logging",
	Destination: &IsVerbose,
}

// IsVeryVerbose is a global var signaling if the CLI is running in
// very verbose mode or not (default: false).
var IsVeryVerbose bool

// FlagVerbose enables verbose mode, which shows verbose information about
// operations invoked in the CLI. It should be included as a flag on the
// top-level command (e.g. lassie -v).
var FlagVeryVerbose = &cli.BoolFlag{
	Name:        "very-verbose",
	Aliases:     []string{"vv"},
	Usage:       "enable very verbose mode for debugging",
	Destination: &IsVeryVerbose,
}

// FlagEventRecorderAuth asks for and provides the authorization token for
// sending metrics to an event recorder API via a Basic auth Authorization
// HTTP header. Value will formatted as "Basic <value>" if provided.
var FlagEventRecorderAuth = &cli.StringFlag{
	Name:    "event-recorder-auth",
	Usage:   "the authorization token for an event recorder API",
	EnvVars: []string{"LASSIE_EVENT_RECORDER_AUTH"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderInstanceId = &cli.StringFlag{
	Name:        "event-recorder-instance-id",
	Usage:       "the instance ID to use for an event recorder API request",
	DefaultText: "a random v4 uuid",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_INSTANCE_ID"},
}

// FlagEventRecorderUrl asks for and provides the URL for an event recorder API
// to send metrics to.
var FlagEventRecorderUrl = &cli.StringFlag{
	Name:    "event-recorder-url",
	Usage:   "the url of an event recorder API",
	EnvVars: []string{"LASSIE_EVENT_RECORDER_URL"},
}

// FlagExposeMetrics exposes prometheus metrics at /metrics on the daemon http
// server.
var FlagExposeMetrics = &cli.BoolFlag{
	Name:    "expose-metrics",
	Usage:   "expose metrics at /metrics",
	EnvVars: []string{"LASSIE_EXPOSE_METRICS"},
}

// FlagMetricsAddress sets the address on which to expose metrics and pprof information
var FlagMetricsAddress = &cli.StringFlag{
	Name:        "metrics-address",
	Usage:       "the address to expose metrics for prometheus and pprof when expose metrics is true",
	Value:       "127.0.0.1",
	DefaultText: "127.0.0.1",
	EnvVars:     []string{"LASSIE_METRICS_ADDRESS"},
}

// FlagMetricsPort sets the port on which to expose metrics and pprof information
var FlagMetricsPort = &cli.UintFlag{
	Name:        "metrics-port",
	Usage:       "the port to expose metrics for prometheus and pprof when expose metrics is true",
	Value:       0,
	DefaultText: "random",
	EnvVars:     []string{"LASSIE_METRICS_PORT"},
}

var providerBlockList map[peer.ID]bool

var FlagExcludeProviders = &cli.StringFlag{
	Name:        "exclude-providers",
	DefaultText: "All providers allowed",
	Usage:       "Provider peer IDs, seperated by a comma. Example: 12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
	EnvVars:     []string{"LASSIE_EXCLUDE_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		providerBlockList = make(map[peer.ID]bool)
		vs := strings.Split(v, ",")
		for _, v := range vs {
			peerID, err := peer.Decode(v)
			if err != nil {
				return err
			}
			providerBlockList[peerID] = true
		}
		return nil
	},
}

var protocols []multicodec.Code
var FlagProtocols = &cli.StringFlag{
	Name:        "protocols",
	DefaultText: "bitswap,graphsync",
	Usage:       "List of retrieval protocols to use, seperated by a comma",
	EnvVars:     []string{"LASSIE_SUPPORTED_PROTOCOLS"},
	Action: func(cctx *cli.Context, v string) error {
		var err error
		protocols, err = types.ParseProtocolsString(v)
		return err
	},
}

// FlagDisableBitswap turns off all retrievals over the bitswap protocol
var FlagDisableBitswap = &cli.BoolFlag{
	Name:    "disable-bitswap",
	Usage:   "turn off bitswap retrievals",
	EnvVars: []string{"LASSIE_DISABLE_BITSWAP"},
}
