package main

import (
	"os"
	"strings"

	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var (
	defaultTempDirectory     string   = os.TempDir() // use the system default temp dir
	verboseLoggingSubsystems []string = []string{    // verbose logging is enabled for these subsystems when using the verbose or very-verbose flags
		"lassie/main",
		"lassie/retriever",
		"lassie/httpserver",
		"lassie/indexerlookup",
		"lassie/aggregateeventrecorder",
	}
)

// FlagVerbose enables verbose mode, which shows info information about
// operations invoked in the CLI.
var FlagVerbose = &cli.BoolFlag{
	Name:    "verbose",
	Aliases: []string{"v"},
	Usage:   "enable verbose mode for logging",
	Action:  setLogLevel("INFO"),
}

// FlagVeryVerbose enables very verbose mode, which shows debug information about
// operations invoked in the CLI.
var FlagVeryVerbose = &cli.BoolFlag{
	Name:    "very-verbose",
	Aliases: []string{"vv"},
	Usage:   "enable very verbose mode for debugging",
	Action:  setLogLevel("DEBUG"),
}

// setLogLevel returns a CLI Action function that sets the
// logging level for the given subsystems to the given level.
// It is used as an action for the verbose and very-verbose flags.
func setLogLevel(level string) func(*cli.Context, bool) error {
	return func(cctx *cli.Context, _ bool) error {
		// don't override logging if set in the environment.
		if os.Getenv("GOLOG_LOG_LEVEL") != "" {
			return nil
		}
		// set the logging level for the given subsystems
		for _, name := range verboseLoggingSubsystems {
			_ = log.SetLogLevel(name, level)
		}
		return nil
	}
}

// FlagEventRecorderAuth asks for and provides the authorization token for
// sending metrics to an event recorder API via a Basic auth Authorization
// HTTP header. Value will formatted as "Basic <value>" if provided.
var FlagEventRecorderAuth = &cli.StringFlag{
	Name:        "event-recorder-auth",
	Usage:       "the authorization token for an event recorder API",
	DefaultText: "no authorization token will be used",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_AUTH"},
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
	Name:        "event-recorder-url",
	Usage:       "the url of an event recorder API",
	DefaultText: "no event recorder API will be used",
	EnvVars:     []string{"LASSIE_EVENT_RECORDER_URL"},
}

var providerBlockList map[peer.ID]bool
var FlagExcludeProviders = &cli.StringFlag{
	Name:        "exclude-providers",
	DefaultText: "All providers allowed",
	Usage:       "Provider peer IDs to exclude, separated by a comma. Note: peer IDs are opaque identifiers from the delegated router. Example: 12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
	EnvVars:     []string{"LASSIE_EXCLUDE_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		// Do nothing if given an empty string
		if v == "" {
			return nil
		}

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

var fetchProviders []types.Provider

var FlagAllowProviders = &cli.StringFlag{
	Name:        "providers",
	Aliases:     []string{"provider"},
	DefaultText: "Providers will be discovered automatically",
	Usage: "Comma-separated addresses of HTTP gateways to use instead of " +
		"automatic discovery. Accepts HTTP URLs and multiaddrs with /http or /https. " +
		"Example: https://ipfs.io,/dns/gateway.example.com/tcp/443/https",
	EnvVars: []string{"LASSIE_ALLOW_PROVIDERS"},
	Action: func(cctx *cli.Context, v string) error {
		if v == "" {
			return nil
		}
		var err error
		fetchProviders, err = types.ParseProviderStrings(v)
		return err
	},
}

var FlagTempDir = &cli.StringFlag{
	Name:        "tempdir",
	Aliases:     []string{"td"},
	Usage:       "directory to store temporary files while downloading",
	Value:       defaultTempDirectory,
	DefaultText: "os temp directory",
	EnvVars:     []string{"LASSIE_TEMP_DIRECTORY"},
}

var FlagGlobalTimeout = &cli.DurationFlag{
	Name:    "global-timeout",
	Aliases: []string{"gt"},
	Usage:   "consider it an error after not completing a retrieval after this amount of time",
	EnvVars: []string{"LASSIE_GLOBAL_TIMEOUT"},
}

var FlagDelegatedRoutingEndpoint = &cli.StringFlag{
	Name:        "delegated-routing-endpoint",
	Aliases:     []string{"delegated"},
	DefaultText: "Defaults to https://cid.contact",
	Usage:       "HTTP endpoint of the delegated routing service used to discover providers.",
	EnvVars:     []string{"LASSIE_DELEGATED_ROUTING_ENDPOINT"},
}

// FlagSkipBlockVerification disables per-block hash verification.
// WARNING: This is dangerous and should only be used for benchmarking.
var FlagSkipBlockVerification = &cli.BoolFlag{
	Name:    "skip-block-verification",
	Usage:   "DANGEROUS: skip per-block hash verification. Malicious gateways can serve arbitrary data!",
	EnvVars: []string{"LASSIE_SKIP_BLOCK_VERIFICATION"},
}

func ResetGlobalFlags() {
	// Reset global variables here so that they are not used
	// in subsequent calls to commands during testing.
	fetchProviders = make([]types.Provider, 0)
	providerBlockList = make(map[peer.ID]bool)
}
