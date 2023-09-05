package main

import (
	"context"
	"testing"
	"time"

	a "github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	l "github.com/filecoin-project/lassie/pkg/lassie"
	h "github.com/filecoin-project/lassie/pkg/server/http"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestDaemonCommandFlags(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		assert daemonRunFunc
	}{
		{
			name: "with default args",
			args: []string{"daemon"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				// lassie config
				require.Equal(t, nil, lCfg.Finder)
				require.NotNil(t, lCfg.Host, "host should not be nil")
				require.Equal(t, 20*time.Second, lCfg.ProviderTimeout)
				require.Equal(t, uint(0), lCfg.ConcurrentSPRetrievals)
				require.Equal(t, 0*time.Second, lCfg.GlobalTimeout)
				require.Equal(t, 0, len(lCfg.Libp2pOptions))
				require.Equal(t, 0, len(lCfg.Protocols))
				require.Equal(t, 0, len(lCfg.ProviderBlockList))
				require.Equal(t, 0, len(lCfg.ProviderAllowList))
				require.Equal(t, 32, lCfg.BitswapConcurrency)
				require.Equal(t, 12, lCfg.BitswapConcurrencyPerRetrieval)

				// http server config
				require.Equal(t, "127.0.0.1", hCfg.Address)
				require.Equal(t, uint(0), hCfg.Port)
				require.Equal(t, uint64(0), hCfg.MaxBlocksPerRequest)
				require.Equal(t, "", hCfg.AccessToken)

				// event recorder config
				require.Equal(t, "", erCfg.EndpointURL)
				require.Equal(t, "", erCfg.EndpointAuthorization)
				require.Equal(t, "", erCfg.InstanceID)
				return nil
			},
		},
		{
			name: "with libp2p low and high connection thresholds and concurrent sp retrievals",
			args: []string{"daemon", "--libp2p-conns-lowwater", "10", "--libp2p-conns-highwater", "20"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.NotNil(t, lCfg.Host, "host should not be nil")
				cmgr, ok := lCfg.Host.ConnManager().(*connmgr.BasicConnMgr)
				require.True(t, ok)
				cmInfo := cmgr.GetInfo()
				require.Equal(t, cmInfo.LowWater, 10)
				require.Equal(t, cmInfo.HighWater, 20)
				return nil
			},
		},
		{
			name: "with concurrent sp retrievals",
			args: []string{"daemon", "--concurrent-sp-retrievals", "10"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, uint(10), lCfg.ConcurrentSPRetrievals)
				return nil
			},
		},
		{
			name: "with temp directory",
			args: []string{"daemon", "--tempdir", "/mytmpdir"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "/mytmpdir", hCfg.TempDir)
				return nil
			},
		},
		{
			name: "with provider timeout",
			args: []string{"daemon", "--provider-timeout", "30s"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, 30*time.Second, lCfg.ProviderTimeout)
				return nil
			},
		},
		{
			name: "with global timeout",
			args: []string{"daemon", "--global-timeout", "30s"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, 30*time.Second, lCfg.GlobalTimeout)
				return nil
			},
		},
		{
			name: "with protocols",
			args: []string{"daemon", "--protocols", "bitswap,graphsync"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportGraphsyncFilecoinv1}, lCfg.Protocols)
				return nil
			},
		},
		{
			name: "with exclude providers",
			args: []string{"daemon", "--exclude-providers", "12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4,12D3KooWPNbkEgjdBNeaCGpsgCrPRETe4uBZf1ShFXStobdN18ys"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				p1, err := peer.Decode("12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")
				require.NoError(t, err)
				p2, err := peer.Decode("12D3KooWPNbkEgjdBNeaCGpsgCrPRETe4uBZf1ShFXStobdN18ys")
				require.NoError(t, err)

				require.Equal(t, true, lCfg.ProviderBlockList[p1])
				require.Equal(t, true, lCfg.ProviderBlockList[p2])
				return nil
			},
		},
		{
			name: "with bitswap concurrency",
			args: []string{"daemon", "--bitswap-concurrency", "10"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, 10, lCfg.BitswapConcurrency)
				return nil
			},
		},
		{
			name: "with address",
			args: []string{"daemon", "--address", "0.0.0.0"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "0.0.0.0", hCfg.Address)
				return nil
			},
		},
		{
			name: "with port",
			args: []string{"daemon", "--port", "1234"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, uint(1234), hCfg.Port)
				return nil
			},
		},
		{
			name: "with max blocks",
			args: []string{"daemon", "--maxblocks", "10"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, uint64(10), hCfg.MaxBlocksPerRequest)
				return nil
			},
		},
		{
			name: "with event recorder url",
			args: []string{"daemon", "--event-recorder-url", "https://myeventrecorder.com/v1/retrieval-events"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "https://myeventrecorder.com/v1/retrieval-events", erCfg.EndpointURL)
				return nil
			},
		},
		{
			name: "with event recorder auth",
			args: []string{"daemon", "--event-recorder-auth", "secret"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "secret", erCfg.EndpointAuthorization)
				return nil
			},
		},
		{
			name: "with event recorder instance ID",
			args: []string{"daemon", "--event-recorder-instance-id", "myinstanceid"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "myinstanceid", erCfg.InstanceID)
				return nil
			},
		},
		{
			name: "with access token",
			args: []string{"daemon", "--access-token", "super-secret"},
			assert: func(ctx context.Context, lCfg *l.LassieConfig, hCfg h.HttpServerConfig, erCfg *a.EventRecorderConfig) error {
				require.Equal(t, "super-secret", hCfg.AccessToken)
				return nil
			},
		},
	}

	for _, test := range tests {
		daemonRun = test.assert
		app := &cli.App{
			Name:     "cli-test",
			Flags:    daemonFlags,
			Commands: []*cli.Command{daemonCmd},
		}

		t.Run(test.name, func(t *testing.T) {
			err := app.Run(append([]string{"cli-test"}, test.args...))
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
