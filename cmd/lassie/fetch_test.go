package main

import (
	"context"
	"io"
	"testing"
	"time"

	a "github.com/filecoin-project/lassie/pkg/aggregateeventrecorder"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	l "github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestFetchCommandFlags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		shouldError bool
		assertRun   fetchRunFunc
	}{
		{
			name: "with default args",
			args: []string{"fetch", "bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4"},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				// fetch specific params
				require.Equal(t, "bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4", rootCid.String())
				require.Equal(t, "", path)
				require.Equal(t, string(types.DagScopeAll), dagScope)
				require.Equal(t, false, progress)
				require.Equal(t, "bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4.car", outfile)

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
				require.Equal(t, "/tmp", lCfg.TempDir)
				require.Equal(t, 6, lCfg.BitswapConcurrency)

				// event recorder config
				require.Equal(t, "", erCfg.EndpointURL)
				require.Equal(t, "", erCfg.EndpointAuthorization)
				require.Equal(t, "", erCfg.InstanceID)
				return nil
			},
		},
		{
			name: "with bad root cid",
			args: []string{
				"fetch",
				"not-a-cid",
			},
			shouldError: true,
		},
		{
			name: "with root cid path",
			args: []string{
				"fetch",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4/birb.mp4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "/birb.mp4", path)
				return nil
			},
		},
		{
			name: "with dag scope entity",
			args: []string{
				"fetch",
				"--dag-scope",
				"entity",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, string(types.DagScopeEntity), dagScope)
				return nil
			},
		},
		{
			name: "with dag scope block",
			args: []string{
				"fetch",
				"--dag-scope",
				"block",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, string(types.DagScopeBlock), dagScope)
				return nil
			},
		},
		{
			name: "with progress",
			args: []string{
				"fetch",
				"--progress",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, true, progress)
				return nil
			},
		},
		{
			name: "with output",
			args: []string{
				"fetch",
				"--output",
				"myfile",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "myfile", outfile)
				return nil
			},
		},
		{
			name: "with providers",
			args: []string{
				"fetch",
				"--providers",
				"/ip4/127.0.0.1/tcp/5000/p2p/12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.IsType(t, &retriever.DirectCandidateFinder{}, lCfg.Finder, "finder should be a DirectCandidateFinder when providers are specified")
				return nil
			},
		},
		{
			name: "with ipni endpoint",
			args: []string{
				"fetch",
				"--ipni-endpoint",
				"https://cid.contact",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.IsType(t, &indexerlookup.IndexerCandidateFinder{}, lCfg.Finder, "finder should be an IndexerCandidateFinder when providing an ipni endpoint")
				return nil
			},
		},
		{
			name: "with bad ipni endpoint",
			args: []string{
				"fetch",
				"--ipni-endpoint",
				"not-a-url",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			shouldError: true,
		},
		{
			name: "with temp directory",
			args: []string{
				"fetch",
				"--tempdir",
				"/mytmpdir",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "/mytmpdir", lCfg.TempDir)
				return nil
			},
		},
		{
			name: "with provider timeout",
			args: []string{
				"fetch",
				"--provider-timeout",
				"30s",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, 30*time.Second, lCfg.ProviderTimeout)
				return nil
			},
		},
		{
			name: "with global timeout",
			args: []string{
				"fetch",
				"--global-timeout",
				"30s",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, 30*time.Second, lCfg.GlobalTimeout)
				return nil
			},
		},
		{
			name: "with protocols",
			args: []string{
				"fetch",
				"--protocols",
				"bitswap,graphsync",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportGraphsyncFilecoinv1}, lCfg.Protocols)
				return nil
			},
		},
		{
			name: "with exclude providers",
			args: []string{
				"fetch",
				"--exclude-providers",
				"12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4,12D3KooWPNbkEgjdBNeaCGpsgCrPRETe4uBZf1ShFXStobdN18ys",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
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
			args: []string{
				"fetch",
				"--bitswap-concurrency",
				"10",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, 10, lCfg.BitswapConcurrency)
				return nil
			},
		},
		{
			name: "with event recorder url",
			args: []string{
				"fetch",
				"--event-recorder-url",
				"https://myeventrecorder.com/v1/retrieval-events",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "https://myeventrecorder.com/v1/retrieval-events", erCfg.EndpointURL)
				return nil
			},
		},
		{
			name: "with event recorder auth",
			args: []string{
				"fetch",
				"--event-recorder-auth",
				"secret",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "secret", erCfg.EndpointAuthorization)
				return nil
			},
		},
		{
			name: "with event recorder instance ID",
			args: []string{
				"fetch",
				"--event-recorder-instance-id",
				"myinstanceid",
				"bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4",
			},
			assertRun: func(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
				require.Equal(t, "myinstanceid", erCfg.InstanceID)
				return nil
			},
		},
	}

	for _, test := range tests {
		fetchRun = test.assertRun
		if test.shouldError {
			fetchRun = noopRun
		}

		app := &cli.App{
			Name:     "cli-test",
			Flags:    fetchFlags,
			Commands: []*cli.Command{fetchCmd},
		}

		t.Run(test.name, func(t *testing.T) {
			err := app.Run(append([]string{"cli-test"}, test.args...))
			if err != nil && !test.shouldError {
				t.Fatal(err)
			}

			if err == nil && test.shouldError {
				t.Fatal("expected error")
			}
		})
	}
}

func noopRun(ctx context.Context, lCfg *l.LassieConfig, erCfg *a.EventRecorderConfig, msgWriter io.Writer, dataWriter io.Writer, rootCid cid.Cid, path string, dagScope string, tempDir string, progress bool, outfile string) error {
	return nil
}
