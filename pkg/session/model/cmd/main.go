package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/session/model"
	"github.com/multiformats/go-multicodec"
)

var (
	GRAPHSYNC_FAST_RELIABLE_LOTS_OF_POPULAR_DATA = model.Provider{
		Name: "graphsync fast, semi-reliable, lots of popular data",
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportGraphsyncFilecoinv1: {
				Candidate:         model.Chance(0.5),
				Success:           model.Chance(0.6),
				ConnectTimeMs:     model.ProbDist{StdDev: 6, Mean: 10},
				TimeToFirstByteMs: model.ProbDist{StdDev: 6, Mean: 10},
				BandwidthBps:      model.ProbDist{StdDev: 1e6, Mean: 1e8}, // Mean of 100Mb/s +/- 1MB/s
				LatencyMs:         model.ProbDist{StdDev: 1, Mean: 20},
				FastRetrieval:     model.Chance(0.9),
				Verified:          model.Chance(0.9),
			},
		},
	}

	GRAPHSYNC_MEDIUM_RELIABLE_SOME_POPULAR_DATA = model.Provider{
		Name: "graphsync medium, semi-reliable, some popular data",
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportGraphsyncFilecoinv1: {
				Candidate:         model.Chance(0.3),
				Success:           model.Chance(0.5),
				ConnectTimeMs:     model.ProbDist{StdDev: 6, Mean: 50},
				TimeToFirstByteMs: model.ProbDist{StdDev: 10, Mean: 25},
				BandwidthBps:      model.ProbDist{StdDev: 1e6, Mean: 1e7}, // Mean of 10MB/s +/- 1MB/s
				LatencyMs:         model.ProbDist{StdDev: 10, Mean: 40},
				FastRetrieval:     model.Chance(0.9),
				Verified:          model.Chance(0.9),
			},
		},
	}

	GRAPHSYNC_MEDIUM_RELIABLE_MINIMAL_POPULAR_DATA = model.Provider{
		Name: "graphsync medium, semi-reliable, minimal popular data",
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportGraphsyncFilecoinv1: {
				Candidate:         model.Chance(0.1),
				Success:           model.Chance(0.5),
				ConnectTimeMs:     model.ProbDist{StdDev: 6, Mean: 50},
				TimeToFirstByteMs: model.ProbDist{StdDev: 10, Mean: 25},
				BandwidthBps:      model.ProbDist{StdDev: 1e6, Mean: 1e7}, // Mean of 10MB/s +/- 1MB/s
				LatencyMs:         model.ProbDist{StdDev: 10, Mean: 40},
				FastRetrieval:     model.Chance(0.9),
				Verified:          model.Chance(0.9),
			},
		},
	}

	GRAPHSYNC_MEDIUM_UNRELIABLE_SOME_POPULAR_DATA = model.Provider{
		Name: "graphsync medium, unreliable, some popular data",
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportGraphsyncFilecoinv1: {
				Candidate:         model.Chance(0.3),
				Success:           model.Chance(0.3),
				ConnectTimeMs:     model.ProbDist{StdDev: 6, Mean: 50},
				TimeToFirstByteMs: model.ProbDist{StdDev: 20, Mean: 50},
				BandwidthBps:      model.ProbDist{StdDev: 1e5, Mean: 1e6}, // Mean of 1MB/s +/- 100KB/s
				LatencyMs:         model.ProbDist{StdDev: 10, Mean: 40},
				FastRetrieval:     model.Chance(0.5),
				Verified:          model.Chance(0.5),
			},
		},
	}

	GRAPHSYNC_MEDIUM_VERY_UNRELIABLE_SOME_POPULAR_DATA = model.Provider{
		Name: "graphsync medium, very unreliable, some popular data",
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportGraphsyncFilecoinv1: {
				Candidate:         model.Chance(0.3),
				Success:           model.Chance(0.1),
				ConnectTimeMs:     model.ProbDist{StdDev: 100, Mean: 200},
				TimeToFirstByteMs: model.ProbDist{StdDev: 6, Mean: 100},
				BandwidthBps:      model.ProbDist{StdDev: 1e5, Mean: 1e6}, // Mean of 1MB/s +/- 100KB/s
				LatencyMs:         model.ProbDist{StdDev: 10, Mean: 100},
				FastRetrieval:     model.Chance(0.2),
				Verified:          model.Chance(0.2),
			},
		},
	}

	HTTP_FAST_SEMIRELIABLE_LOTS_OF_POPULAR_DATA = model.Provider{
		Name: "http fast, semi-reliable, lots of popular data", // e-ipfs?
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportIpfsGatewayHttp: {
				Candidate:         model.Chance(0.5),
				Success:           model.Chance(0.5),
				ConnectTimeMs:     model.ProbDist{StdDev: 0, Mean: 0},
				TimeToFirstByteMs: model.ProbDist{StdDev: 6, Mean: 10},
				BandwidthBps:      model.ProbDist{StdDev: 1e6, Mean: 1e8}, // Mean of 100Mb/s +/- 1MB/s
				LatencyMs:         model.ProbDist{StdDev: 1, Mean: 20},
			},
		},
	}

	HTTP_MEDIUM_FLAKY_SOME_POPULAR_DATA = model.Provider{
		Name: "http medium, semi-reliable, lots of popular data", // e-ipfs?
		Probabilities: map[multicodec.Code]model.Probabilities{
			multicodec.TransportIpfsGatewayHttp: {
				Candidate:         model.Chance(0.7),
				Success:           model.Chance(0.6),
				ConnectTimeMs:     model.ProbDist{StdDev: 0, Mean: 0},
				TimeToFirstByteMs: model.ProbDist{StdDev: 6, Mean: 10},
				BandwidthBps:      model.ProbDist{StdDev: 1e6, Mean: 1e7}, // Mean of 10MB/s +/- 1MB/s
				LatencyMs:         model.ProbDist{StdDev: 10, Mean: 40},
			},
		},
	}
)

func main() {
	seed := time.Now().UnixNano()
	switch len(os.Args) {
	case 1:
	case 2:
		// first arg is a seed if it's a number
		if s, err := strconv.ParseInt(os.Args[1], 10, 64); err == nil {
			seed = s
		} else {
			fmt.Println("Usage: go run main.go [seed]")
			os.Exit(1)
		}
	default:
		fmt.Println("Usage: go run main.go [seed]")
		os.Exit(1)
	}

	simRand := rand.New(rand.NewSource(seed))

	// TODO: generate static population up-front with fixed characteristics
	pop := &model.Population{}
	pop.Add(GRAPHSYNC_FAST_RELIABLE_LOTS_OF_POPULAR_DATA, 4)
	pop.Add(GRAPHSYNC_MEDIUM_RELIABLE_SOME_POPULAR_DATA, 20)
	pop.Add(GRAPHSYNC_MEDIUM_UNRELIABLE_SOME_POPULAR_DATA, 20)
	pop.Add(GRAPHSYNC_MEDIUM_RELIABLE_MINIMAL_POPULAR_DATA, 50)
	pop.Add(HTTP_FAST_SEMIRELIABLE_LOTS_OF_POPULAR_DATA, 1)

	sim := model.Simulation{
		Population:      pop,
		Retrievals:      50000,
		RetrievalSize:   model.ProbDist{StdDev: 2e7, Mean: 1e7}, // Mean of 20MB +/- 10MB
		HttpChance:      model.Chance(0.5),
		GraphsyncChance: model.Chance(0.5),
	}

	ret := sim.Run(simRand)
	cfg := session.DefaultConfig()
	cfg.Random = simRand
	ses := session.NewSession(cfg, true)
	res := ret.RunWith(simRand, ses)

	fmt.Println("---------------------------------------------------------------")
	fmt.Println("Simulation of of", len(ret), "retrievals, seed:", seed)
	fmt.Println()
	fmt.Printf("\t     Size per retrieval: %s < %s < %s\n", humanize.IBytes(uint64(ret.MinSize())), humanize.IBytes(uint64(ret.AvgSize())), humanize.IBytes(uint64(ret.MaxSize())))
	fmt.Printf("\tCandidate per retrieval: %s < %s < %s\n", humanize.Comma(int64(ret.MinCandidateCount())), humanize.Comma(int64(ret.AvgCandidateCount())), humanize.Comma(int64(ret.MaxCandidateCount())))
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("\t                   Runs: %d\n", res.Runs)
	fmt.Printf("\t              Successes: %d\n", res.Successes)
	fmt.Printf("\t     Retrieval failures: %d\n", res.RetrievalFailures)
	fmt.Printf("\t                   Size: %s\n", humanize.IBytes(uint64(res.Size)))
	fmt.Printf("\t             Total time: %v\n", time.Duration(res.TotalTimeMs)*time.Millisecond)
	fmt.Printf("\t           Average TTFB: %s\n", time.Duration(res.AverageTimeToFirstByteMs)*time.Millisecond)
	fmt.Printf("\t      Average bandwidth: %s/s\n", humanize.IBytes(uint64(res.AverageBandwidth)))
	fmt.Printf("\t        Total bandwidth: %s/s\n", humanize.IBytes(uint64(res.Size)/uint64(res.TotalTimeMs/1000)))
	fmt.Println("---------------------------------------------------------------")
}
