package model

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipni/go-libipni/metadata"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

const tickIncrement = 5 * time.Millisecond

type Simulation struct {
	Population      *Population
	Retrievals      int
	RetrievalSize   ProbDist
	HttpChance      Chance
	GraphsyncChance Chance
}

func (sim Simulation) Run(rand *rand.Rand) RetrievalSet {
	retrievalSet := make(RetrievalSet, sim.Retrievals)
	for i := 0; i < sim.Retrievals; i++ {
		retrievalSet[i] = sim.Retrieval(rand)
	}
	return retrievalSet
}

func (sim Simulation) Retrieval(rand *rand.Rand) Retrieval {
	var http, gs bool
	for !http && !gs {
		http = sim.HttpChance.Roll(rand)
		gs = sim.GraphsyncChance.Roll(rand)
	}
	candidates := make([]Candidate, 0)
	for len(candidates) == 0 {
		for _, pc := range sim.Population.Providers {
			for i := pc.Count; http && i > 0; i-- {
				if pc.Provider.Probabilities[multicodec.TransportIpfsGatewayHttp].Candidate.Roll(rand) {
					id := peer.ID(fmt.Sprintf("cand%d", len(candidates)))
					candidates = append(candidates, Candidate{id, pc.Provider, multicodec.TransportIpfsGatewayHttp})
				}
			}
			for i := pc.Count; gs && i > 0; i-- {
				if pc.Provider.Probabilities[multicodec.TransportGraphsyncFilecoinv1].Candidate.Roll(rand) {
					id := peer.ID(fmt.Sprintf("cand%d", len(candidates)))
					candidates = append(candidates, Candidate{id, pc.Provider, multicodec.TransportGraphsyncFilecoinv1})
				}
			}
		}
	}
	size := int64(sim.RetrievalSize.Sample(rand))
	// minimum block size of 100KiB
	if size < 102400 {
		size = 102400
	}
	return Retrieval{
		Candidates: candidates,
		Size:       size,
	}
}

type Retrieval struct {
	Candidates []Candidate
	Size       int64
}

type Candidate struct {
	ID       peer.ID
	Provider Provider
	Protocol multicodec.Code
}

type candidateRun struct {
	Candidate         Candidate
	ConnectTimeMs     int64
	TimeToFirstByteMs int64
	DurationMs        int64
	Connected         bool
	Success           bool
	Metadata          metadata.Protocol
}

type protocolRun struct {
	Size           int64
	CandidateRuns  []*candidateRun
	WaitingIndexes []int
	RunningIndex   int
	RunningFb      bool
	RunningFromMs  int64
	FailureCount   int
}

func (pr protocolRun) Done() bool {
	return pr.FailureCount >= len(pr.CandidateRuns)
}

// An artificial retriever simulation that is quantised to `tick`, so it's not
// quite accurate but close enough to simulate a real retrieval.
func (pr *protocolRun) Tick(ses *session.Session, tick time.Duration) (bool, int64, int64) {
	// check for candidates needing to connect in this window
	for i, cr := range pr.CandidateRuns {
		if !cr.Connected && cr.ConnectTimeMs >= 0 && cr.ConnectTimeMs <= tick.Milliseconds() {
			cr.Connected = true
			pr.WaitingIndexes = append(pr.WaitingIndexes, i)
			ses.RecordConnectTime(cr.Candidate.ID, time.Millisecond*time.Duration(cr.ConnectTimeMs))
		}
	}
	// see if the current running candidate is done
	if pr.RunningIndex != -1 {
		cr := pr.CandidateRuns[pr.RunningIndex]
		if !pr.RunningFb && pr.RunningFromMs+cr.TimeToFirstByteMs <= tick.Milliseconds() {
			pr.RunningFb = true
			ses.RecordFirstByteTime(cr.Candidate.ID, time.Millisecond*time.Duration(cr.TimeToFirstByteMs))
		}
		if pr.RunningFromMs+cr.DurationMs <= tick.Milliseconds() {
			pr.RunningIndex = -1
			pr.RunningFb = false
			if cr.Success {
				bandwidth := float64(pr.Size) / (float64(cr.DurationMs) / 1000)
				ses.RecordSuccess(cr.Candidate.ID, uint64(bandwidth))
				return true, cr.TimeToFirstByteMs, pr.RunningFromMs + cr.DurationMs
			} else {
				pr.FailureCount++
				ses.RecordFailure(types.RetrievalID{}, cr.Candidate.ID)
			}
		}
	}
	// nobody running, pick next and start a retrieval
	if pr.RunningIndex == -1 && len(pr.WaitingIndexes) > 0 {
		var next int
		if len(pr.WaitingIndexes) > 1 {
			peers := make([]peer.ID, len(pr.WaitingIndexes))
			mda := make([]metadata.Protocol, len(pr.WaitingIndexes))
			for i, idx := range pr.WaitingIndexes {
				peers[i] = pr.CandidateRuns[idx].Candidate.ID
				mda[i] = pr.CandidateRuns[idx].Metadata
			}
			next = ses.ChooseNextProvider(peers, mda)
		}
		pr.RunningIndex = pr.WaitingIndexes[next]
		pr.RunningFromMs = tick.Milliseconds()
		pr.WaitingIndexes = append(pr.WaitingIndexes[:next], pr.WaitingIndexes[next+1:]...)
	}
	return false, 0, pr.RunningFromMs + tick.Milliseconds()
}

var initialWait = (2 * time.Millisecond)

// RunWith models a retrieval across multiple candidates for both protocols.
// It sets up the initial state and then ticks the simulation until it's done.
// Running the simulation in ticks makes it simpler but quantises the run,
// losing resolution that a real execution would have.
func (rs Retrieval) RunWith(rand *rand.Rand, ses *session.Session) RetrievalResult {
	// setup separate runs for GS & HTTP
	runs := map[multicodec.Code]*protocolRun{
		multicodec.TransportGraphsyncFilecoinv1: {
			Size:          rs.Size,
			CandidateRuns: make([]*candidateRun, 0),
			RunningIndex:  -1,
		},
		multicodec.TransportIpfsGatewayHttp: {
			Size:          rs.Size,
			CandidateRuns: make([]*candidateRun, 0),
			RunningIndex:  -1,
		},
	}
	gsRun := runs[multicodec.TransportGraphsyncFilecoinv1]
	httpRun := runs[multicodec.TransportIpfsGatewayHttp]

	for _, c := range rs.Candidates {
		cr := &candidateRun{Candidate: c}
		runs[c.Protocol].CandidateRuns = append(runs[c.Protocol].CandidateRuns, cr)
		switch c.Protocol {
		case multicodec.TransportGraphsyncFilecoinv1:
			cr.Metadata = &metadata.GraphsyncFilecoinV1{
				VerifiedDeal:  c.Provider.Probabilities[multicodec.TransportGraphsyncFilecoinv1].Verified.Roll(rand),
				FastRetrieval: c.Provider.Probabilities[multicodec.TransportGraphsyncFilecoinv1].FastRetrieval.Roll(rand),
			}
		case multicodec.TransportIpfsGatewayHttp:
			cr.Metadata = &metadata.IpfsGatewayHttp{}
		}
		cr.Success = c.Provider.Probabilities[c.Protocol].Success.Roll(rand)
		if !cr.Success && FIFTY_FIFTY.Roll(rand) {
			// If a failure, is it fail on connect or retrieve?
			// Should this be a probability on the provider since it impacts connect time metric recording?
			cr.ConnectTimeMs = -1
			runs[c.Protocol].FailureCount++
			ses.RecordFailure(types.RetrievalID{}, c.ID) // ignore err, not relevant
		} else { // actually manages to connect
			cr.ConnectTimeMs = int64(c.Provider.Probabilities[c.Protocol].ConnectTimeMs.Sample(rand))
			// figure out a reasonable(ish) bandwidth and latency, and therefore the
			// real bandwidth of a potential successful transfer
			bandwidth := c.Provider.Probabilities[c.Protocol].BandwidthBps.Sample(rand)
			// multiply by 4 to simulate a transfer init
			latency := 2 * c.Provider.Probabilities[c.Protocol].LatencyMs.Sample(rand)
			cr.DurationMs = int64(latency + ((float64(rs.Size) / bandwidth) * 1000))
			if !cr.Success {
				// failure at some random point in the transfer, so not full duration
				cr.DurationMs = int64(rand.Float64() * float64(cr.DurationMs))
			} else {
				cr.TimeToFirstByteMs = int64(latency + c.Provider.Probabilities[c.Protocol].TimeToFirstByteMs.Sample(rand))
			}
		}
	}

	tick := initialWait
	for {
		var gsSuccess, httpSuccess bool
		var gsTtfbMs, httpTtfbMs, gsDurationMs, httpDurationMs int64
		if !gsRun.Done() {
			gsSuccess, gsTtfbMs, gsDurationMs = gsRun.Tick(ses, tick)
		}
		if !httpRun.Done() {
			httpSuccess, httpTtfbMs, httpDurationMs = httpRun.Tick(ses, tick)
		}
		if gsSuccess || httpSuccess {
			if tick == initialWait {
				panic("unexpected success during initialWait")
			}
			durationMs := gsDurationMs
			ttfbMs := gsTtfbMs
			if httpSuccess && (!gsSuccess || httpDurationMs < durationMs) {
				durationMs = httpDurationMs
			}
			// ttfb could be from the opposite protocol to the successful one
			if httpSuccess && (!gsSuccess || httpTtfbMs < ttfbMs) {
				ttfbMs = httpTtfbMs
			}
			return RetrievalResult{
				RunTimeMs:         durationMs,
				TimeToFirstByteMs: ttfbMs,
				Success:           true,
				Failures:          gsRun.FailureCount + httpRun.FailureCount,
				BandwidthBps:      int64(float64(rs.Size) / (float64(durationMs) / 1000)),
			}
		}
		if gsDurationMs == 0 && httpDurationMs == 0 || tick > 30*time.Second {
			if tick > 2*time.Minute {
				fmt.Print("tick > 2m, bailing out, got: ")
				for _, cr := range gsRun.CandidateRuns {
					fmt.Print(cr.ConnectTimeMs, "ms+", time.Duration(cr.DurationMs)*time.Millisecond, " ")
				}
				fmt.Println()
			}
			return RetrievalResult{
				RunTimeMs:    tick.Milliseconds(),
				Success:      false,
				Failures:     gsRun.FailureCount + httpRun.FailureCount,
				BandwidthBps: 0,
			}
		}
		tick += tickIncrement
	}
}

type RetrievalSet []Retrieval

func (rs RetrievalSet) AvgSize() int64 {
	var total int64 = 0
	for _, r := range rs {
		total += r.Size
	}
	return total / int64(len(rs))
}

func (rs RetrievalSet) MaxSize() int64 {
	var max int64 = 0
	for _, r := range rs {
		if r.Size > max {
			max = r.Size
		}
	}
	return max
}

func (rs RetrievalSet) MinSize() int64 {
	var min int64 = math.MaxInt64
	for _, r := range rs {
		if r.Size < min {
			min = r.Size
		}
	}
	return min
}

func (rs RetrievalSet) AvgCandidateCount() int {
	total := 0
	for _, r := range rs {
		total += len(r.Candidates)
	}
	return total / len(rs)
}

func (rs RetrievalSet) MaxCandidateCount() int {
	max := 0
	for _, r := range rs {
		if len(r.Candidates) > max {
			max = len(r.Candidates)
		}
	}
	return max
}

func (rs RetrievalSet) MinCandidateCount() int {
	min := math.MaxInt
	for _, r := range rs {
		if len(r.Candidates) < min {
			min = len(r.Candidates)
		}
	}
	return min
}

// RunWith iterates through the retrievals and executes them, collecting and
// returning summary data.
func (rs RetrievalSet) RunWith(rand *rand.Rand, ses *session.Session) Result {
	results := make([]RetrievalResult, len(rs))
	var successes, retrievalFailures int
	var size, totalTimeMs, totalBandwidthBps, totalTtfbMs int64

	for i, r := range rs {
		results[i] = r.RunWith(rand, ses)
		size += int64(r.Size)
		retrievalFailures += results[i].Failures
		totalTimeMs += results[i].RunTimeMs
		if results[i].Success {
			successes++
			totalBandwidthBps += results[i].BandwidthBps
			totalTtfbMs += results[i].TimeToFirstByteMs
		}
	}
	return Result{
		Runs:                     len(rs),
		Successes:                successes,
		RetrievalFailures:        retrievalFailures,
		Size:                     size,
		TotalTimeMs:              totalTimeMs,
		AverageTimeToFirstByteMs: totalTtfbMs / int64(successes),
		AverageBandwidth:         totalBandwidthBps / int64(successes),
	}
}

type RetrievalResult struct {
	RunTimeMs         int64
	TimeToFirstByteMs int64
	Success           bool
	Failures          int
	BandwidthBps      int64
}

type Result struct {
	Runs                     int
	Successes                int
	RetrievalFailures        int // total individual failures, failures across all retrievals
	Size                     int64
	TotalTimeMs              int64
	AverageTimeToFirstByteMs int64
	AverageBandwidth         int64
}
