package model

import "math/rand"

// Probabilities defines the probabilistic behaviour of a provider for a
// particular protocol
type Probabilities struct {
	// Probability of being a candidate for any given retrieval [0,1]
	Candidate Chance
	// Probability of a successful retrieval [0,1]
	Success Chance
	// Distribution for connect time in milliseconds
	ConnectTimeMs ProbDist
	// Distribution for time to first byte in milliseconds
	TimeToFirstByteMs ProbDist
	// Distribution in bandwidth in bytes per second, this has to account for
	// block fetching speed on the remote, not just the pipe
	BandwidthBps ProbDist
	// Distribution for latency in milliseconds, this will be multiplied to
	// simulate connection initialisation round-trips
	LatencyMs ProbDist
	// Probability of having FastRetrieval for a graphsync retrieval [0,1]
	FastRetrieval Chance
	// Probability of having Verified for a graphsync retrieval [0,1]
	Verified Chance
}

type ProbDist struct {
	StdDev float64
	Mean   float64
}

func (pd ProbDist) Sample(rand *rand.Rand) float64 {
	return rand.NormFloat64()*pd.StdDev + pd.Mean
}

// Chance is the probability of a Roll() being true, the higher the value in the
// range [0,1] the more likely it is to be true.
type Chance float64

func (c Chance) Roll(rand *rand.Rand) bool {
	return rand.Float64() < float64(c)
}

const FIFTY_FIFTY = Chance(0.5)
