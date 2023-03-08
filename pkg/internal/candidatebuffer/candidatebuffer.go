package candidatebuffer

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/lassie/pkg/types"
)

type CandidateBuffer struct {
	clock             clock.Clock
	timerCancel       context.CancelFunc
	currentCandidates []types.RetrievalCandidate
	lk                sync.Mutex
	onCandidates      func([]types.RetrievalCandidate)
	afterEach         chan<- struct{}
}

func NewCandidateBuffer(onCandidates func([]types.RetrievalCandidate), clock clock.Clock) *CandidateBuffer {
	return NewCandidateBufferWithSync(onCandidates, clock, nil)
}

func NewCandidateBufferWithSync(onCandidates func([]types.RetrievalCandidate), clock clock.Clock, afterEach chan<- struct{}) *CandidateBuffer {
	return &CandidateBuffer{
		onCandidates: onCandidates,
		clock:        clock,
		afterEach:    afterEach,
	}
}

func (c *CandidateBuffer) clear() []types.RetrievalCandidate {
	c.lk.Lock()
	if c.timerCancel != nil {
		c.timerCancel()
	}
	c.timerCancel = nil
	prevCandidates := c.currentCandidates
	c.currentCandidates = nil
	c.lk.Unlock()
	return prevCandidates
}

func (c *CandidateBuffer) emit() {
	prevCandidates := c.clear()
	if len(prevCandidates) > 0 {
		c.onCandidates(prevCandidates)
	}
}

// BufferStream consumes a stream of individual candidate results. When a new result comes in, a collection is started, and further results
// are added to the collection until the specified bufferingTime has passed, at which point the collection is passed to the callback setup
// when the Buffer was setup. The timer is reset and the collection emptied until another result comes in. This has the effect of grouping
// results that occur in the same general time frame.
func (c *CandidateBuffer) BufferStream(ctx context.Context, incoming <-chan types.FindCandidatesResult, bufferingTime time.Duration) error {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()
	for {
		select {
		case <-ctx.Done():
			c.clear()
			return ctx.Err()
		case candidateResult, ok := <-incoming:
			if !ok {
				c.emit()
				return nil
			}
			if candidateResult.Err != nil {
				c.emit()
				return candidateResult.Err
			}
			var timerCtx context.Context
			c.lk.Lock()

			c.currentCandidates = append(c.currentCandidates, candidateResult.Candidate)
			if c.timerCancel != nil {
				c.lk.Unlock()
				if c.afterEach != nil {
					c.afterEach <- struct{}{}
				}
				continue
			}
			timerCtx, c.timerCancel = context.WithCancel(ctx)
			c.lk.Unlock()
			timer := c.clock.Timer(bufferingTime)
			if c.afterEach != nil {
				c.afterEach <- struct{}{}
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-timerCtx.Done():
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					c.emit()
				}
			}()
		}
	}
}
