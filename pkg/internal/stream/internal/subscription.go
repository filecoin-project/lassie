package internal

import (
	"sync"

	"github.com/filecoin-project/lassie/pkg/types"
	"go.uber.org/multierr"
)

func NewSubscription(initialTearDown types.GracefulCanceller) *Subscription {
	return &Subscription{initialTeardown: initialTearDown}
}

type Subscription struct {
	lock            sync.Mutex
	closed          bool
	parents         []*Subscription
	initialTeardown types.GracefulCanceller
	children        []types.GracefulCanceller
}

func (s *Subscription) TearDown() error {
	s.lock.Lock()
	if s.closed {
		s.lock.Unlock()
		return nil
	}
	s.closed = true
	initialTeardown := s.initialTeardown
	parents := append([]*Subscription(nil), s.parents...)
	children := append([]types.GracefulCanceller(nil), s.children...)
	s.lock.Unlock()

	for _, parent := range parents {
		parent.Remove(s)
	}
	var err error
	if initialTeardown != nil {
		err = initialTeardown.TearDown()
	}

	for _, child := range children {
		multierr.Append(err, child.TearDown())
	}
	return err
}

func (s *Subscription) tryAddParent(parent *Subscription) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return false
	}
	// check if parent is already subscribed
	for _, testParent := range s.parents {
		if testParent == parent {
			return false
		}
	}
	s.parents = append(s.parents, parent)
	return true
}

func (s *Subscription) Add(child types.GracefulCanceller) {
	// can't add to self or add a null subscription
	if child == nil || child == s {
		return
	}
	s.lock.Lock()
	if s.closed {
		s.lock.Unlock()
		child.TearDown()
		return
	}
	defer s.lock.Unlock()
	if childSubscription, ok := child.(*Subscription); ok {
		if !childSubscription.tryAddParent(s) {
			return
		}
	}
	s.children = append(s.children, child)
}

func (s *Subscription) removeParent(parent *Subscription) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, testParent := range s.parents {
		if testParent == parent {
			s.parents[i] = s.parents[len(s.children)-1]
			s.parents = s.parents[:len(s.children)-1]
			return
		}
	}
}

func (s *Subscription) Remove(child types.GracefulCanceller) {
	s.lock.Lock()
	for i, testChild := range s.children {
		if testChild == child {
			s.children[i] = s.children[len(s.children)-1]
			s.children = s.children[:len(s.children)-1]
			break
		}
	}
	s.lock.Unlock()

	if childSubscription, ok := child.(*Subscription); ok {
		childSubscription.removeParent(s)
	}
}

func Empty() *Subscription {
	e := NewSubscription(nil)
	e.TearDown()
	return e
}
