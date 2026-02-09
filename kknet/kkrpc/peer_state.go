package kkrpc

import (
	"sync"
	"sync/atomic"
)

type peerState struct {
	mu      sync.Mutex
	pending map[uint64]chan Frame // reqId -> chan Frame
	closed  atomic.Bool
}

func newPeerState() *peerState {
	return &peerState{
		pending: make(map[uint64]chan Frame),
	}
}

func (s *peerState) addPending(id uint64) (chan Frame, bool) {
	ch := make(chan Frame, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, false
	}
	s.pending[id] = ch
	return ch, true
}

func (s *peerState) delPending(id uint64) {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
}

func (s *peerState) deliver(fr Frame) {
	s.mu.Lock()
	ch := s.pending[fr.ID]
	s.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- fr:
	default:
	}
}

func (s *peerState) closeAll() {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return
	}
	s.closed.Store(true)
	for id, ch := range s.pending {
		delete(s.pending, id)
		close(ch)
	}
	s.mu.Unlock()
}
