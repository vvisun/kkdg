package gnrpc

import (
	"context"
	"sync"
	"sync/atomic"
)

type peerState struct {
	seq atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan Frame
	closed  bool
}

func newPeerState() *peerState {
	return &peerState{
		pending: make(map[uint64]chan Frame),
	}
}

func (s *peerState) nextID() uint64 {
	id := s.seq.Add(1)
	if id == 0 {
		id = s.seq.Add(1)
	}
	return id
}

func (s *peerState) addPending(id uint64) (chan Frame, bool) {
	ch := make(chan Frame, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
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
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for id, ch := range s.pending {
		delete(s.pending, id)
		close(ch)
	}
	s.mu.Unlock()
}

type peerKey struct{}

func getPeerState(cctx context.Context) (*peerState, bool) {
	if cctx == nil {
		return nil, false
	}
	v := cctx.Value(peerKey{})
	if v == nil {
		return nil, false
	}
	ps, ok := v.(*peerState)
	return ps, ok
}

func withPeerState(ctx context.Context, ps *peerState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, peerKey{}, ps)
}

