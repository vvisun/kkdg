package kkrpc

import (
	"sync"
	"sync/atomic"
)

type pendingMap struct {
	closed  atomic.Bool
	mu      sync.Mutex
	pending map[uint64]func(Frame)
	chMap   map[uint64]chan Frame
}

func newPendingMap() *pendingMap {
	return &pendingMap{
		pending: make(map[uint64]func(Frame)),
		chMap:   make(map[uint64]chan Frame),
	}
}

func (p *pendingMap) addCh(id uint64) (chan Frame, bool) {
	if p.closed.Load() {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chMap[id] = make(chan Frame, 1)
	return p.chMap[id], true
}

func (p *pendingMap) delCh(id uint64) {
	p.mu.Lock()
	delete(p.chMap, id)
	p.mu.Unlock()
}

func (p *pendingMap) deliverCh(id uint64, fr Frame) {
	p.mu.Lock()
	ch := p.chMap[id]
	delete(p.chMap, id)
	p.mu.Unlock()
	if ch == nil {
		return
	}
	ch <- fr
}

func (p *pendingMap) addCallback(id uint64, fn func(Frame)) {
	if p.closed.Load() {
		return
	}
	p.mu.Lock()
	p.pending[id] = fn
	p.mu.Unlock()
}

func (p *pendingMap) delCallback(id uint64) {
	p.mu.Lock()
	delete(p.pending, id)
	p.mu.Unlock()
}

func (p *pendingMap) deliverCallback(id uint64, fr Frame) {
	p.mu.Lock()
	fn := p.pending[fr.ID]
	delete(p.pending, fr.ID)
	p.mu.Unlock()
	if fn == nil {
		return
	}
	fn(fr)
}

func (p *pendingMap) closeAll() {
	if p.closed.Load() {
		return
	}
	p.mu.Lock()
	p.closed.Store(true)
	for id := range p.pending {
		delete(p.pending, id)
	}
	for id := range p.chMap {
		delete(p.chMap, id)
	}
	p.mu.Unlock()
}
