package kkrpc

import (
	"sync"
	"sync/atomic"
)

type pendingMap struct {
	closed atomic.Bool
	mu     sync.Mutex
	cbMap  map[uint64]func(Frame)
	chMap  map[uint64]chan Frame
}

func newPendingMap() *pendingMap {
	return &pendingMap{
		cbMap: make(map[uint64]func(Frame)),
		chMap: make(map[uint64]chan Frame),
	}
}

func (p *pendingMap) addCh(reqId uint64) (chan Frame, bool) {
	if p.closed.Load() {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chMap[reqId] = make(chan Frame, 1)
	return p.chMap[reqId], true
}

func (p *pendingMap) delCh(reqId uint64) {
	p.mu.Lock()
	delete(p.chMap, reqId)
	p.mu.Unlock()
}

func (p *pendingMap) deliverCh(reqId uint64, fr Frame) {
	p.mu.Lock()
	ch := p.chMap[reqId]
	delete(p.chMap, reqId)
	p.mu.Unlock()
	if ch == nil {
		return
	}
	ch <- fr
}

func (p *pendingMap) addCallback(reqId uint64, fn func(Frame)) {
	if p.closed.Load() {
		return
	}
	p.mu.Lock()
	p.cbMap[reqId] = fn
	p.mu.Unlock()
}

func (p *pendingMap) delCallback(reqId uint64) {
	p.mu.Lock()
	delete(p.cbMap, reqId)
	p.mu.Unlock()
}

func (p *pendingMap) deliverCallback(reqId uint64, fr Frame) {
	p.mu.Lock()
	fn := p.cbMap[reqId]
	delete(p.cbMap, reqId)
	p.mu.Unlock()
	if fn == nil {
		return
	}
	fn(fr)
}

// deliver 将响应投递给同步等待者（channel）或异步回调，同一 reqId 只会有其一
func (p *pendingMap) deliver(id uint64, fr Frame) {
	if p == nil {
		return
	}
	p.mu.Lock()
	ch := p.chMap[id]
	delete(p.chMap, id)
	fn := p.cbMap[id]
	delete(p.cbMap, id)
	p.mu.Unlock()
	if ch != nil {
		ch <- fr
		return
	}
	if fn != nil {
		fn(fr)
	}
}

func (p *pendingMap) closeAll() {
	if p.closed.Load() {
		return
	}
	p.mu.Lock()
	p.closed.Store(true)
	for id := range p.cbMap {
		delete(p.cbMap, id)
	}
	for id := range p.chMap {
		delete(p.chMap, id)
	}
	p.mu.Unlock()
}
