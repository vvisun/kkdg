package kkrpc

import (
	"sync"
	"sync/atomic"
)

// pendingMap 用于管理请求的响应和回调
type pendingMap struct {
	closed atomic.Bool
	mu     sync.Mutex
	cbMap  map[uint64]func(Frame) //异步回调
	chMap  map[uint64]chan Frame  //同步等待
}

func newPendingMap() *pendingMap {
	return &pendingMap{
		cbMap: make(map[uint64]func(Frame)),
		chMap: make(map[uint64]chan Frame),
	}
}

func (p *pendingMap) IsClosed() bool {
	return p.closed.Load()
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

// takeCallback 移除并返回指定 reqId 的 callback，用于超时等场景下保证只回调一次。
func (p *pendingMap) takeCallback(reqId uint64) (fn func(Frame), ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn, ok = p.cbMap[reqId]
	delete(p.cbMap, reqId)
	return fn, ok && fn != nil
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
	for id, ch := range p.chMap {
		close(ch)
		delete(p.chMap, id)
	}
	p.mu.Unlock()
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
