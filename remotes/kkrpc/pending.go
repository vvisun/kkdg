package kkrpc

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
)

const pendingShardCount = 64

// pendingShard 单个分片的 ch/cb 映射，降低锁竞争
type pendingShard struct {
	mu    sync.Mutex
	chMap map[uint64]chan Frame
	cbMap map[uint64]func(Frame)
}

func newPendingShard() *pendingShard {
	return &pendingShard{
		chMap: make(map[uint64]chan Frame),
		cbMap: make(map[uint64]func(Frame)),
	}
}

var chanFramePool = sync.Pool{
	New: func() any { return make(chan Frame, 1) },
}

// pendingMap 用于管理请求的响应和回调
type pendingMap struct {
	maxPendingCount int64
	curPendingCount int64
	stats           *RpcStats
	closed          atomic.Bool
	shards          [pendingShardCount]*pendingShard
}

func newPendingMap(maxPendingCount int) *pendingMap {
	p := &pendingMap{
		maxPendingCount: int64(maxPendingCount),
	}
	for i := 0; i < pendingShardCount; i++ {
		p.shards[i] = newPendingShard()
	}
	return p
}

func (p *pendingMap) shard(id uint64) *pendingShard {
	return p.shards[id%pendingShardCount]
}

func (p *pendingMap) IsClosed() bool {
	return p.closed.Load()
}

func (p *pendingMap) addCh(reqId uint64) (chan Frame, error) {
	if p.closed.Load() {
		return nil, kkerrors.ErrRpcClosed
	}
	if atomic.LoadInt64(&p.curPendingCount) >= p.maxPendingCount {
		return nil, kkerrors.ErrRpcQueueFull
	}
	atomic.AddInt64(&p.curPendingCount, 1)
	s := p.shard(reqId)
	s.mu.Lock()
	if p.closed.Load() {
		s.mu.Unlock()
		atomic.AddInt64(&p.curPendingCount, -1)
		return nil, kkerrors.ErrRpcClosed
	}
	ch := chanFramePool.Get().(chan Frame)
	s.chMap[reqId] = ch
	s.mu.Unlock()
	return ch, nil
}

// delCh 移除 reqId 对应的 channel。若找到则排空后放回池并返回该 channel；若已由 deliver 移除则返回 nil。
func (p *pendingMap) delCh(reqId uint64) (removed chan Frame) {
	s := p.shard(reqId)
	s.mu.Lock()
	ch := s.chMap[reqId]
	delete(s.chMap, reqId)
	s.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		default:
		}
		chanFramePool.Put(ch)
		atomic.AddInt64(&p.curPendingCount, -1)
	}
	return ch
}

func (p *pendingMap) putChBack(ch chan Frame) {
	if ch != nil {
		chanFramePool.Put(ch)
	}
}

func (p *pendingMap) addCallback(reqId uint64, fn func(Frame)) error {
	if p.closed.Load() {
		return kkerrors.ErrRpcClosed
	}
	if atomic.LoadInt64(&p.curPendingCount) >= p.maxPendingCount {
		return kkerrors.ErrRpcQueueFull
	}
	atomic.AddInt64(&p.curPendingCount, 1)
	s := p.shard(reqId)
	s.mu.Lock()
	if p.closed.Load() {
		s.mu.Unlock()
		atomic.AddInt64(&p.curPendingCount, -1)
		return kkerrors.ErrRpcClosed
	}
	s.cbMap[reqId] = fn
	s.mu.Unlock()
	return nil
}

func (p *pendingMap) delCallback(reqId uint64) {
	s := p.shard(reqId)
	s.mu.Lock()
	_, ok := s.cbMap[reqId]
	delete(s.cbMap, reqId)
	s.mu.Unlock()
	if ok {
		atomic.AddInt64(&p.curPendingCount, -1)
	}
}

// takeCallback 移除并返回指定 reqId 的 callback，用于超时等场景下保证只回调一次。
func (p *pendingMap) takeCallback(reqId uint64) (fn func(Frame), ok bool) {
	s := p.shard(reqId)
	s.mu.Lock()
	defer s.mu.Unlock()
	fn, ok = s.cbMap[reqId]
	delete(s.cbMap, reqId)
	return fn, ok && fn != nil
}

func (p *pendingMap) closeAll() {
	if p.closed.Swap(true) {
		return
	}
	// 收集所有 callback，在锁外调用，避免 callback 内阻塞导致死锁
	type cbItem struct {
		reqId uint64
		fn    func(Frame)
	}
	var callbacks []cbItem
	for _, s := range p.shards {
		s.mu.Lock()
		for _, ch := range s.chMap {
			close(ch)
		}
		s.chMap = make(map[uint64]chan Frame)
		for reqId, fn := range s.cbMap {
			if fn != nil {
				callbacks = append(callbacks, cbItem{reqId: reqId, fn: fn})
			}
		}
		s.cbMap = make(map[uint64]func(Frame))
		s.mu.Unlock()
	}
	atomic.StoreInt64(&p.curPendingCount, 0)

	// 在锁外调用 callback，通知用户连接已关闭；callback 会 close(doneCh) 唤醒 InvokeAsync 的 goroutine
	closedFrame := Frame{T: FrameTypeResponse, Code: ErrorCodeConnClosed, Err: "connection closed"}
	for _, item := range callbacks {
		closedFrame.ID = item.reqId
		item.fn(closedFrame)
	}
}

// deliver 将响应投递给同步等待者（channel）或异步回调，同一 reqId 只会有其一
func (p *pendingMap) deliver(id uint64, fr Frame) {
	if p == nil {
		return
	}
	s := p.shard(id)
	s.mu.Lock()
	ch := s.chMap[id]
	delete(s.chMap, id)
	fn := s.cbMap[id]
	delete(s.cbMap, id)
	s.mu.Unlock()
	if ch != nil || fn != nil {
		atomic.AddInt64(&p.curPendingCount, -1)
	}
	if ch != nil {
		ch <- fr
		return
	}
	if fn != nil {
		fn(fr)
	}
}
