package ccgate

import (
	"sync"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

type FeedLimit struct {
	mu    sync.RWMutex
	cache map[uint64]int64 // key(connId,errCode) -> deadline(unixNano)
}

func NewFeedLimit() *FeedLimit {
	return &FeedLimit{
		cache: make(map[uint64]int64),
	}
}

func (f *FeedLimit) IsLimited(connId kknet.CONN_ID, errCode GateErrorCode) bool {
	now := time.Now().UnixNano()
	key := (uint64(connId) << 8) | uint64(errCode&0xFF)

	f.mu.RLock()
	deadline, ok := f.cache[key]
	f.mu.RUnlock()
	if ok && now < deadline {
		return true // 同一个连接同一个错误码，短时间内只通知一次。
	}

	f.mu.Lock()
	f.cache[key] = now + int64(time.Second)
	f.mu.Unlock()
	return false
}

func (f *FeedLimit) Reset(connId kknet.CONN_ID, errCode GateErrorCode) {
	now := time.Now().UnixNano()
	key := (uint64(connId) << 8) | uint64(errCode&0xFF)

	f.mu.Lock()
	f.cache[key] = now + int64(time.Second)
	f.mu.Unlock()
}
