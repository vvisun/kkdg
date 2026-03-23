package ccgate

import (
	"sync"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

type FeedLimit struct {
	mu        sync.RWMutex
	cache     map[uint64]int64 // key(connId,errCode) -> deadline(unixNano)
	limitTime int64            // 限制时间(秒)
}

func NewFeedLimit(limitMS time.Duration) *FeedLimit {
	return &FeedLimit{
		cache:     make(map[uint64]int64),
		limitTime: int64(limitMS.Milliseconds()),
	}
}

func (f *FeedLimit) IsLimited(connId kknet.CONN_ID, errCode GateErrorCode) bool {
	now := time.Now().UnixMilli()
	key := (uint64(connId) << 8) | uint64(errCode&0xFF)

	f.mu.RLock()
	deadline, ok := f.cache[key]
	f.mu.RUnlock()
	if ok && now < deadline {
		return true // 同一个连接同一个错误码，短时间内只通知一次。
	}

	f.mu.Lock()
	f.cache[key] = now + f.limitTime
	f.mu.Unlock()
	return false
}

func (f *FeedLimit) Reset(connId kknet.CONN_ID, errCode GateErrorCode) {
	now := time.Now().UnixMilli()
	key := (uint64(connId) << 8) | uint64(errCode&0xFF)

	f.mu.Lock()
	f.cache[key] = now + f.limitTime
	f.mu.Unlock()
}

func (f *FeedLimit) Remove(connId kknet.CONN_ID) {
	f.mu.Lock()
	for i := err_code_min; i < err_code_count; i++ {
		key := (uint64(connId) << 8) | uint64(i&0xFF)
		delete(f.cache, key)
	}
	f.mu.Unlock()
}
