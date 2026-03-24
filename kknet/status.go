package kknet

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
)

type ConnStatus = int32 //连接状态

const (
	ConnStatusInit         ConnStatus = iota //初始状态
	ConnStatusConnecting                     //连接中
	ConnStatusConnected                      //已连接
	ConnStatusReconnecting                   //重连中
	ConnStatusReconnected                    //已重连
	ConnStatusClosing                        //关闭中
	ConnStatusClosed                         //已关闭
)

func ChangeConnStatus(status *ConnStatus, newStatus ConnStatus) {
	atomic.StoreInt32(status, int32(newStatus))
}

// LoadConnStatus 原子读取当前连接状态
func LoadConnStatus(status *ConnStatus) ConnStatus {
	return atomic.LoadInt32(status)
}

// CASConnStatus 原子比较并交换连接状态，成功返回 true
func CASConnStatus(status *ConnStatus, old, new ConnStatus) bool {
	return atomic.CompareAndSwapInt32(status, old, new)
}

// IsConnected 判断连接状态是否为连接已建立
func IsConnected(status *ConnStatus) bool {
	cur := atomic.LoadInt32(status)
	return cur == ConnStatusConnected || cur == ConnStatusReconnected
}

// IsClosingOrClosed 判断连接是否处于关闭中或已关闭状态
func IsClosingOrClosed(status *ConnStatus) bool {
	cur := atomic.LoadInt32(status)
	return cur >= ConnStatusClosing
}

// SafeHandlerCall runs fn and recovers from panics.
// It logs the panic and increments stats errors when available.
func SafeHandlerCall(logger kklog.ILogger, stats *Stats, label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if stats != nil {
				stats.AddError()
			}
			if logger != nil {
				logger.Errorf("%s handler panic: %v", label, r)
			}
		}
	}()
	fn()
}

// var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// ReconnectBackoff computes a reconnect delay with exponential backoff and jitter.
//
//	delay = base * 2^max(0, consecutiveFails-1), capped at maxInterval.
//	Adds [0, 25%) of delay as jitter to spread out reconnect storms.
func ReconnectBackoff(base, maxInterval time.Duration, consecutiveFails int) time.Duration {
	delay := base
	for i := 1; i < consecutiveFails; i++ {
		delay *= 2
		if delay >= maxInterval {
			delay = maxInterval
			break
		}
	}
	if jitterRange := int64(delay) / 4; jitterRange > 0 {
		delay += time.Duration(rand.Int63n(jitterRange))
	}
	return delay
}
