package kkcluster

import "sync/atomic"

var gRequestIDSeq uint64 = 0

const maxUint64 = ^uint64(0)

func GenRequestID() uint64 {
	// 如果超过uint64最大值，则重置为0。理论上不可能，但以防万一。
	// 基本上达到uint64最大值，即使每秒1000万个连接，也需要几十年，
	// 这时候1~几亿的requestId基本上必然已经断开逻辑也已经清理了，不存在逻辑向死亡的requestId发消息的情况。
	if atomic.LoadUint64(&gRequestIDSeq) >= maxUint64 {
		atomic.StoreUint64(&gRequestIDSeq, 0)
	}
	return atomic.AddUint64(&gRequestIDSeq, 1)
}
