package kkcluster

import "sync/atomic"

var gRequestIDSeq uint64 = 0

const maxUint64 = ^uint64(0)

func GenRequestID() uint64 {
	if atomic.LoadUint64(&gRequestIDSeq) >= maxUint64 {
		atomic.StoreUint64(&gRequestIDSeq, 1)
	}
	return atomic.AddUint64(&gRequestIDSeq, 1)
}
