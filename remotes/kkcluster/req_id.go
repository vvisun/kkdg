package kkcluster

import "sync/atomic"

var gRequestIDSeq uint64 = 0

func GenRequestID() uint64 {
	return atomic.AddUint64(&gRequestIDSeq, 1)
}
