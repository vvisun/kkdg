package kkprocessor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func Benchmark_KKSCSPSendDrain(b *testing.B) {
	const (
		payload   = 64
		queueSize = 4096 * 2
	)

	b.ReportAllocs()

	opts := kknet.WriteOptions{
		SendQueueSize:             queueSize,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 30 * time.Second,
		BatchWriteSize:            64,
		BatchWriteLimitBytes:      2048,
	}
	wp := NewWriteProcessor(opts)

	var drained atomic.Int64
	writeFn := func(batch []*kkbuffer.ByteBuffer, n int) error {
		for i := 0; i < n; i++ {
			bb := batch[i]
			batch[i] = nil
			if bb != nil {
				kkbuffer.Put(bb)
			}
		}
		drained.Add(int64(n))
		return nil
	}
	wp.Start(nil, writeFn, func(_ error) {})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb := kkbuffer.GetWithCapacity(payload)
			bb.B = bb.B[:payload]
			for {
				err := wp.SendBuffer(bb)
				if err == nil {
					break
				}
				if err == kkerrors.ErrSendQueueFull {
					time.Sleep(0)
					continue
				}
				b.Fatalf("SendBuffer err=%v", err)
			}
		}
	})
	b.StopTimer()

	wp.Stop(nil)
	_ = drained.Load()
}
