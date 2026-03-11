package kkprocessor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Benchmark_WorkerWriteProcessorSendDrain 基于 WorkerWriteProcessor 的发送+消费吞吐测试，
// 设计与 Benchmark_KKSCSPSendDrain 尽量对齐，便于对比 write_thread 与 write_worker 实现。
func Benchmark_WorkerWriteProcessorSendDrain(b *testing.B) {
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
		BatchWriteLimitBytes:      2048,
	}
	wp := NewWorkerWriteProcessor(opts)

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
				if err == kkerrors.ErrNetSendQueueFull {
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
