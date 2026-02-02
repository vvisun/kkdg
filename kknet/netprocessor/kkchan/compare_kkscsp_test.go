package kkchan

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/netprocessor"
	"github.com/vvisun/kkdg/kknet/netprocessor/kkscsp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// This file provides comparative tests/benchmarks between:
// - kkchan.LockFreeLinkBackPressureChan (MPMC -> single drainer)
// - kkscsp.WriteProcessor (MPMC -> single writer goroutine with wakeCh + batch PopMany)
//
// Note: these are microbenchmarks intended to compare relative overhead under the same workload,
// not a full system benchmark.

func Test_Compare_KKChan_vs_KKSCSPSend_DrainNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compare test in -short mode")
	}

	const (
		producers   = 4
		perProducer = 200_000
		payload     = 64
		queueSize   = 1 << 20
	)

	t.Run("kkchan", func(t *testing.T) {
		bpc := NewLockFreeLinkBackPressureChan[*kkbuffer.ByteBuffer](queueSize, 128)

		var drained atomic.Int64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bb := range bpc.Chan() {
				if bb != nil {
					kkbuffer.Put(bb)
				}
				drained.Add(1)
			}
		}()

		var wp sync.WaitGroup
		wp.Add(producers)
		for p := 0; p < producers; p++ {
			go func() {
				defer wp.Done()
				for i := 0; i < perProducer; i++ {
					bb := kkbuffer.GetWithCapacity(payload)
					bb.B = bb.B[:payload]
					for bpc.Send(bb) == 1 {
						// queue full: yield and retry
						time.Sleep(0)
					}
				}
			}()
		}
		wp.Wait()

		// Close waits internal forwarder; drainer exits on mainChan close.
		bpc.Close()
		wg.Wait()

		want := int64(producers * perProducer)
		got := drained.Load()
		if got != want {
			t.Fatalf("drained=%d want=%d", got, want)
		}
	})

	t.Run("kkscsp_writeprocessor", func(t *testing.T) {
		opts := netprocessor.WriteOptions{
			SendQueueSize:             queueSize,
			SendQueueStrict:           false,
			SendQueueNeedFlushOver:    true,
			SendQueueTimeoutFlushOver: 30 * time.Second,
			WriteBatchSize:            64,
			WriteBatchLimitBytes:      0,
		}
		wp := kkscsp.NewWriteProcessor(opts)

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

		var pwg sync.WaitGroup
		pwg.Add(producers)
		for p := 0; p < producers; p++ {
			go func() {
				defer pwg.Done()
				for i := 0; i < perProducer; i++ {
					bb := kkbuffer.GetWithCapacity(payload)
					bb.B = bb.B[:payload]
					for {
						err := wp.SendBuffer(bb)
						if err == nil {
							break
						}
						if err == kkerrors.ErrSendQueueFull {
							// retry with the same buffer instance (not released)
							time.Sleep(0)
							continue
						}
						// connection closed: should not happen here
						t.Fatalf("SendBuffer err=%v", err)
					}
				}
			}()
		}
		pwg.Wait()

		// Wait for flush/drain and stop.
		wp.Stop(nil)

		want := int64(producers * perProducer)
		got := drained.Load()
		if got != want {
			t.Fatalf("drained=%d want=%d", got, want)
		}
	})
}

func Benchmark_KKChan_SendDrain(b *testing.B) {
	const (
		payload   = 64
		queueSize = 1 << 20
		spinTimes = 128
	)

	b.ReportAllocs()
	bpc := NewLockFreeLinkBackPressureChan[*kkbuffer.ByteBuffer](queueSize, spinTimes)

	var drained atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for bb := range bpc.Chan() {
			if bb != nil {
				kkbuffer.Put(bb)
			}
			drained.Add(1)
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb := kkbuffer.GetWithCapacity(payload)
			bb.B = bb.B[:payload]
			for bpc.Send(bb) == 1 {
				time.Sleep(0)
			}
		}
	})
	b.StopTimer()

	// Stop and wait for drain completion.
	bpc.Close()
	wg.Wait()
	_ = drained.Load()
}

func Benchmark_KKSCSPSendDrain(b *testing.B) {
	const (
		payload   = 64
		queueSize = 1 << 20
	)

	b.ReportAllocs()

	opts := netprocessor.WriteOptions{
		SendQueueSize:             queueSize,
		SendQueueStrict:           false,
		SendQueueNeedFlushOver:    true,
		SendQueueTimeoutFlushOver: 30 * time.Second,
		WriteBatchSize:            64,
		WriteBatchLimitBytes:      0,
	}
	wp := kkscsp.NewWriteProcessor(opts)

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
