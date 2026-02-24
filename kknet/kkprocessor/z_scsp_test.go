package kkprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func Test_Compare_KKChan_vs_KKSCSPSend_DrainNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compare test in -short mode")
	}

	const (
		producers   = 4
		perProducer = 200_000
		payload     = 64
		queueSize   = 4096
	)

	t.Run("kkscsp_writeprocessor", func(t *testing.T) {
		opts := kknet.WriteOptions{
			SendQueueSize:             queueSize,
			SendQueueStrict:           false,
			SendQueueNeedFlushOver:    true,
			SendQueueTimeoutFlushOver: 30 * time.Second,
			BatchWriteSize:            64,
			BatchWriteLimitBytes:      0,
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
