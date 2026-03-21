package main

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// tttcpserver: simple TCP stress server used by tttcpclient.
// Run:
//   go run ./tests/tttcp/tttcpserver
//
// Default listen address: 127.0.0.1:19090

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	var srv *kktcp.Server

	go func() {
		defer wg.Done()
		addr := "127.0.0.1:19090"
		recv := &stressRecvHandler{}
		opts := kknet.ApplyOptions(
			kknet.WithNoneCopyHandler(recv),
			kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
			kknet.WithRecvQueueSize(512),
			kknet.WithLogger(kklog.GetConsoleLogger()),
		)
		srv = kktcp.NewServer(addr, nil, opts)
		if err := srv.Start(); err != nil {
			kklog.Errorf("kktcp server start: %v", err)
			return
		}
		kklog.Infof("kktcp server started on %s", addr)
		select {}
	}()

	// periodically print server stats
	go func() {
		for {
			if srv == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			time.Sleep(1 * time.Second)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	wg.Wait()
}

// stressRecvHandler counts received messages for stress tests.
type stressRecvHandler struct {
	recvCount atomic.Int64
}

func (h *stressRecvHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {
	if data == nil {
		return
	}
	h.recvCount.Add(1)
}

func (h *stressRecvHandler) OnRaw(connID int64, data *kkbuffer.ByteBuffer) {
	if data == nil {
		return
	}
	h.recvCount.Add(1)
	kkbuffer.Put(data)
}
