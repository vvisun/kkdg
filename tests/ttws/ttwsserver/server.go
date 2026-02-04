package main

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	var srv *kkws.Server

	go func() {
		defer wg.Done()
		addr := "localhost:8080"
		recv := &stressRecvHandler{}
		opts := kknet.ApplyOptions(
			//kknet.WithRawHandler(recv),
			kknet.WithNoneCopyHandler(recv),
			kknet.WithRecvQueueSize(512),
		)
		srv = kkws.NewServer(addr, nil, opts)
		// 默认 path 已是 /ws，如需其他路径可 srv.SetPath("/custom")
		if err := srv.Start(); err != nil {
			kklog.Errorf("Start: %v", err)
			return
		}
		kklog.Infof("Server started on %s", addr)
		select {}
	}()

	//定时打印服务器统计信息
	go func() {
		for {
			if srv == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			time.Sleep(1000 * time.Millisecond)
			stats := srv.Stats()
			kknet.PrintStress(&stats)
		}
	}()

	wg.Wait()
}

// stressRecvHandler counts received messages for stress tests.
type stressRecvHandler struct {
	recvCount atomic.Int64
	sendCount atomic.Int64
}

func (h *stressRecvHandler) OnNoneCopy(connID int64, data []byte) {
	if data == nil {
		return
	}
	h.recvCount.Add(1)
}

func (h *stressRecvHandler) OnRaw(connID int64, data buffers.IBuffer) {
	if data == nil {
		return
	}
	h.recvCount.Add(1)
}
