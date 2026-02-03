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
		// 监听地址用 host:port，不要用 ws://...（那是客户端 URL）
		addr := "localhost:8080"
		recv := &stressRecvHandler{ch: make(chan struct{})}
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
	ch        chan struct{} // optional: closed when target count reached
	target    int64         // 0 = no target
	closeOnce sync.Once
}

func (h *stressRecvHandler) OnNoneCopy(connID int64, data []byte) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
}

func (h *stressRecvHandler) OnRaw(connID int64, data buffers.IBuffer) {
	if data == nil {
		return
	}
	n := h.recvCount.Add(1)
	if h.target > 0 && h.ch != nil && n >= h.target {
		h.closeOnce.Do(func() { close(h.ch) })
	}
}
