// ttgwsserver: 基于 kkgws(gws) 的压测服务端，与 ttws/ttwsserver 对比（ttws 用 kkws/gorilla）
// 运行：go run ./other/tests/ttgws/ttgwsserver -addr=:8080
package main

import (
	"flag"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

var addr = flag.String("addr", ":8080", "listen address")

func main() {
	flag.Parse()

	var recvCount atomic.Int64

	recv := &stressRecvHandler{recvCount: &recvCount}
	opts := kknet.ApplyOptions(
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithNoneCopyHandler(recv),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)

	lifecycleHandler := &connLifecycleHandler{}
	srv := kkgws.NewServer(*addr, lifecycleHandler, opts)
	if err := srv.Start(); err != nil {
		kklog.Errorf("[ttgws] Start: %v", err)
		return
	}
	defer srv.Stop()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		stats := srv.Stats()
		kknet.PrintStress(&stats)
	}
}

type connLifecycleHandler struct{}

func (h *connLifecycleHandler) OnConnect(c kknet.IConn) {}

func (h *connLifecycleHandler) OnClose(c kknet.IConn, err error) {}

type stressRecvHandler struct {
	recvCount *atomic.Int64
}

func (h *stressRecvHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {
	h.recvCount.Add(1)
}

func (h *stressRecvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	h.recvCount.Add(1)
	kkbuffer.Put(data)
}
