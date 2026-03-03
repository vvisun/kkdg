package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// 纯 gnet TCP 压测服务，用来对比 kktcp 的堆内存占用。
//
// 运行示例：
//   go run ./other/tests/ttgnet/ttgnetserver
//
// 默认监听：127.0.0.1:19190
// 每秒打印一次：
//   堆内存占用：XXX MB
//   单连接堆内存：YYY KB/conn

type stressHandler struct {
	*gnet.BuiltinEventEngine
	activeConns int64
}

func (h *stressHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	fmt.Println("[ttgnetserver] gnet server listening on tcp://127.0.0.1:19190")

	// 定时打印堆内存与每连接占用
	go func() {
		for {
			time.Sleep(time.Second)
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heapBytes := ms.HeapAlloc
			heapMB := float64(heapBytes) / (1024 * 1024)
			conns := atomic.LoadInt64(&h.activeConns)
			perConnKB := 0.0
			if conns > 0 {
				perConnKB = float64(heapBytes) / 1024 / float64(conns)
			}
			fmt.Printf("当前连接数：%d\n", conns)
			fmt.Printf("堆内存占用：%.0f MB\n", heapMB)
			fmt.Printf("单连接堆内存：%.2f KB/conn\n", perConnKB)
			fmt.Println("------------------------")
		}
	}()

	return gnet.None
}

func (h *stressHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	atomic.AddInt64(&h.activeConns, 1)
	return nil, gnet.None
}

func (h *stressHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	atomic.AddInt64(&h.activeConns, -1)
	return gnet.None
}

func (h *stressHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	// 仅 drain 数据，不做解包处理；这里的目标是观测内存而非业务逻辑。
	for {
		if c.InboundBuffered() == 0 {
			return gnet.None
		}
		// 读取所有缓冲数据并丢弃
		if _, err := c.Next(c.InboundBuffered()); err != nil {
			return gnet.Close
		}
	}
}

func main() {
	h := &stressHandler{}
	// 为了更容易看出 gnet 自身的内存开销，这里刻意使用单线程事件循环。
	if err := gnet.Run(h, "tcp://127.0.0.1:19190",
		gnet.WithMulticore(false),
		//gnet.WithLogger(gnet.NopLogger()),
	); err != nil {
		panic(err)
	}
}
