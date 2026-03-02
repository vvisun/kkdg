// ttgwsclient: gws 压测客户端，与 ttws/ttwsclient 对比（参数一致，便于对比 ttws vs ttgws）
// 运行：go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=1000 -size=64 -interval=10ms
package main

import (
	"errors"
	"flag"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kknet/gws"
)

var (
	addr         = flag.String("addr", "localhost:8080", "ws server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
)

var clientCount atomic.Int64

func main() {
	flag.Parse()
	serverURL := "ws://" + *addr + "/ws"
	println("serverUrl:", serverURL)

	var wg sync.WaitGroup
	wg.Add(*connNum)

	payload := make([]byte, *msgSize)
	for i := range payload {
		payload[i] = 0x01
	}

	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneClient(serverURL, payload)
		}()
		time.Sleep(1 * time.Millisecond)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			println("当前连接数：", clientCount.Load())
		}
	}()

	println("=== ttgws 压测启动 ===")
	println("目标地址：", *addr)
	println("并发连接：", *connNum)
	println("消息大小：", *msgSize, "B")
	println("发送间隔：", *sendInterval)
	println("====================\n")
	wg.Wait()
}

type clientHandler struct {
	gws.BuiltinEventHandler
}

func (h *clientHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	message.Close()
}

func runOneClient(serverURL string, payload []byte) {
	handler := &clientHandler{}
	conn, _, err := gws.NewClient(handler, &gws.ClientOption{Addr: serverURL})
	if err != nil {
		if !isConnRefused(err) {
			println("connect error:", err.Error())
			return
		}
		for attempt := 0; attempt < 60; attempt++ {
			conn, _, err = gws.NewClient(handler, &gws.ClientOption{Addr: serverURL})
			if err == nil {
				runClientLoop(conn, payload)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		println("connect error:", err.Error())
		return
	}
	runClientLoop(conn, payload)
}

func runClientLoop(conn *gws.Conn, payload []byte) {
	defer conn.NetConn().Close()
	clientCount.Add(1)
	defer clientCount.Add(-1)

	go conn.ReadLoop()
	time.Sleep(20 * time.Millisecond)

	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := conn.WriteMessage(gws.OpcodeBinary, payload); err != nil {
			return
		}
	}
}

func isConnRefused(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "refused") {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}
	return false
}
