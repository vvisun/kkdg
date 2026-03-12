// ttgwsclient: 基于 kkgws 的压测客户端，与 ttws/ttwsclient 对比（参数一致，便于对比 ttws vs ttgws）
// 运行：go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=1000 -size=64 -interval=10ms
package main

import (
	"errors"
	"flag"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkgws"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	addr         = flag.String("addr", "localhost:8080", "ws server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
	connDelay    = flag.Duration("connDelay", 5*time.Millisecond, "delay between starting each connection (avoid Windows buffer space error)")
)

var (
	cliMgr     *clientsMgr      = newClientsMgr()
	streamTool kkpacket.IPacket = kkpacket.DefaultStreamPacket()
)

func main() {
	flag.Parse()
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	serverURL := u.String()
	println("serverUrl:", serverURL)

	var wg sync.WaitGroup
	wg.Add(*connNum)

	rawMsg := make([]byte, *msgSize)
	for i := range rawMsg {
		rawMsg[i] = 0x01
	}
	_, err := streamTool.Pack(rawMsg)
	if err != nil {
		println("pack error:", err.Error())
		return
	}

	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneClient(serverURL, rawMsg)
		}()
		time.Sleep(*connDelay)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			println("当前连接数：", cliMgr.getCount())
		}
	}()

	println("=== ttgws 压测启动 ===")
	println("目标地址：", *addr)
	println("并发连接：", *connNum)
	println("消息大小：", *msgSize, "B")
	println("发送间隔：", *sendInterval)
	println("建连间隔：", *connDelay, "(Windows 下若报 buffer space 可调大)")
	println("====================\n")
	wg.Wait()
}

type stressRecvHandler struct {
	recvCount atomic.Int64
	sendCount atomic.Int64
}

func (h *stressRecvHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {
	if data == nil {
		return
	}
	h.recvCount.Add(1)
}

func runOneClient(serverURL string, rawMsg []byte) {
	recv := &stressRecvHandler{}
	client := kkgws.NewClient(serverURL, nil, kknet.ApplyOptions(
		kknet.WithStreamTool(streamTool),
		kknet.WithNoneCopyHandler(recv),
		kknet.WithRpProvider(kkprocessor.NewSyncReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWorkerWriteProcessor),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithPingInterval(5*time.Second),
	))

	if err := connectWithRetry(client, 60, 10*time.Millisecond); err != nil {
		println("connect error:", err.Error())
		return
	}

	cliMgr.addClient(client)

	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		bb, err := streamTool.Pack(rawMsg)
		if err != nil {
			println("pack error:", err.Error())
			return
		}
		err = client.SendBuffer(bb)
		if err != nil {
			println("send error:", err.Error())
			return
		}
		recv.sendCount.Add(1)
	}

	cliMgr.removeClient(client)
}

func connectWithRetry(client *kkgws.Client, attempts int, baseBackoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 10 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := client.Connect(); err == nil {
			return nil
		} else {
			lastErr = err
			if !isConnRefused(err) {
				return err
			}
		}
		time.Sleep(baseBackoff + time.Duration(i)*baseBackoff)
	}
	return lastErr
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

type clientsMgr struct {
	clients map[*kkgws.Client]struct{}
	mu      sync.Mutex
	count   atomic.Int64
}

func newClientsMgr() *clientsMgr {
	return &clientsMgr{
		clients: make(map[*kkgws.Client]struct{}),
	}
}

func (m *clientsMgr) addClient(client *kkgws.Client) {
	m.mu.Lock()
	m.clients[client] = struct{}{}
	m.mu.Unlock()
	m.count.Add(1)
}

func (m *clientsMgr) removeClient(client *kkgws.Client) {
	m.mu.Lock()
	delete(m.clients, client)
	m.mu.Unlock()
	m.count.Add(-1)
}

func (m *clientsMgr) getCount() int64 {
	return m.count.Load()
}
