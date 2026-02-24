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

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/kklog"
)

// tttcpclient: TCP stress client, similar to tests/ttws/ttwsclient.
//
// Example:
//   go run ./tests/tttcp/tttcpclient -addr=127.0.0.1:19090 -conn=5000 -size=64 -interval=10ms

var (
	addr         = flag.String("addr", "127.0.0.1:19090", "tcp server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
)

var (
	cliMgr *clientsMgr = newClientsMgr()
)

func main() {
	flag.Parse()
	serverAddr := *addr
	println("serverAddr:", serverAddr)

	var wg sync.WaitGroup
	wg.Add(*connNum)

	// build fixed-size test payload
	rawMsg := make([]byte, *msgSize)
	for i := range rawMsg {
		rawMsg[i] = 0x01
	}
	if _, err := kkpacket.DefaultStreamPacket().Pack(rawMsg); err != nil {
		println("pack error:", err.Error())
		return
	}

	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneClient(serverAddr, rawMsg)
		}()
		time.Sleep(1 * time.Millisecond)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			println("当前连接数：", cliMgr.getCount())
		}
	}()

	println("=== tttcp 压测启动 ===")
	println("目标地址：", *addr)
	println("并发连接：", *connNum)
	println("消息大小：", *msgSize, "B")
	println("发送间隔：", *sendInterval)
	println("=====================")
	wg.Wait()
}

// stressRecvHandler counts received messages for stress tests.
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

func runOneClient(serverAddr string, rawMsg []byte) {
	recv := &stressRecvHandler{}
	client := kktcp.NewClient(serverAddr, nil, kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(recv),
		kknet.WithLogger(kklog.GetConsoleLogger()),
	))

	if err := connectWithRetry(client, 60, 10*time.Millisecond); err != nil {
		println("connect error:", err.Error())
		return
	}

	cliMgr.addClient(client)

	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		bb, err := kkpacket.DefaultStreamPacket().Pack(rawMsg)
		if err != nil {
			println("pack error:", err.Error())
			return
		}
		if err := client.SendBuffer(bb); err != nil {
			println("send error:", err.Error())
			return
		}
		recv.sendCount.Add(1)
	}

	cliMgr.removeClient(client)
}

func connectWithRetry(client *kktcp.GnetClient, attempts int, baseBackoff time.Duration) error {
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

//---------------------------------------------------

type clientsMgr struct {
	clients map[*kktcp.GnetClient]struct{}
	mu      sync.Mutex
	count   atomic.Int64
}

func newClientsMgr() *clientsMgr {
	return &clientsMgr{
		clients: make(map[*kktcp.GnetClient]struct{}),
	}
}

func (m *clientsMgr) addClient(client *kktcp.GnetClient) {
	m.mu.Lock()
	m.clients[client] = struct{}{}
	m.mu.Unlock()
	m.count.Add(1)
}

func (m *clientsMgr) removeClient(client *kktcp.GnetClient) {
	m.mu.Lock()
	delete(m.clients, client)
	m.mu.Unlock()
	m.count.Add(-1)
}

func (m *clientsMgr) getCount() int64 {
	return m.count.Load()
}
