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
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 运行：go run ./tests/ttws/ttwsclient -addr=localhost:8080 -conn=5000 -size=64 -interval=10ms

var (
	// 命令行参数：目标地址、并发数、消息大小
	addr         = flag.String("addr", "localhost:8080", "ws server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
)

var (
	cliMgr *clientsMgr = newClientsMgr()
)

func main() {
	flag.Parse()
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	serverUrl := u.String()
	println("serverUrl:", serverUrl)

	var wg sync.WaitGroup
	wg.Add(*connNum)

	// 构造固定大小的测试消息（二进制）
	rawMsg := make([]byte, *msgSize)
	for i := range rawMsg {
		rawMsg[i] = 0x01 // 填充固定内容
	}
	_, err := kkpacket.DefaultStreamPacket().Pack(rawMsg)
	if err != nil {
		println("pack error:", err.Error())
		return
	}

	// 启动N个协程，每个协程对应1个WS连接
	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneClient(serverUrl, rawMsg)
		}()
		// 连接建立间隔1ms，避免瞬间压垮服务端
		time.Sleep(1 * time.Millisecond)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			println("当前连接数：", cliMgr.getCount())
		}
	}()

	println("=== 压测启动 ===")
	println("目标地址：", *addr)
	println("并发连接：", *connNum)
	println("消息大小：", *msgSize, "B")
	println("发送间隔：", *sendInterval)
	println("================\n")
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

func runOneClient(serverUrl string, rawMsg []byte) {
	recv := &stressRecvHandler{}
	client := kkws.NewClient(serverUrl, nil, kknet.ApplyOptions(
		kknet.WithNoneCopyHandler(recv),
		kknet.WithLogger(kklog.GetConsoleLogger()),
		kknet.WithPingInterval(5*time.Second),
	))

	if err := connectWithRetry(client, 60, 10*time.Millisecond); err != nil {
		println("connect error:", err.Error())
		return
	}

	cliMgr.addClient(client)

	// 定时发消息，模拟业务场景
	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		bb, err := kkpacket.DefaultStreamPacket().Pack(rawMsg)
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

func connectWithRetry(client *kkws.Client, attempts int, baseBackoff time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 10 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		// time.Sleep(time.Duration(xrand.Int64(5, 50)) * time.Millisecond)
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
	// Windows typically surfaces "connectex: ... actively refused ..."
	if strings.Contains(strings.ToLower(err.Error()), "refused") {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// best-effort: match common errno
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}
	return false
}

//---------------------------------------------------

type clientsMgr struct {
	clients map[*kkws.Client]struct{}
	mu      sync.Mutex
	count   atomic.Int64
}

func newClientsMgr() *clientsMgr {
	return &clientsMgr{
		clients: make(map[*kkws.Client]struct{}),
	}
}

func (m *clientsMgr) addClient(client *kkws.Client) {
	m.mu.Lock()
	m.clients[client] = struct{}{}
	m.mu.Unlock()
	m.count.Add(1)
}

func (m *clientsMgr) removeClient(client *kkws.Client) {
	m.mu.Lock()
	delete(m.clients, client)
	m.mu.Unlock()
	m.count.Add(-1)
}

func (m *clientsMgr) getCount() int64 {
	return m.count.Load()
}
