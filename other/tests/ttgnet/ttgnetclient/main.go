package main

// ttgnetclient: 纯 TCP(gnet 服务端) 压测客户端，参数和行为尽量贴近 ttgwsclient，便于三方对比：
//   - addr:   TCP 服务器地址（如 127.0.0.1:19190）
//   - conn:   并发连接数
//   - size:   每条消息大小（字节）
//   - interval: 每个连接的发送间隔
//   - connDelay: 建连间隔，用于避免 Windows 上的 buffer space 报错
//
// 示例：
//   go run ./other/tests/ttgnet/ttgnetclient -addr=127.0.0.1:19190 -conn=5555 -size=512 -interval=25ms -connDelay=1ms

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	addr         = flag.String("addr", "127.0.0.1:19190", "gnet tcp server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 512, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
	connDelay    = flag.Duration("connDelay", 1*time.Millisecond, "delay between starting each connection")
)

var clientCount atomic.Int64

func main() {
	flag.Parse()

	fmt.Println("=== ttgnet 压测启动 ===")
	fmt.Println("目标地址：", *addr)
	fmt.Println("并发连接：", *connNum)
	fmt.Println("消息大小：", *msgSize, "B")
	fmt.Println("发送间隔：", *sendInterval)
	fmt.Println("建连间隔：", *connDelay)
	fmt.Println("======================")

	payload := make([]byte, *msgSize)
	for i := range payload {
		payload[i] = 0x01
	}

	var wg sync.WaitGroup
	wg.Add(*connNum)

	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneClient(*addr, payload)
		}()
		time.Sleep(*connDelay)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Println("当前连接数：", clientCount.Load())
		}
	}()

	wg.Wait()
}

func runOneClient(serverAddr string, payload []byte) {
	for {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			if !isConnRefused(err) {
				fmt.Println("connect error:", err.Error())
				return
			}
			// retry a few times on ECONNREFUSED
			ok := false
			for attempt := 0; attempt < 60; attempt++ {
				conn, err = net.Dial("tcp", serverAddr)
				if err == nil {
					ok = true
					break
				}
				if !isConnRefused(err) {
					fmt.Println("connect error:", err.Error())
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			if !ok {
				fmt.Println("connect error:", err.Error())
				return
			}
		}

		clientCount.Add(1)
		runClientLoop(conn, payload)
		clientCount.Add(-1)

		// 连接退出后直接返回，和 ttgwsclient 行为一致（由上层控制总连接数）
		return
	}
}

func runClientLoop(conn net.Conn, payload []byte) {
	defer conn.Close()

	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		if _, err := conn.Write(payload); err != nil {
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

