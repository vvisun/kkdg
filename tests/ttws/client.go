package main

import (
	"errors"
	"flag"
	"net"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkws"
)

// go run client.go -addr=localhost:8080 -conn=5000 -size=64 -interval=10ms

var (
	// 命令行参数：目标地址、并发数、消息大小
	addr         = flag.String("addr", "localhost:8080", "ws server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval")
)

func main() {
	flag.Parse()
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
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
	// msg := bb.Bytes()

	// 启动N个协程，每个协程对应1个WS连接
	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()

			// // 建立WS连接
			// conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			// if err != nil {
			// 	println("dial error:", err.Error())
			// 	return
			// }
			// defer conn.Close()

			// // 后台读消息（Echo返回的消息，防止读缓冲区满）
			// go func() {
			// 	for {
			// 		_, _, err := conn.ReadMessage()
			// 		if err != nil {
			// 			return
			// 		}
			// 	}
			// }()

			client := kkws.NewClient(u.String(), nil, kknet.ApplyOptions())
			if err := connectWithRetry(client, 30, 10*time.Millisecond); err != nil {
				println("connect error:", err.Error())
				return
			}

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
			}
		}()
		// 连接建立间隔1ms，避免瞬间压垮服务端
		time.Sleep(1 * time.Millisecond)
	}

	println("=== 压测启动 ===")
	println("目标地址：", *addr)
	println("并发连接：", *connNum)
	println("消息大小：", *msgSize, "B")
	println("发送间隔：", *sendInterval)
	println("================\n")
	wg.Wait()
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
