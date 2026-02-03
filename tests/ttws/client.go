package main

import (
	"flag"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kknet/kkpacket"
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
	msg := make([]byte, *msgSize)
	for i := range msg {
		msg[i] = 0x01 // 填充固定内容
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(msg)
	if err != nil {
		println("pack error:", err.Error())
		return
	}
	msg = bb.Bytes()

	// 启动N个协程，每个协程对应1个WS连接
	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			// 建立WS连接
			conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				println("dial error:", err.Error())
				return
			}
			defer conn.Close()

			// 后台读消息（Echo返回的消息，防止读缓冲区满）
			go func() {
				for {
					_, _, err := conn.ReadMessage()
					if err != nil {
						return
					}
				}
			}()

			// 定时发消息，模拟业务场景
			ticker := time.NewTicker(*sendInterval)
			defer ticker.Stop()
			for range ticker.C {
				err := conn.WriteMessage(websocket.BinaryMessage, msg)
				if err != nil {
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
