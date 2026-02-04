// 原始 gorilla/websocket 客户端，用于测试连接上限及与 kkws 做内存占用对比。
// 运行：go run ./tests/ttws/rawclient -addr=localhost:8080 -conn=5000 -size=64 -interval=10ms
package main

import (
	"flag"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	addr         = flag.String("addr", "localhost:8080", "ws server address")
	connNum      = flag.Int("conn", 1000, "concurrent connection num")
	msgSize      = flag.Int("size", 64, "message size (byte)")
	sendInterval = flag.Duration("interval", 10*time.Millisecond, "send interval (0 = no send, idle conns only)")
	pingInterval = flag.Duration("ping", 5*time.Second, "ping interval (0 to disable)")
)

var (
	activeConns atomic.Int64
	recvMsgs    atomic.Int64
	sendMsgs    atomic.Int64
)

func main() {
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/ws"}
	serverURL := u.String()
	println("rawclient | server:", serverURL, "conn:", *connNum, "msgSize:", *msgSize, "interval:", *sendInterval)

	var wg sync.WaitGroup
	wg.Add(*connNum)

	rawMsg := make([]byte, *msgSize)
	for i := range rawMsg {
		rawMsg[i] = 0x01
	}

	for i := 0; i < *connNum; i++ {
		go func() {
			defer wg.Done()
			runOneConn(serverURL, rawMsg)
		}()
		time.Sleep(1 * time.Millisecond)
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			println("rawclient | conns:", activeConns.Load(), "recv:", recvMsgs.Load(), "sent:", sendMsgs.Load())
		}
	}()

	println("=== rawclient 压测启动 ===")
	wg.Wait()
	println("=== rawclient 压测结束 ===")
}

func runOneConn(serverURL string, rawMsg []byte) {
	conn, _, err := websocket.DefaultDialer.Dial(serverURL, nil)
	if err != nil {
		println("dial error:", err.Error())
		return
	}
	defer conn.Close()

	activeConns.Add(1)
	defer activeConns.Add(-1)

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer conn.Close()
		for {
			mt, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				recvMsgs.Add(1)
			}
		}
	}()

	if *pingInterval > 0 {
		go func() {
			ticker := time.NewTicker(*pingInterval)
			defer ticker.Stop()
			for range ticker.C {
				deadline := time.Now().Add(*pingInterval * 2)
				if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					return
				}
			}
		}()
	}

	if *sendInterval <= 0 {
		<-readDone
		return
	}

	ticker := time.NewTicker(*sendInterval)
	defer ticker.Stop()
	for range ticker.C {
		if err := conn.WriteMessage(websocket.BinaryMessage, rawMsg); err != nil {
			return
		}
		sendMsgs.Add(1)
	}
}
