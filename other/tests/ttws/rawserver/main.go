// 原始 gorilla/websocket 服务端，用于测试连接上限及与 kkws 做内存占用对比。
// 运行：go run ./tests/ttws/rawserver -addr=:8080
package main

import (
	"flag"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	addr         = flag.String("addr", ":8080", "listen address")
	pingInterval = flag.Duration("ping", 5*time.Second, "ping interval (0 to disable)")
	readTimeout  = flag.Duration("readtimeout", 30*time.Second, "read deadline refresh on pong (0 to disable)")
)

var (
	activeConns atomic.Int64
	recvBytes   atomic.Int64
	recvMsgs    atomic.Int64
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin:     func(*http.Request) bool { return true },
}

func main() {
	flag.Parse()

	http.HandleFunc("/ws", handleWS)
	go printStats()

	srv := &http.Server{Addr: *addr}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	activeConns.Add(1)
	defer func() {
		_ = conn.Close()
		activeConns.Add(-1)
	}()

	if *readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(*readTimeout))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(*readTimeout))
		})
	}

	// 可选：服务端发 Ping 保活（与 kkws 行为一致便于对比）
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

	// 读循环：只收二进制帧，不解析业务（阻塞直到连接关闭）
	readLoop(conn)
}

func readLoop(conn *websocket.Conn) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.BinaryMessage {
			recvMsgs.Add(1)
			recvBytes.Add(int64(len(data)))
		}
	}
}

func printStats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		heapMB := mem.Alloc / 1024 / 1024
		conns := activeConns.Load()
		msgs := recvMsgs.Load()
		bytes := recvBytes.Load()
		println("rawserver | conns:", conns, "recvMsgs:", msgs, "recvBytes:", bytes, "heapMB:", heapMB)
	}
}
