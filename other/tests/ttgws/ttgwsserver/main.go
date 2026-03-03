// ttgwsserver: gws 压测服务端，与 ttws/ttwsserver 对比（ttws 用 kkws/gorilla，本程序用 kknet/engines/gws）
// 运行：go run ./other/tests/ttgws/ttgwsserver
package main

import (
	"bufio"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet/engines/gws"
	"github.com/vvisun/kkdg/utils/kklog"
)

func main() {
	var recvCount atomic.Int64
	var connCount atomic.Int64

	handler := &stressHandler{
		recvCount: &recvCount,
		connCount: &connCount,
	}
	srv := gws.NewServer(handler, nil)
	// 仅对 /ws 做 WebSocket 升级，与 ttws 行为一致
	srv.OnRequest = func(conn net.Conn, br *bufio.Reader, r *http.Request) {
		if r.URL.Path != "/ws" {
			writeHTTP404(conn)
			conn.Close()
			return
		}
		socket, err := srv.GetUpgrader().UpgradeFromConn(conn, br, r)
		if err != nil {
			srv.OnError(conn, err)
			return
		}
		connCount.Add(1)
		socket.ReadLoop()
		connCount.Add(-1)
	}

	go func() {
		addr := ":8080"
		kklog.Infof("[ttgws] server listening on %s (path /ws)", addr)
		if err := srv.Run(addr); err != nil {
			kklog.Errorf("[ttgws] Run: %v", err)
		}
	}()

	// 定时打印统计，便于与 ttws 对比
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		active := connCount.Load()
		recv := recvCount.Load()
		kklog.Infof("[ttgws] 并发连接: %d  累计收包: %d", active, recv)
	}
}

type stressHandler struct {
	gws.BuiltinEventHandler
	recvCount *atomic.Int64
	connCount *atomic.Int64
}

func (h *stressHandler) OnOpen(socket *gws.Conn) {
	socket.SetNoDelay(true)
}

func (h *stressHandler) OnMessage(socket *gws.Conn, message *gws.Message) {
	h.recvCount.Add(1)
	message.Close()
}

func (h *stressHandler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.WritePong(payload)
}

func writeHTTP404(conn net.Conn) {
	body := "404 Not Found"
	_, _ = conn.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 13\r\nConnection: close\r\n\r\n" + body))
}
