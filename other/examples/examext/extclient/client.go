package main

import (
	"encoding/json"
	"log"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ====================== 压测配置 ======================
const (
	GatewayWSAddr     = "ws://127.0.0.1:8080/ws"
	TargetConnections = 1000 // 目标总连接数
	CPS               = 50   // 每秒新建连接数
	MsgIntervalMS     = 500  // 消息发送间隔(ms)
	EnableHeartbeat   = true // 开启心跳
)

// 全局统计
var (
	connTotal   int32 = 0
	connSuccess int32 = 0
	connClosed  int32 = 0

	msgSend     uint64 = 0
	msgRecv     uint64 = 0
	msgDelaySum uint64 = 0
	msgDelayCnt uint64 = 0
)

type clientCtx struct {
	idx    int
	uid    uint64
	conn   *websocket.Conn
	closed int32
}

func main() {
	log.Println("=== 网关压测客户端 增强版 ===")
	log.Printf("目标连接: %d | CPS: %d | 消息间隔: %dms",
		TargetConnections, CPS, MsgIntervalMS)

	// 平滑建连
	go func() {
		for i := 0; i < TargetConnections; i++ {
			if atomic.LoadInt32(&connTotal) >= TargetConnections {
				break
			}
			go startClient(i)
			atomic.AddInt32(&connTotal, 1)
			time.Sleep(time.Second / time.Duration(CPS))
		}
	}()

	// 每秒统计
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		stat()
	}
}

func startClient(idx int) {
	uid := uint64(10000 + idx)
	ctx := &clientCtx{idx: idx, uid: uid}

	// 重连循环
	for {
		if atomic.LoadInt32(&connSuccess) >= TargetConnections {
			time.Sleep(100 * time.Millisecond)
		}
		err := ctx.dial()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		ctx.run()
		atomic.AddInt32(&connClosed, 1)
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *clientCtx) dial() error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 3 * time.Second
	dialer.NetDial = func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, 3*time.Second)
	}

	conn, _, err := dialer.Dial(GatewayWSAddr, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	atomic.AddInt32(&connSuccess, 1)
	return nil
}

func (c *clientCtx) run() {
	defer func() {
		atomic.StoreInt32(&c.closed, 1)
		_ = c.conn.Close()
	}()

	// 登录
	login, _ := json.Marshal(map[string]any{
		"uid": c.uid,
		"cmd": "login",
	})
	_ = c.conn.WriteMessage(websocket.TextMessage, login)

	// 读协程
	go c.readLoop()

	// 写循环
	t := time.NewTicker(time.Duration(MsgIntervalMS) * time.Millisecond)
	defer t.Stop()
	for range t.C {
		if atomic.LoadInt32(&c.closed) == 1 {
			return
		}
		ts := time.Now().UnixMilli()
		msg, _ := json.Marshal(map[string]any{
			"uid":  c.uid,
			"cmd":  "ping",
			"ts":   ts,
			"data": "benchmark message",
		})
		err := c.conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
		atomic.AddUint64(&msgSend, 1)
	}
}

func (c *clientCtx) readLoop() {
	for {
		typ, body, err := c.conn.ReadMessage()
		if err != nil || typ == websocket.CloseMessage {
			atomic.StoreInt32(&c.closed, 1)
			return
		}
		atomic.AddUint64(&msgRecv, 1)

		// 计算延迟
		var m struct{ TS int64 }
		if json.Unmarshal(body, &m) == nil && m.TS > 0 {
			delay := time.Now().UnixMilli() - m.TS
			atomic.AddUint64(&msgDelaySum, uint64(delay))
			atomic.AddUint64(&msgDelayCnt, 1)
		}
	}
}

// ====================== 统计输出 ======================
func stat() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	currConn := atomic.LoadInt32(&connSuccess) - atomic.LoadInt32(&connClosed)
	send := atomic.SwapUint64(&msgSend, 0)
	recv := atomic.SwapUint64(&msgRecv, 0)

	delaySum := atomic.SwapUint64(&msgDelaySum, 0)
	delayCnt := atomic.SwapUint64(&msgDelayCnt, 0)
	avgDelay := 0
	if delayCnt > 0 {
		avgDelay = int(delaySum / delayCnt)
	}

	log.Printf(
		"[压测] 在线: %4d | SendQPS: %4d | RecvQPS: %4d | 延迟: %3dms | Goroutine: %4d | Heap: %.1fMB",
		currConn,
		send, recv,
		avgDelay,
		runtime.NumGoroutine(),
		float64(m.HeapAlloc)/1024/1024,
	)
}
