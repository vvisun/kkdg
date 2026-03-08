package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lxzan/gws"
)

// ====================== 配置 ======================
const (
	GatewayTCPPort       = "9981"
	GatewayWSPort        = "8080"
	WriteGroups          = 8
	BackendShards        = 8
	ClientHeartbeatSec   = 30
	ClientMaxMiss        = 2
	sessionKeyClientConn = "clientConn"
)

// ====================== 客户端连接 ======================
type ClientConn struct {
	ws         *gws.Conn
	connID     uint64
	uid        uint64
	writeGroup int
	closed     int32
	lastBeat   int64
}

// ====================== MPSC无锁队列 ======================
type writeTask struct {
	connID uint64
	data   []byte
}

type node struct {
	task writeTask
	next unsafe.Pointer
}

type MPSCQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func NewMPSC() *MPSCQueue {
	n := unsafe.Pointer(&node{})
	return &MPSCQueue{head: n, tail: n}
}

func (q *MPSCQueue) Enqueue(t writeTask) {
	n := unsafe.Pointer(&node{task: t})
	for {
		tail := atomic.LoadPointer(&q.tail)
		next := atomic.LoadPointer(&(*node)(tail).next)
		if tail == atomic.LoadPointer(&q.tail) {
			if next == nil {
				if atomic.CompareAndSwapPointer(&(*node)(tail).next, next, n) {
					atomic.CompareAndSwapPointer(&q.tail, tail, n)
					return
				}
			} else {
				atomic.CompareAndSwapPointer(&q.tail, tail, next)
			}
		}
	}
}

func (q *MPSCQueue) Dequeue() (writeTask, bool) {
	head := atomic.LoadPointer(&q.head)
	next := atomic.LoadPointer(&(*node)(head).next)
	if next == nil {
		return writeTask{}, false
	}
	t := (*node)(next).task
	atomic.StorePointer(&q.head, next)
	return t, true
}

// ====================== 全局 ======================
var (
	clientMap     sync.Map
	writeGroup    [WriteGroups]*MPSCQueue
	connIDSeq     uint64 = 10000
	logicBackends []*LogicServer
	logicMu       sync.RWMutex
)

func initWriteGroups() {
	for i := 0; i < WriteGroups; i++ {
		writeGroup[i] = NewMPSC()
		go writeLoop(i)
	}
}

func writeLoop(gid int) {
	q := writeGroup[gid]
	for {
		t, ok := q.Dequeue()
		if !ok {
			runtime.Gosched()
			continue
		}
		v, ok := clientMap.Load(t.connID)
		if !ok {
			continue
		}
		c := v.(*ClientConn)
		if atomic.LoadInt32(&c.closed) == 1 {
			continue
		}
		_ = c.ws.WriteMessage(gws.OpcodeText, t.data)
	}
}

func sendToClient(connID uint64, data []byte) {
	gid := int(connID % WriteGroups)
	writeGroup[gid].Enqueue(writeTask{connID: connID, data: data})
}

// ====================== 消息结构 ======================
type UpMsg struct {
	ConnID uint64          `json:"connID"`
	Uid    uint64          `json:"uid"`
	Data   json.RawMessage `json:"data"`
	Cmd    string          `json:"cmd,omitempty"`
}

type DownMsg struct {
	ConnID uint64          `json:"connID"`
	Data   json.RawMessage `json:"data"`
}

// ====================== 逻辑服管理 ======================
type LogicServer struct {
	conns [BackendShards]net.Conn
}

func AddLogicServer(ls *LogicServer) {
	logicMu.Lock()
	logicBackends = append(logicBackends, ls)
	logicMu.Unlock()
	log.Println("新逻辑服接入，当前服数量:", len(logicBackends))
}

func RouteLogicConn(uid uint64) net.Conn {
	logicMu.RLock()
	defer logicMu.RUnlock()
	if len(logicBackends) == 0 {
		return nil
	}
	srvIdx := uid % uint64(len(logicBackends))
	shardIdx := uid % BackendShards
	return logicBackends[srvIdx].conns[shardIdx]
}

// ====================== 网关TCP监听逻辑服 ======================
func StartGatewayTCPListener() {
	lis, err := net.Listen("tcp", ":"+GatewayTCPPort)
	if err != nil {
		log.Fatal("网关TCP监听失败:", err)
	}
	log.Println("网关TCP监听端口:", GatewayTCPPort)
	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		go handleLogicInConn(conn)
	}
}

func handleLogicInConn(conn net.Conn) {
	ls := &LogicServer{}
	ls.conns[0] = conn

	for i := 1; i < BackendShards; i++ {
		c, err := net.Dial("tcp", conn.RemoteAddr().String())
		if err != nil {
			log.Println("逻辑服分流创建失败:", err)
			return
		}
		ls.conns[i] = c
	}

	for i := 0; i < BackendShards; i++ {
		go logicReadLoop(ls.conns[i])
	}
	AddLogicServer(ls)
}

func logicReadLoop(conn net.Conn) {
	dec := json.NewDecoder(conn)
	for {
		var m DownMsg
		if err := dec.Decode(&m); err != nil {
			return
		}
		sendToClient(m.ConnID, m.Data)
	}
}

// ====================== WS客户端处理 + 心跳 + 断开清理 ======================
type WsHandler struct{}

func (h *WsHandler) OnOpen(s *gws.Conn) {
	connID := atomic.AddUint64(&connIDSeq, 1)
	c := &ClientConn{
		ws:         s,
		connID:     connID,
		writeGroup: int(connID % WriteGroups),
		lastBeat:   time.Now().Unix(),
	}
	clientMap.Store(connID, c)
	s.Session().Store(sessionKeyClientConn, c)
	go h.startHeartbeat(c)
}

func (h *WsHandler) OnMessage(s *gws.Conn, msg gws.Message) {
	defer msg.Close()
	cc, ok := s.Session().Load(sessionKeyClientConn)
	if !ok {
		return
	}
	c := cc.(*ClientConn)
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}

	// 心跳更新
	atomic.StoreInt64(&c.lastBeat, time.Now().Unix())

	// 解析命令
	var req struct {
		Cmd string `json:"cmd"`
		Uid uint64 `json:"uid"`
	}
	_ = json.Unmarshal(msg.Bytes(), &req)

	// 心跳包直接响应，不上发逻辑服
	if req.Cmd == "heartbeat" {
		resp, _ := json.Marshal(map[string]string{"cmd": "heartbeat_ack"})
		sendToClient(c.connID, resp)
		return
	}

	// 设置UID
	if req.Uid != 0 {
		c.uid = req.Uid
	}

	// 转发逻辑服
	bs, _ := json.Marshal(UpMsg{
		ConnID: c.connID,
		Uid:    c.uid,
		Data:   msg.Bytes(),
		Cmd:    req.Cmd,
	})
	if conn := RouteLogicConn(c.uid); conn != nil {
		_, _ = conn.Write(append(bs, '\n'))
	}
}

func (h *WsHandler) OnClose(s *gws.Conn, err error) {
	cc, ok := s.Session().Load(sessionKeyClientConn)
	if !ok {
		return
	}
	c := cc.(*ClientConn)
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		clientMap.Delete(c.connID)
		log.Printf("客户端已清理 connID=%d uid=%d err=%v", c.connID, c.uid, err)

		// 通知逻辑服：玩家断开
		go func() {
			bs, _ := json.Marshal(UpMsg{
				ConnID: c.connID,
				Uid:    c.uid,
				Cmd:    "client_disconnect",
			})
			if conn := RouteLogicConn(c.uid); conn != nil {
				_, _ = conn.Write(append(bs, '\n'))
			}
		}()
	}
}

// 心跳超时检测
func (h *WsHandler) startHeartbeat(c *ClientConn) {
	ticker := time.NewTicker(ClientHeartbeatSec * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if atomic.LoadInt32(&c.closed) == 1 {
			return
		}
		now := time.Now().Unix()
		last := atomic.LoadInt64(&c.lastBeat)
		if now-last > ClientHeartbeatSec*ClientMaxMiss {
			_ = c.ws.WriteClose(1000, []byte("heartbeat_timeout"))
			return
		}
	}
}

// ====================== 主函数 ======================
func main() {
	go StartGatewayTCPListener()
	initWriteGroups()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		gws.Upgrade(w, r, new(WsHandler), nil)
	})
	log.Println("网关WS启动: ws://127.0.0.1:8080/ws")
	log.Fatal(http.ListenAndServe(":"+GatewayWSPort, nil))
}
