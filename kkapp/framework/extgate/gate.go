package extgate

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/queues/kkmpsc"
	"github.com/vvisun/kkdg/utils/xnet"
)

//设计思路：
// 客户端与网关之间建立WebSocket连接，每条连接一个读携程 + WriteGroupCnt个分组写携程。
// 逻辑服与网关之间建立TCP连接，每个逻辑服与网关之间建立BackendShardCnt条连接，组成一个LogicServer。

// ====================== 配置 ======================
const (
	GatewayTCPPort       = "9981"
	GatewayWSPort        = "8080"
	WriteGroupCnt        = 8
	BackendShardCnt      = 8
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

// ====================== MPSC 写队列（使用 kkmpsc） ======================
type writeTask struct {
	connID uint64
	data   []byte
}

// ====================== 全局 ======================
var (
	// 客户端连接列表
	clientMap sync.Map

	// 开启WriteGroupCnt个写协程，每个协程负责一个写队列。
	// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
	writeGroup [WriteGroupCnt]*kkmpsc.Queue[writeTask]

	// 逻辑服列表
	logicBackends []*LogicServer
	logicMu       sync.RWMutex

	// 逻辑服连接列表，当逻辑服连接满8条时，组成一个LogicServer。
	pendingConns []net.Conn
	pendingMu    sync.Mutex
)

// 初始化WriteGroupCnt个写协程, 负责将writeTask写入客户端。
func initWriteGroups() {
	for i := 0; i < WriteGroupCnt; i++ {
		writeGroup[i] = kkmpsc.NewQueue[writeTask]()
		// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
		go writeLoop(i)
	}
}

// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
func writeLoop(gid int) {
	q := writeGroup[gid]
	for {
		t, ok := q.Pop()
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
		byteslice.Put(t.data)
	}
}

// 发送数据到客户端。
func sendToClient(connID uint64, data []byte) {
	gid := int(connID % WriteGroupCnt)
	// 复制 data，避免调用方复用缓冲区导致竞态
	dataCpy := byteslice.GetWithLenCap(len(data), len(data))
	copy(dataCpy, data)
	wt := &writeTask{connID: connID, data: dataCpy}
	writeGroup[gid].Push(wt)
}

// ====================== 逻辑服管理 ======================
type LogicServer struct {
	nodeId   string
	nodeType string
	conns    [BackendShardCnt]net.Conn
}

type LogicServerMgr struct {
	logicServerMap sync.Map // map[string]*LogicServer
}

func (m *LogicServerMgr) addLogicServer(info *extmsg.RegisterMsg) {
	_, ok := m.logicServerMap.Load(info.NodeId)
	if ok {
		return
	}
	m.logicServerMap.Store(info.NodeId, &LogicServer{
		nodeId:   info.NodeId,
		nodeType: info.NodeType,
	})
	log.Printf("逻辑服注册: nodeId=%s nodeType=%s shardIdx=%d", info.NodeId, info.NodeType, info.ShardIdx)
}

func (m *LogicServerMgr) removeLogicServer(nodeId string) {
	m.logicServerMap.Delete(nodeId)
}

func (m *LogicServerMgr) getLogicServer(nodeId string) *LogicServer {
	ls, ok := m.logicServerMap.Load(nodeId)
	if !ok {
		return nil
	}
	return ls.(*LogicServer)
}

func (m *LogicServerMgr) addShardConn(nodeId string, shardIdx int, conn net.Conn) {
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return
	}
	ls.conns[shardIdx] = conn
}

func (m *LogicServerMgr) removeShardConn(nodeId string, shardIdx int) {
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return
	}
	ls.conns[shardIdx] = nil
}

func AddLogicServer(ls *LogicServer) {
	logicMu.Lock()
	logicBackends = append(logicBackends, ls)
	logicMu.Unlock()
	log.Println("新逻辑服接入，当前服数量:", len(logicBackends))
}

func RouteLogicConn(uid uint64, connId uint64) net.Conn {
	logicMu.RLock()
	defer logicMu.RUnlock()
	if len(logicBackends) == 0 {
		return nil
	}
	srvIdx := connId % uint64(len(logicBackends))
	shardIdx := connId % BackendShardCnt
	return logicBackends[srvIdx].conns[shardIdx]
}

// ====================== 网关TCP监听逻辑服 ======================
// 逻辑服主动向网关建立 BackendShards 条连接；网关收集满 8 条后组成一个 LogicServer。
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
		xnet.SetNoDelay(conn, true)
		pendingMu.Lock()
		pendingConns = append(pendingConns, conn)
		if len(pendingConns) < BackendShardCnt {
			pendingMu.Unlock()
			continue
		}
		ls := &LogicServer{}
		copy(ls.conns[:], pendingConns)
		pendingConns = pendingConns[:0]
		pendingMu.Unlock()
		for i := 0; i < BackendShardCnt; i++ {
			go logicReadLoop(ls.conns[i])
		}
		AddLogicServer(ls)
	}
}

func logicReadLoop(conn net.Conn) {
	dec := json.NewDecoder(conn)
	for {
		var m extmsg.DownMsg
		if err := dec.Decode(&m); err != nil {
			return
		}
		if m.Cmd == extmsg.CmdRegister {
			var registerMsg extmsg.RegisterMsg
			_ = json.Unmarshal(m.Data, &registerMsg)
			log.Printf("逻辑服注册: nodeId=%s nodeType=%s shardIdx=%d", registerMsg.NodeId, registerMsg.NodeType, registerMsg.ShardIdx)
		} else {
			sendToClient(m.ConnID, m.Data)
		}
	}
}

// ====================== WS客户端处理 + 心跳 + 断开清理 ======================
type WsHandler struct{}

func (h *WsHandler) OnOpen(s *gws.Conn) {
	connID := kknet.NextConnID()
	c := &ClientConn{
		ws:         s,
		connID:     connID,
		writeGroup: int(connID % WriteGroupCnt),
		lastBeat:   time.Now().Unix(),
	}
	clientMap.Store(connID, c)
	s.Session().Store(sessionKeyClientConn, c)
	go h.startHeartbeat(c)
}

func (h *WsHandler) OnMessage(s *gws.Conn, msg *gws.Message) {
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
	if req.Cmd == extmsg.CmdHeartbeat {
		resp, _ := json.Marshal(map[string]string{"cmd": extmsg.CmdHeartbeatAck})
		sendToClient(c.connID, resp)
		return
	}

	// 设置UID
	if req.Uid != 0 {
		c.uid = req.Uid
	}

	// 转发逻辑服
	bs, _ := json.Marshal(extmsg.UpMsg{
		ConnID: c.connID,
		Uid:    c.uid,
		Data:   msg.Bytes(),
		Cmd:    req.Cmd,
	})
	if conn := RouteLogicConn(c.uid, c.connID); conn != nil {
		_, _ = conn.Write(append(bs, '\n'))
	}
}

func (h *WsHandler) OnPing(s *gws.Conn, payload []byte) {
	_ = s.WritePong(nil)
}

func (h *WsHandler) OnPong(s *gws.Conn, payload []byte) {}

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
			bs, _ := json.Marshal(extmsg.UpMsg{
				ConnID: c.connID,
				Uid:    c.uid,
				Cmd:    extmsg.CmdClientDisconnect,
			})
			if conn := RouteLogicConn(c.uid, c.connID); conn != nil {
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
			if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
				clientMap.Delete(c.connID)
				log.Printf("心跳超时关闭 connID=%d uid=%d", c.connID, c.uid)
			}
			_ = c.ws.WriteClose(1000, []byte("heartbeat_timeout"))
			return
		}
	}
}

// ====================== 主函数 ======================
func main() {
	go StartGatewayTCPListener()
	initWriteGroups()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			heapUsedMB, heapKBPerConn := kknet.ReadMetricsStress(1000)
			log.Println("------------------------")
			log.Println("当前连接数:", 1000)
			log.Println("堆内存占用:", heapUsedMB, "MB")
			log.Println("单连接堆内存:", heapKBPerConn)
		}
	}()

	upgrader := gws.NewUpgrader(new(WsHandler), &gws.ServerOption{})
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r)
		if err != nil {
			return
		}
		// 每个客户端连接一个独立的读携程。
		go socket.ReadLoop()
	})
	log.Println("网关WS启动: ws://127.0.0.1:8080/ws")
	log.Fatal(http.ListenAndServe(":"+GatewayWSPort, nil))
}
