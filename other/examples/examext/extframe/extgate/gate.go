package extgate

import (
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/kkspsc"
)

// counter for connection ID. unique id for the connection.
var connIDCounter atomic.Uint64

const maxUint64 = ^uint64(0)

// nextConnID returns a unique connection ID.
func nextConnID() kknet.CONN_ID {
	// 如果超过uint64最大值，则重置为0。理论上不可能，但以防万一。
	// 基本上达到uint64最大值，即使每秒1000万个连接，也需要几十年，
	// 这时候1~几亿的connId基本上必然已经断开逻辑也已经清理了，不存在逻辑向死亡的connId发消息的情况。
	if connIDCounter.Load() >= maxUint64 {
		connIDCounter.Store(0)
	}
	return connIDCounter.Add(1)
}

//设计思路：
// 客户端与网关之间建立WebSocket连接，每条连接一个读携程 + WriteGroupCnt个分组写携程。
// 逻辑服与网关之间建立TCP连接，每个逻辑服与网关之间建立BackendShardCnt条连接，组成一个LogicServer。

// ====================== 配置 ======================
const (
	GatewayTCPPort     = "9981"
	GatewayWSPort      = "8080"
	EnableWriteGroup   = false
	WriteGroupCnt      = 8
	ClientHeartbeatSec = 30
	ClientMaxMiss      = 2
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
	clientMap   sync.Map
	clientCount int32

	// 开启WriteGroupCnt个写协程，每个协程负责一个写队列。
	// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
	writeGroup [WriteGroupCnt]*kkspsc.Queue[writeTask]
)

// 初始化WriteGroupCnt个写协程, 负责将writeTask写入客户端。
func initWriteGroups() {
	if !EnableWriteGroup {
		return
	}
	for i := 0; i < WriteGroupCnt; i++ {
		writeGroup[i] = kkspsc.NewQueue[writeTask]()
		// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
		go writeLoop(i)
	}
}

// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
func writeLoop(gid int) {
	if !EnableWriteGroup {
		kklog.PanicLog("EnableWriteGroup is false")
	}
	q := writeGroup[gid]
	for {
		t, ok := q.Pop()
		if !ok {
			runtime.Gosched()
			continue
		}
		v, ok := clientMap.Load(t.connID)
		if !ok {
			byteslice.Put(t.data)
			continue
		}
		c := v.(*ClientConn)
		if atomic.LoadInt32(&c.closed) == 1 {
			byteslice.Put(t.data)
			continue
		}
		_ = c.ws.WriteMessage(gws.OpcodeText, t.data)
		byteslice.Put(t.data)
	}
}

// 发送数据到客户端。
func sendToClient(connID uint64, data []byte) {
	// 复制 data，避免调用方复用缓冲区导致竞态
	dataCpy := byteslice.GetWithLenCap(len(data), len(data))
	copy(dataCpy, data)

	if EnableWriteGroup {
		wt := &writeTask{connID: connID, data: dataCpy}
		gid := int(connID % WriteGroupCnt)
		writeGroup[gid].Push(wt)
		return
	}

	v, ok := clientMap.Load(connID)
	if !ok {
		return
	}
	c := v.(*ClientConn)
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}
	c.ws.WriteAsync(gws.OpcodeText, data, func(err error) {
		if err != nil {
			return
		}
		byteslice.Put(data)
	})
}

// ====================== 主函数 ======================
func StartUp() {
	go startGatewayTCPListener()

	if EnableWriteGroup {
		initWriteGroups()
	}

	go func() {
		for {
			connCount := atomic.LoadInt32(&clientCount)
			time.Sleep(5 * time.Second)
			heapUsedMB, heapKBPerConn := kknet.ReadMetricsStress(int64(connCount))
			kklog.Debugf("------------------------")
			kklog.Debugf("当前连接数: %d", connCount)
			kklog.Debugf("堆内存占用: %d MB", heapUsedMB)
			kklog.Debugf("单连接堆内存: %.2f KB/conn", heapKBPerConn)
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
	kklog.Debugf("网关WS启动: ws://127.0.0.1:8080/ws")
	kklog.Fatal(http.ListenAndServe(":"+GatewayWSPort, nil))
}
