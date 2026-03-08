package extgate

import (
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/queues/kkmpsc"
)

//设计思路：
// 客户端与网关之间建立WebSocket连接，每条连接一个读携程 + WriteGroupCnt个分组写携程。
// 逻辑服与网关之间建立TCP连接，每个逻辑服与网关之间建立BackendShardCnt条连接，组成一个LogicServer。

// ====================== 配置 ======================
const (
	GatewayTCPPort     = "9981"
	GatewayWSPort      = "8080"
	WriteGroupCnt      = 8
	BackendShardCnt    = 8
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
	clientMap sync.Map

	// 开启WriteGroupCnt个写协程，每个协程负责一个写队列。
	// 写协程从写队列中取出writeTask，然后调用ws.WriteMessage写入客户端。
	writeGroup [WriteGroupCnt]*kkmpsc.Queue[writeTask]
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

// ====================== 主函数 ======================
func StartUp() {
	go startGatewayTCPListener()
	initWriteGroups()

	go func() {
		for {
			time.Sleep(5 * time.Second)
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
