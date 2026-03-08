package extgate

import (
	"encoding/json"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xnet"
)

var (
	// 逻辑服列表
	logicServerMgr = &LogicServerMgr{}
	logicConnMgr   sync.Map // map[connId]*ShardConn
)

type ShardConn struct {
	conn     net.Conn
	connId   uint64
	shardIdx int
	nodeId   string
	closed   atomic.Bool
}

type LogicServer struct {
	nodeId      string
	nodeType    string
	conns       [BackendShardCnt]*ShardConn
	muConns     sync.RWMutex
	clientCount int64
}

type LogicServerMgr struct {
	logicServerMap sync.Map // map[string]*LogicServer
	registerMu     sync.Mutex
}

// addLogicServer 确保 nodeId 对应的 LogicServer 存在（多连接并发注册时只创建一次），
// 然后由调用方再调用 addShardConn 挂上当前连接。
func (m *LogicServerMgr) addLogicServer(info *extmsg.RegisterMsg) {
	newLS := &LogicServer{
		nodeId:   info.NodeId,
		nodeType: info.NodeType,
	}
	m.registerMu.Lock()
	actual, loaded := m.logicServerMap.LoadOrStore(info.NodeId, newLS)
	if !loaded {
		kklog.Infof("逻辑服注册: nodeId=%s nodeType=%s（首条连接）", info.NodeId, info.NodeType)
	}
	m.registerMu.Unlock()
	_ = actual // 已存在或新建的 *LogicServer，addShardConn 会通过 getLogicServer 取到
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

func (m *LogicServerMgr) addShardConn(nodeId string, shardIdx int, conn *ShardConn) {
	if shardIdx < 0 || shardIdx >= BackendShardCnt {
		kklog.Errorf("逻辑服[nodeId=%s]添加连接失败 shardIdx=%d 超出范围", nodeId, shardIdx)
		return
	}
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		kklog.Errorf("逻辑服[nodeId=%s]添加连接失败 nodeId不存在", nodeId)
		return
	}
	ls.muConns.Lock()
	conn.nodeId = nodeId
	conn.shardIdx = shardIdx
	ls.conns[shardIdx] = conn
	ls.muConns.Unlock()
	kklog.Infof("逻辑服 shard 已挂接: nodeId=%s shardIdx=%d", nodeId, shardIdx)
}

func (m *LogicServerMgr) removeShardConn(nodeId string, shardIdx int) {
	if shardIdx < 0 || shardIdx >= BackendShardCnt {
		return
	}
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return
	}
	ls.muConns.Lock()
	if ls.conns[shardIdx] != nil {
		ls.conns[shardIdx].shardIdx = -1
	}
	ls.conns[shardIdx] = nil
	ls.muConns.Unlock()
}

func (m *LogicServerMgr) getShardConn(nodeId string, shardIdx int) *ShardConn {
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return nil
	}
	ls.muConns.RLock()
	conn := ls.conns[shardIdx]
	ls.muConns.RUnlock()
	return conn
}

// ====================== 网关TCP监听逻辑服 ======================
// 逻辑服主动向网关建立 BackendShards 条连接；网关收集满 8 条后组成一个 LogicServer。
func startGatewayTCPListener() {
	lis, err := net.Listen("tcp", ":"+GatewayTCPPort)
	if err != nil {
		kklog.Fatal("网关TCP监听失败: %v", err)
	}
	kklog.Infof("网关TCP监听端口: %s", GatewayTCPPort)
	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		xnet.SetNoDelay(conn, true)

		shardConn := &ShardConn{
			conn:     conn,
			connId:   kknet.NextConnID(),
			shardIdx: -1,
			nodeId:   "",
		}
		logicConnMgr.Store(shardConn.connId, shardConn)

		go logicReadLoop(shardConn)
	}
}

// 逻辑服连接关闭事件处理
//
//	@param shardConn 逻辑服连接
//	@param err 错误
func onLogicConnClose(shardConn *ShardConn, err error) {
	shardConn.closed.Store(true)
	logicConnMgr.Delete(shardConn.connId)
	logicServerMgr.removeShardConn(shardConn.nodeId, shardConn.shardIdx)
	kklog.Infof("逻辑服[nodeId=%s]连接已关闭 connId=%d shardIdx=%d err=%v", shardConn.nodeId, shardConn.connId, shardConn.shardIdx, err)
}

// 每个逻辑服连接一个读携程。
func logicReadLoop(shardConn *ShardConn) {
	dec := json.NewDecoder(shardConn.conn)
	for {
		var m extmsg.DownMsg
		if err := dec.Decode(&m); err != nil {
			onLogicConnClose(shardConn, err)
			return
		}
		if m.Cmd == extmsg.CmdRegister {
			// 注册逻辑服
			var registerMsg extmsg.RegisterMsg
			_ = json.Unmarshal(m.Data, &registerMsg)
			logicServerMgr.addLogicServer(&registerMsg)
			logicServerMgr.addShardConn(registerMsg.NodeId, registerMsg.ShardIdx, shardConn)
		} else {
			// 转发逻辑服消息到客户端
			sendToClient(m.ConnID, m.Data)
		}
	}
}

var (
	// 每个逻辑服连接一个写携程。
	logicWriteThread = kkprocessor.NewWorkerQueue(1)
)

// 将客户端消息转发到逻辑服。
//
//	@param connId 客户端连接ID
//	@param data 数据
//	@param uid 用户ID
//	@param cmd 命令
func transToLogic(connId uint64, data []byte, uid uint64, cmd string) {
	conn := routeLogicConn(connId)
	if conn == nil {
		return
	}

	dataCpy := byteslice.GetWithLenCap(len(data), len(data))
	copy(dataCpy, data)

	logicWriteThread.Push(func() {
		bs, _ := json.Marshal(extmsg.UpMsg{
			ConnID: connId,
			Uid:    uid,
			Data:   dataCpy,
			Cmd:    cmd,
		})
		_, _ = conn.Write(append(bs, '\n'))
		byteslice.Put(dataCpy)
	})
}

// 将客户端消息转发到逻辑服。
//
//	@param connId 客户端连接ID
//	@return 逻辑服连接
func routeLogicConn(connId uint64) net.Conn {
	// 选择一个逻辑服。todo: 优化选择策略
	var chooseServer *LogicServer
	minClientCount := int64(math.MaxInt64)
	logicServerMgr.logicServerMap.Range(func(key any, value any) bool {
		ls := value.(*LogicServer)
		if atomic.LoadInt64(&ls.clientCount) < minClientCount {
			minClientCount = atomic.LoadInt64(&ls.clientCount)
			chooseServer = ls
		}
		return true
	})
	if chooseServer == nil {
		return nil
	}

	shardIdx := connId % BackendShardCnt
	chooseServer.muConns.RLock()
	sconn := chooseServer.conns[shardIdx]
	chooseServer.muConns.RUnlock()
	if sconn == nil {
		return nil
	}
	return sconn.conn
}
