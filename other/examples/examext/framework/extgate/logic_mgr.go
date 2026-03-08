package extgate

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/other/examples/examext/framework/extmsg"
	"github.com/vvisun/kkdg/utils/kklog"
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
	conns       [kkapp.BackendShardCnt]*ShardConn
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
	if shardIdx < 0 || shardIdx >= kkapp.BackendShardCnt {
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
	if shardIdx < 0 || shardIdx >= kkapp.BackendShardCnt {
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
