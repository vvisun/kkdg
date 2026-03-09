package transshard

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate/gatetrans"
	"github.com/vvisun/kkdg/kkapp/comps/ptotrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ShardConn struct {
	conn     kknet.IConn
	connId   uint64
	shardIdx int
	nodeId   string
	closed   atomic.Bool
}

func (sc *ShardConn) clear() {
	sc.closed.Store(true)
	sc.conn = nil
	sc.shardIdx = -1
	sc.nodeId = ""
	sc.connId = 0
}

type LogicServer struct {
	nodeId      string
	nodeType    string
	conns       [kkapp.BackendShardCnt]*ShardConn
	muConns     sync.RWMutex
	clientCount int64
}

type LogicServerMgr struct {
	logicServerMap sync.Map // nodeId -> *LogicServer
	registerMu     sync.Mutex
}

func newLogicServerMgr() *LogicServerMgr {
	return &LogicServerMgr{}
}

// addLogicServer 确保 nodeId 对应的 LogicServer 存在（多连接并发注册时只创建一次），
// 然后由调用方再调用 addShardConn 挂上当前连接。
func (m *LogicServerMgr) addLogicServer(info *ptotrans.RpcMsgRegister) {
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

func (m *LogicServerMgr) chooseLogicServer(nodeType string, totalMgr gatetrans.ILogicTotalManager) (*LogicServer, bool) {
	var chooseServer *LogicServer = nil
	finded := false
	m.logicServerMap.Range(func(k any, v any) bool {
		ls := v.(*LogicServer)
		if ls.nodeType != nodeType {
			return true
		}
		if chooseServer == nil {
			chooseServer = ls
			finded = true
			return true
		}
		if totalMgr != nil {
			if totalMgr.GetSessionCount(ls.nodeId) < totalMgr.GetSessionCount(chooseServer.nodeId) {
				chooseServer = ls
				finded = true
			}
		} else {
			if ls.clientCount < chooseServer.clientCount {
				chooseServer = ls
				finded = true
			}
		}

		return true
	})
	if chooseServer == nil || !finded {
		return nil, false
	}
	return chooseServer, true
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
