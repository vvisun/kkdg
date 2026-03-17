package transshard

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ShardConn struct {
	conn     kknet.IConn
	connId   uint64
	shardIdx int
	nodeId   string
}

func (sc *ShardConn) clear() {
	sc.conn = nil
	sc.shardIdx = -1
	sc.nodeId = ""
	sc.connId = 0
}

//--------------------------------------------------

type LogicServer struct {
	nodeId               string
	nodeType             string
	conns                [transport.BackendShardCnt]*ShardConn
	muConns              sync.RWMutex
	activeShardConnCount int32
}

var _ gatetrans.IMember = (*LogicServer)(nil)

func (ls *LogicServer) GetNodeID() string {
	return ls.nodeId
}

func (ls *LogicServer) GetNodeType() string {
	return ls.nodeType
}

func (ls *LogicServer) updateActiveShardConnCount() {
	count := 0
	for _, sc := range ls.conns {
		if sc != nil && sc.conn != nil {
			count++
		}
	}
	atomic.StoreInt32(&ls.activeShardConnCount, int32(count))
}

//--------------------------------------------------

type LogicServerMgr struct {
	logicServerMap sync.Map // nodeId -> *LogicServer
	registerMu     sync.Mutex
}

var _ gatetrans.IMemberMgr = (*LogicServerMgr)(nil)

func (m *LogicServerMgr) Range(fn func(nodeId string, member gatetrans.IMember) bool) {
	m.logicServerMap.Range(func(k any, v any) bool {
		ls := v.(*LogicServer)
		if ls == nil {
			return true
		}
		if atomic.LoadInt32(&ls.activeShardConnCount) == 0 {
			return true // 如果逻辑服没有活跃分片，则不返回该逻辑服。
		}
		return fn(ls.nodeId, ls)
	})
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

func (m *LogicServerMgr) addShardConn(nodeId string, shardIdx int, conn *ShardConn) {
	if shardIdx < 0 || shardIdx >= transport.BackendShardCnt {
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
	ls.updateActiveShardConnCount()
	ls.muConns.Unlock()
	kklog.Infof("逻辑服 shard 已挂接: nodeId=%s shardIdx=%d", nodeId, shardIdx)
}

func (m *LogicServerMgr) removeShardConn(nodeId string, shardIdx int) {
	if shardIdx < 0 || shardIdx >= transport.BackendShardCnt {
		return
	}
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return
	}
	ls.muConns.Lock()
	ls.conns[shardIdx] = nil
	ls.updateActiveShardConnCount()
	ls.muConns.Unlock()
}

func (m *LogicServerMgr) getShardConn(nodeId string, shardIdx int) (*ShardConn, error) {
	ls := m.getLogicServer(nodeId)
	if ls == nil {
		return nil, kkerrors.ErrAppLogicNodeNotRegistered
	}
	ls.muConns.RLock()
	conn := ls.conns[shardIdx]
	ls.muConns.RUnlock()
	if conn == nil {
		return nil, kkerrors.ErrAppLogicShardNotConnected
	}
	return conn, nil
}
