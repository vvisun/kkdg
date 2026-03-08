package extgate

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/xnet"
)

var (
	// 逻辑服列表
	logicBackends []*LogicServer
	logicMu       sync.RWMutex

	// 逻辑服连接列表，当逻辑服连接满8条时，组成一个LogicServer。
	pendingConns []net.Conn
	pendingMu    sync.Mutex
)

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

// ====================== 网关TCP监听逻辑服 ======================
// 逻辑服主动向网关建立 BackendShards 条连接；网关收集满 8 条后组成一个 LogicServer。
func startGatewayTCPListener() {
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

		AddLogicServer(ls)

		for i := 0; i < BackendShardCnt; i++ {
			go logicReadLoop(ls.conns[i])
		}
	}
}

// 每个逻辑服连接一个读携程。
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

var (
	transThread = kkprocessor.NewWorkerQueue(1)
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

	transThread.Push(func() {
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
//	@param msg 客户端消息
//	@param uid 用户ID
//	@param cmd 命令
func transToLogicNoCopy(connId uint64, msg *gws.Message, uid uint64, cmd string) {
	conn := routeLogicConn(connId)
	if conn == nil {
		msg.Close()
		return
	}
	dataCpy := msg.Bytes()
	transThread.Push(func() {
		bs, _ := json.Marshal(extmsg.UpMsg{
			ConnID: connId,
			Uid:    uid,
			Data:   dataCpy,
			Cmd:    cmd,
		})
		_, _ = conn.Write(append(bs, '\n'))
		msg.Close()
	})
}

// 将客户端消息转发到逻辑服。
//
//	@param connId 客户端连接ID
//	@return 逻辑服连接
func routeLogicConn(connId uint64) net.Conn {
	logicMu.RLock()
	defer logicMu.RUnlock()
	if len(logicBackends) == 0 {
		return nil
	}
	srvIdx := connId % uint64(len(logicBackends))
	shardIdx := connId % BackendShardCnt
	return logicBackends[srvIdx].conns[shardIdx]
}
