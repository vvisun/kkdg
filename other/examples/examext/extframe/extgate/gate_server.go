package extgate

import (
	"encoding/json"
	"math"
	"net"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/other/examples/examext/extframe/extmsg"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
	"github.com/vvisun/kkdg/utils/xnet"
)

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
		onNewConn(conn)
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

// 新连接建立事件处理
func onNewConn(conn net.Conn) {
	shardConn := &ShardConn{
		conn:     conn,
		connId:   nextConnID(),
		shardIdx: -1,
		nodeId:   "",
	}
	logicConnMgr.LoadOrStore(shardConn.connId, shardConn)
	// 每个逻辑服连接一个读携程。
	go logicReadLoop(shardConn)
}

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
	logicWriteThread = taskqueue.NewWorkerQueue(1)
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

	shardIdx := connId % transport.BackendShardCnt
	chooseServer.muConns.RLock()
	sconn := chooseServer.conns[shardIdx]
	chooseServer.muConns.RUnlock()
	if sconn == nil {
		return nil
	}
	return sconn.conn
}
