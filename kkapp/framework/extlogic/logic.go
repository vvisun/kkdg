package extlogic

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
	"github.com/vvisun/kkdg/utils/xnet"
)

// ====================== 配置 ======================
const (
	GatewayAddr     = "127.0.0.1:9981"
	BackendShardCnt = 8
	nodeId          = "logic_1"
	nodeType        = "logic"
)

// ====================== 主函数 ======================
func StartUp() {
	log.Println("=== 独立逻辑服启动 ===")
	var conns [BackendShardCnt]net.Conn

	for i := 0; i < BackendShardCnt; i++ {
		conns[i] = connectGateway(i)
	}

	for i := 0; i < BackendShardCnt; i++ {
		go businessLoop(i, conns[i])
	}

	select {}
}

// 重连网关
func connectGateway(idx int) net.Conn {
	for {
		conn, err := net.Dial("tcp", GatewayAddr)
		if err == nil {
			log.Printf("分流[%d] 连接网关成功", idx)
			xnet.SetNoDelay(conn, true)

			go func() {
				time.Sleep(100 * time.Millisecond)
				// 将自己注册到网关
				registerMsg := extmsg.RegisterMsg{
					ShardIdx: idx,
					NodeId:   nodeId,
					NodeType: nodeType,
				}
				registerBytes, _ := json.Marshal(registerMsg)
				down := extmsg.DownMsg{Cmd: extmsg.CmdRegister, Data: registerBytes}
				downBytes, _ := json.Marshal(down)
				_, _ = conn.Write(append(downBytes, '\n'))
			}()

			return conn
		}
		log.Printf("分流[%d] 连接失败，重试中", idx)
		time.Sleep(1 * time.Second)
	}
}

// 业务处理 + 断开事件清理
func businessLoop(idx int, conn net.Conn) {
	defer func() {
		_ = conn.Close()
		time.Sleep(1 * time.Second)
		businessLoop(idx, connectGateway(idx))
	}()

	dec := json.NewDecoder(conn)
	for {
		var msg extmsg.UpMsg
		if err := dec.Decode(&msg); err != nil {
			return
		}

		// ====================== 客户端断开事件 ======================
		if msg.Cmd == extmsg.CmdClientDisconnect {
			log.Printf("[逻辑服%d] 玩家断开 uid=%d connID=%d", idx, msg.Uid, msg.ConnID)
			// 在这里写：离线清理、存库、踢下线、房间退出等逻辑
			continue
		}

		// ====================== 正常业务逻辑 ======================
		resp := map[string]any{
			"logic_shard": idx,
			"uid":         msg.Uid,
			"conn_id":     msg.ConnID,
			"msg":         "逻辑服已接收",
			"data":        string(msg.Data),
		}
		respBytes, _ := json.Marshal(resp)

		down := extmsg.DownMsg{ConnID: msg.ConnID, Data: respBytes}
		sendBytes, _ := json.Marshal(down)
		_, _ = conn.Write(append(sendBytes, '\n'))
	}
}
