package main

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

// ====================== 配置 ======================
const (
	GatewayAddr   = "127.0.0.1:9981"
	BackendShards = 8
)

// ====================== 消息结构 ======================
type UpMsg struct {
	ConnID uint64          `json:"connID"`
	Uid    uint64          `json:"uid"`
	Data   json.RawMessage `json:"data"`
	Cmd    string          `json:"cmd,omitempty"`
}

type DownMsg struct {
	ConnID uint64          `json:"connID"`
	Data   json.RawMessage `json:"data"`
}

// ====================== 主函数 ======================
func main() {
	log.Println("=== 独立逻辑服启动 ===")
	var conns [BackendShards]net.Conn

	for i := 0; i < BackendShards; i++ {
		conns[i] = connectGateway(i)
	}

	for i := 0; i < BackendShards; i++ {
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
		var msg UpMsg
		if err := dec.Decode(&msg); err != nil {
			return
		}

		// ====================== 客户端断开事件 ======================
		if msg.Cmd == "client_disconnect" {
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

		down := DownMsg{ConnID: msg.ConnID, Data: respBytes}
		sendBytes, _ := json.Marshal(down)
		_, _ = conn.Write(append(sendBytes, '\n'))
	}
}
