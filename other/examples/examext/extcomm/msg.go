package extcomm

import "encoding/json"

// ====================== 消息ID ======================

const (
	//客户端连接断开
	CmdClientDisconnect = "client_disconnect"
	//心跳
	CmdHeartbeat = "heartbeat"
	//心跳响应
	CmdHeartbeatAck = "heartbeat_ack"
)

// ====================== 消息结构 ======================

type UpMsg struct {
	ConnID uint64          `json:"connID"`        // 客户端连接ID(网关分配), 暂时当做sessionId用，后续需要优化，因为不同网关connId都从1开始，可能重复，缺乏唯一性。
	Uid    uint64          `json:"uid"`           //用户ID
	Data   json.RawMessage `json:"data"`          //消息数据
	Cmd    string          `json:"cmd,omitempty"` //消息ID
}

type DownMsg struct {
	ConnID uint64          `json:"connID"` // 客户端连接ID(网关分配), 暂时当做sessionId用，后续需要优化，因为不同网关connId都从1开始，可能重复，缺乏唯一性。
	Data   json.RawMessage `json:"data"`   //消息数据
}
