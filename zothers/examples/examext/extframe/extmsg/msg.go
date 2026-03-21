package extmsg

import "encoding/json"

// ====================== 消息ID ======================

const (
	//客户端连接断开
	CmdClientDisconnect = "client_disconnect"
	//心跳
	CmdHeartbeat = "heartbeat"
	//心跳响应
	CmdHeartbeatAck = "heartbeat_ack"
	//注册逻辑服信息
	CmdRegister = "register"
	//更新逻辑服信息
	CmdUpdate = "update"
)

// ====================== 消息结构 ======================

// 上行消息，网关转发客户端消息(Data)到逻辑服. 客户端 -> 【网关 -> 逻辑服】
type UpMsg struct {
	ConnID uint64          `json:"connID"`        // 客户端连接ID(网关分配), 暂时当做sessionId用，后续需要优化，因为不同网关connId都从1开始，可能重复，缺乏唯一性。
	Uid    uint64          `json:"uid"`           //用户ID
	Data   json.RawMessage `json:"data"`          //消息数据
	Cmd    string          `json:"cmd,omitempty"` //消息ID
}

// 下行消息，网关转发逻辑服消息(Data)到客户端. 【逻辑服 -> 网关】 -> 客户端
type DownMsg struct {
	Cmd    string          `json:"cmd"`    // 消息ID
	ConnID uint64          `json:"connID"` // 客户端连接ID(网关分配), 暂时当做sessionId用，后续需要优化，因为不同网关connId都从1开始，可能重复，缺乏唯一性。
	Data   json.RawMessage `json:"data"`   //消息数据
}

// 注册逻辑服信息，逻辑服注册到网关. 逻辑服 -> 网关
type RegisterMsg struct {
	ShardIdx int    `json:"shardIdx"` // 逻辑服分片索引
	NodeId   string `json:"nodeId"`   // 逻辑服节点ID
	NodeType string `json:"nodeType"` // 逻辑服节点类型
}
