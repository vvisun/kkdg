package ptotrans

type (
	// msgID: 1
	// 逻辑服注册到网关: 逻辑服->网关->逻辑服
	RpcMsgRegister struct {
		ShardIdx int
		NodeId   string
		NodeType string
	}

	// msgID: 2
	// 网关转发消息到客户端: 逻辑服->网关->客户端
	RpcS2Client struct {
		ClientId string
		Payload  []byte
	}

	// msgID: 3
	// 网关转发消息到多个客户端: 逻辑服->网关->多个客户端
	RpcS2Clients struct {
		ClientIds []string
		Payload   []byte
	}

	// msgID: 4
	// 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
	RpcC2S struct {
		ClientId   string //sessionID
		GateNodeId string //网关节点ID
		Payload    []byte
	}

	// msgID: 5
	// 网关 -> 逻辑服：客户端断开事件
	RpcClientDisconnect struct {
		ClientId  string   //sessionID
		ClientIds []string //sessionID列表
	}

	// msgID: 6
	// 网关 -> 逻辑服：分配客户端到本逻辑服
	RpcAllocClient struct {
		ClientId string //sessionID
	}
)
