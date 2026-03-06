package ptotrans

type (
	// 逻辑服注册到网关: 逻辑服->网关->逻辑服
	RpcMsgRegister struct {
		NodeId   string
		NodeType string
	}

	// 网关转发消息到客户端: 逻辑服->网关->客户端
	RpcS2Client struct {
		ClientId string
		Payload  []byte
	}

	// 网关转发消息到多个客户端: 逻辑服->网关->多个客户端
	RpcS2Clients struct {
		ClientIds []string
		Payload   []byte
	}

	// 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
	RpcC2S struct {
		ClientId   string
		GateNodeId string
		Payload    []byte
	}
)
