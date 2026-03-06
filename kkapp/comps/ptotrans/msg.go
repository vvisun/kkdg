package ptotrans

type (
	// 逻辑服注册到网关
	RpcMsgRegister struct {
		NodeId   string
		NodeType string
	}

	// 网关转发消息到客户端
	RpcS2Client struct {
		ClientId string
		Payload  []byte
	}

	// 网关转发消息到多个客户端
	RpcS2Clients struct {
		ClientIds []string
		Payload   []byte
	}

	// 网关转发客户端消息到逻辑服
	RpcC2S struct {
		ClientId string
		Payload  []byte
	}
)
