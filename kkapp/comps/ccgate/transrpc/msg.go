package transrpc

type (
	// 逻辑服注册到网关
	RpcMsgRegister struct {
		NodeId   string
		NodeType string
	}

	// 网关转发消息到客户端
	RpcS2Client struct {
		clientId string
		payload  []byte
	}

	// 网关转发消息到多个客户端
	RpcS2Clients struct {
		clientIds []string
		payload   []byte
	}

	// 网关转发客户端消息到逻辑服
	RpcC2S struct {
		clientId string
		payload  []byte
	}
)
