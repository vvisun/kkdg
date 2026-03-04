package transrpc

type (
	RpcMsgRegister struct {
		NodeId   string
		NodeType string
	}

	RpcS2C struct {
		clientId string
		payload  []byte
	}
)
