package transport

// 传输层类型。用于网关与业务服之间的消息转发。
type TransType = string

const (
	TransTypeNats  TransType = "nats"  // 使用nats集群转发消息
	TransTypeRpc   TransType = "rpc"   // 使用rpc转发消息
	TransTypeShard TransType = "shard" // 使用shard转发消息
)

// 分片数。shard模式使用。
const BackendShardCnt = 8

// 网关与业务服之间的消息转发函数名。nats模式使用
const (
	// 客户端->网关->业务服的消息转发函数名
	FuncNameC2S = "c2s"
	// 业务服->网关->客户端的消息转发函数名
	FuncNameSendToClient = "1"
	// 业务服->网关->多个客户端的消息转发函数名
	FuncNameSendToClients = "N"
	// 网关 -> 业务服：客户端断开事件
	FuncNameClientDisconnect = "cliMiss"
	// 网关 -> 业务服：分配客户端到本业务服
	FuncNameAllocClient = "allocClient"
)
