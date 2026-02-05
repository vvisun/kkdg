package kkrpc

type (
	// RpcRequest 请求消息，需要响应数据。响应数据格式为RpcResponse
	RpcRequest struct {
		ReqId  uint64 // 请求id(自动生成, 每个请求id唯一, 用于匹配请求和响应，收到ReqId为一致的RpcResponse表示请求成功)
		Method string // 方法名
		Data   []byte // 请求数据
	}
	// RpcResponse 响应消息。响应RpcRequest的请求。
	RpcResponse struct {
		ReqId   uint64 // 请求id(与RpcRequest的ReqId相同)
		Data    []byte // 响应数据
		ErrCode int32  // 错误码
		ErrMsg  string // 错误信息
	}

	// RpcTell 单点推送消息，不需要响应数据
	RpcTell struct {
		Method string // 方法名
		Data   []byte // 推送数据
	}
)

type (
	// TransMsg 网关转发消息
	TransMsg struct {
		MsgID    int64  // 消息id
		ClientId int64  // 客户端id(connID)
		Data     []byte // 转发数据
	}

	// TransBroadcast 网关转发群发消息
	TransBroadMsg struct {
		MsgID   int64   // 消息id
		Clients []int64 // 客户端id列表(connID列表)
		Data    []byte  // 转发数据
	}
)
