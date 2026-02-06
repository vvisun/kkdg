package kkrpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var req_id uint64 = 0

func genReqId() uint64 {
	return atomic.AddUint64(&req_id, 1)
}

var (
	// rpc用的编码器
	rpcCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	// rpc消息里的Data字段编码器
	dataCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)

	// 网关消息使用的编码器. 需和客户端约定好编码器类型
	// gatewayCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)
)

type CallConfig struct {
	timeout time.Duration
}

type IRpcClient interface {
	SendBuffer(data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
}

type IRpcServer interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
}

// Invoker is a small abstraction over "something that can invoke a method".
//
// Typical implementations:
// - ClientInvoker: client -> server (address-based)
// - ConnInvoker: server -> client (connection-based)
type Invoker interface {
	/**同步调用方法。with Return Value
	@param ctx context.Context 上下文
	@param method string 方法名
	@param data any 方法参数
	@param opts CallConfig 调用配置
	@return any 返回数据
	@return error 错误
	*/
	Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error)
	/**异步调用方法。without Return Value
	@param ctx context.Context 上下文
	@param method string 方法名
	@param data any 方法参数
	@param opts CallConfig 调用配置
	@return error 错误
	*/
	InvokeNoResponse(ctx context.Context, method string, data any, opts CallConfig) error
}

type IGatewayTransport interface {
	/**发送消息到指定客户端
	@param clientId kknet.CONN_ID 客户端ID
	@param msg any 消息
	@return error 错误
	*/
	SendMsg(clientId kknet.CONN_ID, msg any) error
	/**广播消息到指定客户端列表
	@param clientIds []kknet.CONN_ID 客户端ID列表
	@param msg any 消息
	@return error 错误
	*/
	BroadcastMsg(clientIds []kknet.CONN_ID, msg any) error
}
