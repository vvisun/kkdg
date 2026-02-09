package kkrpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

var req_id uint64 = 0

func genReqId() uint64 {
	return atomic.AddUint64(&req_id, 1)
}

type CallConfig struct {
	timeout time.Duration
}

type IRpcClient interface {
	SendBuffer(data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
	Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error)
	InvokeAsync(ctx context.Context, method string, data any, opts CallConfig) (any, error)
	InvokeNR(ctx context.Context, method string, data any, opts CallConfig) error
}

type IRpcServer interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
	Invoke(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) (any, error)
	InvokeAsync(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) (any, error)
	InvokeNR(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) error
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
