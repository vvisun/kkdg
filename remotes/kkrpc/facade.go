package kkrpc

import (
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
	Timeout time.Duration
}

func DefaultCallConfig() CallConfig {
	return CallConfig{
		Timeout: 1 * time.Second,
	}
}

func fixCallConfig(opts *CallConfig) {
	if opts == nil {
		return
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 1 * time.Second
	}
	if opts.Timeout < 100*time.Microsecond {
		opts.Timeout = 100 * time.Microsecond
	}
}

type IRpcClient interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
}

type IRpcServer interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
	GetConnManager() kknet.IConnManager
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
