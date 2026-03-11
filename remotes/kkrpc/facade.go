package kkrpc

import (
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type ISender interface {
	// SendBuffer 发送数据
	//  如果Sender是Server，则connId为连接ID；
	//  如果Sender是Client，则connId会被忽略，因为会直接发送给Client所连接的Server
	//  @param connId 连接ID
	//  @param data 整包数据[length,message]
	//  @return error 错误
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	getPending() *pendingMap
}

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
	if opts.Timeout > 0 && opts.Timeout < 1*time.Millisecond {
		opts.Timeout = 1 * time.Millisecond
	}
}

type IRpcClient interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
	IsStopped() bool
}

type IRpcServer interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	Start() error
	Stop() error
	GetConnManager() kknet.IConnManager
}
