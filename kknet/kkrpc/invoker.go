package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/utils/kklog"
)

type ClientInvoker[T any, R any] struct {
	cli IRpcClient
}

func (i ClientInvoker[T, R]) Invoke(ctx context.Context, method string, data *T, opts CallConfig) (*R, error) {
	bb, err := EncodeRpcFrame(FrameTypeRequest, genReqId(), method, data)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return nil, err
	}
	err = i.cli.SendBuffer(bb)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

//----------------------------------------------------------------

type RpcManager struct {
	peers   map[string]interface{}
	oneWays map[string]interface{}
}

func newRpcManager() *RpcManager {
	return &RpcManager{
		peers:   make(map[string]interface{}),
		oneWays: make(map[string]interface{}),
	}
}

//----------------------------------------------------------------

type RpcPeer[REQ any, RSP any] struct {
	method string
}

func (p *RpcPeer[REQ, RSP]) GetMethod() string {
	return p.method
}

func newRpcPeer[REQ any, RSP any](method string, manager *RpcManager) *RpcPeer[REQ, RSP] {
	p := &RpcPeer[REQ, RSP]{
		method: method,
	}
	manager.peers[method] = p
	return p
}

//----------------------------------------------------------------

type OneWay[REQ any] struct {
	method string
}

func (o *OneWay[REQ]) GetMethod() string {
	return o.method
}

func newOneWay[REQ any](method string, manager *RpcManager) *OneWay[REQ] {
	o := &OneWay[REQ]{
		method: method,
	}
	manager.oneWays[method] = o
	return o
}
