package kkrpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
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
	SendBuffer(data buffers.IBuffer) error
}

type IRpcServer interface {
	SendBuffer(connId kknet.CONN_ID, data buffers.IBuffer) error
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

// ------------------------- rpc Client -------------------------

// ClientInvoker adapts *Client to Invoker.
type ClientInvoker struct {
	rpcClient IRpcClient
}

var _ Invoker = (*ClientInvoker)(nil)
var _ IGatewayTransport = (*ClientInvoker)(nil)

func (i *ClientInvoker) Init(cli IRpcClient) error {
	i.rpcClient = cli
	return nil
}

func (i *ClientInvoker) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	if i.rpcClient == nil {
		return nil, ErrClientNotConnected
	}
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return nil, err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeRequest,
		ID: genReqId(),
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return nil, err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return nil, err
	}
	// send buffer
	return nil, i.rpcClient.SendBuffer(bb)
}

func (i *ClientInvoker) InvokeNoResponse(ctx context.Context, method string, data any, opts CallConfig) error {
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeTell,
		ID: 0,
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return err
	}
	// send buffer
	return i.rpcClient.SendBuffer(bb)
}

func (i *ClientInvoker) SendMsg(clientId kknet.CONN_ID, msg any) error {
	if i.rpcClient == nil {
		return ErrClientNotConnected
	}

	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), nil)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransMsg{
		ClientId: clientId,
		Data:     msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransMsg,
		ID: 0,
		M:  "TransMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcClient.SendBuffer(stream)
}

func (i *ClientInvoker) BroadcastMsg(clientIds []kknet.CONN_ID, msg any) error {
	if i.rpcClient == nil {
		return ErrClientNotConnected
	}
	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), nil)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransBroadMsg{
		Clients: clientIds,
		Data:    msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransBroadMsg,
		ID: 0,
		M:  "TransBroadMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcClient.SendBuffer(stream)
}

// ------------------------- rpc Server -------------------------

// ConnInvoker adapts (Server + ConnID) to Invoker, enabling server-initiated calls to a connected peer.
type ConnInvoker struct {
	rpcServer IRpcServer
	connId    kknet.CONN_ID
}

var _ Invoker = (*ConnInvoker)(nil)
var _ IGatewayTransport = (*ConnInvoker)(nil)

func (i *ConnInvoker) Init(server IRpcServer, connId kknet.CONN_ID) error {
	i.rpcServer = server
	i.connId = connId
	return nil
}

func (i *ConnInvoker) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	if i.rpcServer == nil {
		return nil, ErrServerNotStarted
	}
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return nil, err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeRequest,
		ID: genReqId(),
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return nil, err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return nil, err
	}
	// send buffer
	return nil, i.rpcServer.SendBuffer(i.connId, bb)
}

func (i *ConnInvoker) InvokeNoResponse(ctx context.Context, method string, data any, opts CallConfig) error {
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeTell,
		ID: 0,
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return err
	}
	// send buffer
	return i.rpcServer.SendBuffer(i.connId, bb)
}

func (i *ConnInvoker) SendMsg(clientId kknet.CONN_ID, msg any) error {
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}

	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), nil)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransMsg{
		ClientId: clientId,
		Data:     msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransMsg,
		ID: 0,
		M:  "TransMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcServer.SendBuffer(i.connId, stream)
}

func (i *ConnInvoker) BroadcastMsg(clientIds []kknet.CONN_ID, msg any) error {
	if len(clientIds) == 0 {
		return nil
	}
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}

	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), nil)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransBroadMsg{
		Clients: clientIds,
		Data:    msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransBroadMsg,
		ID: 0,
		M:  "TransBroadMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcServer.SendBuffer(i.connId, stream)
}
