package kkrpc

import (
	"reflect"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type RpcSendFunc[T any] func(msg *T, reqId uint64) (*kkbuffer.ByteBuffer, error)

type ISendFunc interface {
	GetMsgID() any
}

type RpcSender struct {
	m        map[interface{}]ISendFunc
	tpMsgMap map[reflect.Type]interface{}
}

func (s *RpcSender) GetMsgID(msg interface{}) any {
	tp := reflect.TypeOf(msg)
	if tp == nil {
		return nil
	}
	return s.tpMsgMap[tp]
}

func Send[T any](sender *RpcSender, msg *T, reqId uint64) (*kkbuffer.ByteBuffer, error) {
	var v T
	method := sender.GetMsgID(&v)
	h, ok := sender.m[method]
	if !ok || h == nil {
		return nil, ErrMethodNotFound
	}
	return h.(*SendFunc[T]).Call(msg, reqId)
}

func RegistSendFunc[T any](router *RpcSender, method string, msg *T) {
	call := func(msg *T, reqId uint64) (*kkbuffer.ByteBuffer, error) {
		dataBytes, err := dataCodec.Marshal(msg)
		if err != nil {
			return nil, err
		}
		bb, err := EncodeRpcFrame(FrameTypeRequest, reqId, method, dataBytes)
		if err != nil {
			kkbuffer.Put(bb)
			return nil, err
		}
		return bb, nil
	}
	h := newSendFunc(method, call)
	router.m[method] = h
}

type SendFunc[T any] struct {
	method any
	call   RpcSendFunc[T]
}

func (s *SendFunc[T]) GetMsgID() any {
	return s.method
}

func (s *SendFunc[T]) Call(msg *T, reqId uint64) (*kkbuffer.ByteBuffer, error) {
	bb, err := s.call(msg, reqId)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

func newSendFunc[T any](method any, call RpcSendFunc[T]) ISendFunc {
	return &SendFunc[T]{
		method: method,
		call:   call,
	}
}
