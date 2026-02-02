package kkmcmp

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

/**
 * 消息处理器-发送器。多个连接共享一个发送器。
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 */
type WriteProcessor struct {
	conns sync.Map // map[kknet.CONN_ID]kknet.IConn //连接ID -> 连接
}

func NewWriteProcessor() *WriteProcessor {
	return &WriteProcessor{
		conns: sync.Map{},
	}
}

func (wp *WriteProcessor) Start() {
}

func (wp *WriteProcessor) Stop(connId kknet.CONN_ID, err error) {
}

func (wp *WriteProcessor) SendBuffer(connId kknet.CONN_ID, buffer buffers.IBuffer) error {
	if buffer == nil {
		return nil
	}
	_, ok := wp.conns.Load(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}

func (wp *WriteProcessor) SendMessage(connId kknet.CONN_ID, msg any) error {
	if msg == nil {
		return nil
	}
	_, ok := wp.conns.Load(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}

func (wp *WriteProcessor) AddConn(conn kknet.IConn) {
	if conn == nil {
		return
	}
	wp.conns.Store(conn.ID(), conn)
}

func (wp *WriteProcessor) RemoveConn(connID kknet.CONN_ID) {
	wp.conns.Delete(connID)
}

func (wp *WriteProcessor) GetConn(connID kknet.CONN_ID) kknet.IConn {
	v, ok := wp.conns.Load(connID)
	if !ok {
		return nil
	}
	return v.(kknet.IConn)
}
