package kkmcmp

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

/**
 * 消息处理器-发送器。多个连接共享一个发送器。
 * 负责编码、然后将编码后的数据投入发送队列，供连接发送。
 */
type WriteProcessor struct {
	connMgr *ConnMgr
}

func NewWriteProcessor() *WriteProcessor {
	return &WriteProcessor{
		connMgr: newConnMgr(),
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
	_, ok := wp.connMgr.GetConn(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}

func (wp *WriteProcessor) SendMessage(connId kknet.CONN_ID, msg any) error {
	if msg == nil {
		return nil
	}
	_, ok := wp.connMgr.GetConn(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}
