package kkmcmp

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

/**
 * 消息处理器-接收器。多个连接共享一个接收器。
 */
type ReadProcessor struct {
	conns sync.Map // map[kknet.CONN_ID]kknet.IConn //连接ID -> 连接
}

func NewReadProcessor() *ReadProcessor {
	return &ReadProcessor{
		conns: sync.Map{},
	}
}

func (rp *ReadProcessor) Start() {

}

func (rp *ReadProcessor) Stop() {

}

func (rp *ReadProcessor) EnqueuePacket(connId kknet.CONN_ID, packet []byte) {

}

// 收到数据时（生产者生产数据）
func (rp *ReadProcessor) OnRecvBytes(connId kknet.CONN_ID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, ok := rp.conns.Load(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}

func (rp *ReadProcessor) AddConn(conn kknet.IConn) {
	if conn == nil {
		return
	}
	rp.conns.Store(conn.ID(), conn)
}

func (rp *ReadProcessor) RemoveConn(connID kknet.CONN_ID) {
	rp.conns.Delete(connID)
}

func (rp *ReadProcessor) GetConn(connID kknet.CONN_ID) kknet.IConn {
	v, ok := rp.conns.Load(connID)
	if !ok {
		return nil
	}
	return v.(kknet.IConn)
}
