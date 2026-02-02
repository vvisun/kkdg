package kkmcmp

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

/**
 * 消息处理器-接收器。多个连接共享一个接收器。
 */
type ReadProcessor struct {
	connMgr *ConnMgr
}

func NewReadProcessor() *ReadProcessor {
	return &ReadProcessor{
		connMgr: newConnMgr(),
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
	_, ok := rp.connMgr.GetConn(connId)
	if !ok {
		return kkerrors.ErrConnNotInThisProcessor
	}
	return nil
}
