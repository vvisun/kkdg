package kkprocessor

import (
	"errors"
	"io"
	"net"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type RecvMessage struct {
	connId kknet.CONN_ID
	packet *kkbuffer.ByteBuffer
}

//-------------------------------- channel --------------------------------

type channel chan struct{}

func (c channel) add() { c <- struct{}{} }

func (c channel) done() { <-c }

func (c channel) Go(m *RecvMessage, f func(*RecvMessage) error) error {
	c.add()
	go func() {
		_ = f(m)
		c.done()
	}()
	return nil
}

//-------------------------------- write processor utils --------------------------------

// defaultIsWriteFnRetryable 默认判断：连接已关闭等致命错误不重试。
func defaultIsWriteFnRetryable(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, kkerrors.ErrNetConnectionClosed) ||
		errors.Is(err, kkerrors.ErrClusterInvalidPacket) ||
		errors.Is(err, kkerrors.ErrNetSendQueueFull) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe) {
		return false
	}
	// 其他错误（如临时 EAGAIN、超时等）允许重试
	return true
}
