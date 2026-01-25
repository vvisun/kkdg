package kkwstls

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type netWSConn struct {
	id     kknet.CONN_ID
	conn   net.Conn
	reader *bufio.Reader
	opts   kknet.Options
	stats  *kknet.Stats
	state  ws.State

	upgraded bool

	writeCh   chan *writeTask
	writeDone chan struct{}
	closeOnce sync.Once

	closeErrMu sync.Mutex
	closeErr   error

	fragOp  ws.OpCode
	fragBuf buffers.IBuffer

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*netWSConn)(nil)

func newNetWSConn(conn net.Conn, reader *bufio.Reader, opts kknet.Options, stats *kknet.Stats, state ws.State) *netWSConn {
	if reader == nil {
		reader = bufio.NewReader(conn)
	}
	c := &netWSConn{
		id:     kknet.NextConnID(),
		conn:   conn,
		reader: reader,
		opts:   opts,
		stats:  stats,
		state:  state,
		ctx:    context.Background(),
	}
	queueSize := opts.TcpClientSendQueueSize
	if queueSize <= 0 {
		queueSize = 256
	}
	c.writeCh = make(chan *writeTask, queueSize)
	c.writeDone = make(chan struct{})
	go c.writeLoop()
	return c
}

func (c *netWSConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *netWSConn) RemoteAddr() string {
	if c.conn == nil || c.conn.RemoteAddr() == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

func (c *netWSConn) SendBuffer(buffer buffers.IBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidMessage
	}
	payload := buffer.B
	err := c.Send(payload)
	kkbuffer.Put(buffer)
	return err
}

func (c *netWSConn) Send(data []byte) error {
	if len(data) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	return c.writeFrame(ws.OpBinary, data)
}

func (c *netWSConn) Close() error {
	c.setCloseErr(kkerrors.ErrConnectionClosed)
	c.closeOnce.Do(func() {
		close(c.writeCh)
		_ = c.conn.Close()
	})
	<-c.writeDone
	return nil
}

func (c *netWSConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *netWSConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *netWSConn) setCloseErr(err error) {
	if err == nil {
		return
	}
	c.closeErrMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeErrMu.Unlock()
}

func (c *netWSConn) getCloseErr() error {
	c.closeErrMu.Lock()
	err := c.closeErr
	c.closeErrMu.Unlock()
	return err
}

func (c *netWSConn) writeFrame(op ws.OpCode, payload []byte) error {
	mask := c.state.ClientSide()
	hdr := ws.Header{
		Fin:    true,
		OpCode: op,
		Masked: mask,
		Length: int64(len(payload)),
	}
	if mask {
		hdr.Mask = ws.NewMask()
	}
	return c.writeFrameWithHeader(hdr, payload)
}

func (c *netWSConn) writeFrameWithHeader(hdr ws.Header, payload []byte) error {
	if len(payload) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	headerSize := ws.HeaderSize(hdr)
	if headerSize < 0 {
		return ws.ErrHeaderLengthUnexpected
	}

	bb := kkbuffer.GetWithCapacity(headerSize + len(payload))
	bb.B = bb.B[:headerSize+len(payload)]

	writer := sliceWriter{b: bb.B}
	if err := ws.WriteHeader(&writer, hdr); err != nil {
		kkbuffer.Put(bb)
		return err
	}
	copy(bb.B[writer.n:], payload)
	if hdr.Masked {
		ws.Cipher(bb.B[writer.n:], hdr.Mask, 0)
	}

	if !c.enqueueWrite(bb, len(payload)) {
		kkbuffer.Put(bb)
		return kkerrors.ErrConnectionClosed
	}
	return nil
}

func (c *netWSConn) writeControl(op ws.OpCode, payload []byte) {
	_ = c.writeFrame(op, payload)
}

func (c *netWSConn) appendFragment(op ws.OpCode, payload buffers.IBuffer) error {
	if c.fragBuf == nil {
		c.fragOp = op
		c.fragBuf = payload
		return nil
	}
	if len(c.fragBuf.B)+len(payload.B) > kkpacket.DefaultMaxMessageSize() {
		kkbuffer.Put(c.fragBuf)
		kkbuffer.Put(payload)
		c.fragBuf = nil
		return kkerrors.ErrMaxMessageSize
	}
	c.fragBuf.B = append(c.fragBuf.B, payload.B...)
	kkbuffer.Put(payload)
	return nil
}

func (c *netWSConn) takeFragment() buffers.IBuffer {
	buf := c.fragBuf
	c.fragBuf = nil
	c.fragOp = 0
	return buf
}

func (c *netWSConn) readLoop(handleMessage func(buffers.IBuffer)) error {
	for {
		if c.opts.WsReadTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.WsReadTimeout))
		}
		hdr, err := ws.ReadHeader(c.reader)
		if err != nil {
			return err
		}
		if hdr.Length < 0 || hdr.Length > int64(kkpacket.DefaultMaxMessageSize()) {
			return kkerrors.ErrMaxMessageSize
		}
		if err := ws.CheckHeader(hdr, c.state); err != nil {
			return err
		}

		payload := kkbuffer.GetWithCapacity(int(hdr.Length))
		payload.B = payload.B[:int(hdr.Length)]
		if _, err := io.ReadFull(c.reader, payload.B); err != nil {
			kkbuffer.Put(payload)
			return err
		}
		if hdr.Masked {
			ws.Cipher(payload.B, hdr.Mask, 0)
		}

		if hdr.OpCode.IsControl() {
			switch hdr.OpCode {
			case ws.OpPing:
				c.writeControl(ws.OpPong, payload.B)
			case ws.OpPong:
			case ws.OpClose:
				c.writeControl(ws.OpClose, payload.B)
				kkbuffer.Put(payload)
				return io.EOF
			default:
				kkbuffer.Put(payload)
				return ws.ErrProtocolOpCodeReserved
			}
			kkbuffer.Put(payload)
			continue
		}

		switch hdr.OpCode {
		case ws.OpText, ws.OpBinary:
			if !hdr.Fin {
				if err := c.appendFragment(hdr.OpCode, payload); err != nil {
					return err
				}
				continue
			}
			if c.stats != nil {
				c.stats.AddRecv(len(payload.B))
			}
			handleMessage(payload)
		case ws.OpContinuation:
			if c.fragBuf == nil {
				kkbuffer.Put(payload)
				return ws.ErrProtocolContinuationUnexpected
			}
			if err := c.appendFragment(c.fragOp, payload); err != nil {
				return err
			}
			if hdr.Fin {
				msg := c.takeFragment()
				if c.stats != nil {
					c.stats.AddRecv(len(msg.B))
				}
				handleMessage(msg)
			}
		default:
			kkbuffer.Put(payload)
			return ws.ErrProtocolOpCodeReserved
		}
	}
}

func (c *netWSConn) enqueueWrite(bb *kkbuffer.ByteBuffer, payloadLen int) bool {
	task := &writeTask{bb: bb, payloadLen: payloadLen}
	select {
	case c.writeCh <- task:
		return true
	case <-c.writeDone:
		return false
	}
}

func (c *netWSConn) writeLoop() {
	defer close(c.writeDone)
	for task := range c.writeCh {
		bb := task.bb
		if c.opts.WsWriteTimeout > 0 {
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout))
		}
		n, err := c.conn.Write(bb.B)
		if err != nil {
			c.setCloseErr(err)
			if c.stats != nil {
				c.stats.AddError()
			}
			kkbuffer.Put(bb)
			continue
		}
		if n != len(bb.B) {
			c.setCloseErr(io.ErrShortWrite)
			if c.stats != nil {
				c.stats.AddError()
			}
			kkbuffer.Put(bb)
			continue
		}
		if c.stats != nil {
			c.stats.AddSent(task.payloadLen)
		}
		kkbuffer.Put(bb)
	}
}

type writeTask struct {
	bb         *kkbuffer.ByteBuffer
	payloadLen int
}

type sliceWriter struct {
	b []byte
	n int
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	if len(w.b)-w.n < len(p) {
		return 0, io.ErrShortBuffer
	}
	copy(w.b[w.n:], p)
	w.n += len(p)
	return len(p), nil
}
