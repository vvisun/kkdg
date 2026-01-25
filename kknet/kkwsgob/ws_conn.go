package kkwsgob

import (
	"context"
	"encoding/binary"
	"io"
	"sync"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type wsConn struct {
	id    kknet.CONN_ID
	conn  gnet.Conn
	opts  kknet.Options
	stats *kknet.Stats
	state ws.State

	upgraded bool
	// client-side handshake validation
	expectedAccept string

	writeMu   sync.Mutex
	closeOnce sync.Once

	closeErrMu sync.Mutex
	closeErr   error

	fragOp  ws.OpCode
	fragBuf buffers.IBuffer

	ctxMu sync.RWMutex
	ctx   context.Context
}

var _ kknet.IConn = (*wsConn)(nil)

func newWSConn(c gnet.Conn, opts kknet.Options, stats *kknet.Stats, state ws.State) *wsConn {
	return &wsConn{
		id:    kknet.NextConnID(),
		conn:  c,
		opts:  opts,
		stats: stats,
		state: state,
		ctx:   context.Background(),
	}
}

func (c *wsConn) ID() kknet.CONN_ID {
	return c.id
}

func (c *wsConn) RemoteAddr() string {
	if c.conn == nil || c.conn.RemoteAddr() == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

func (c *wsConn) SendBuffer(buffer buffers.IBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidMessage
	}
	payload := buffer.B
	err := c.Send(payload)
	kkbuffer.Put(buffer)
	return err
}

func (c *wsConn) Send(data []byte) error {
	if len(data) == 0 {
		return kkerrors.ErrInvalidPacket
	}
	if len(data) > kkpacket.DefaultMaxMessageSize() {
		if c.stats != nil {
			c.stats.AddError()
		}
		return kkerrors.ErrMaxMessageSize
	}
	return c.writeFrame(ws.OpBinary, data)
}

func (c *wsConn) Close() error {
	c.closeOnce.Do(func() {
		c.setCloseErr(kkerrors.ErrConnectionClosed)
		payload := ws.NewCloseFrameBody(ws.StatusNormalClosure, "")
		if err := c.writeCloseFrame(payload); err != nil {
			_ = c.conn.Close()
		}
	})
	return nil
}

func (c *wsConn) Context() context.Context {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *wsConn) SetContext(ctx context.Context) {
	c.ctxMu.Lock()
	c.ctx = ctx
	c.ctxMu.Unlock()
}

func (c *wsConn) setCloseErr(err error) {
	if err == nil {
		return
	}
	c.closeErrMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeErrMu.Unlock()
}

func (c *wsConn) getCloseErr() error {
	c.closeErrMu.Lock()
	err := c.closeErr
	c.closeErrMu.Unlock()
	return err
}

func (c *wsConn) writeFrame(op ws.OpCode, payload []byte) error {
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

func (c *wsConn) writeFrameWithHeader(hdr ws.Header, payload []byte) error {
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

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

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

	// TODO: 发送失败应该入队，下次优先从队列中取数据发送。
	payloadLen := len(payload)
	if err := c.conn.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		kkbuffer.Put(bb)
		if err != nil {
			// 发送失败
			if c.stats != nil {
				c.stats.AddError()
			}
			return nil
		}
		if c.stats != nil {
			c.stats.AddSent(payloadLen)
		}
		return nil
	}); err != nil {
		// 入队失败
		kkbuffer.Put(bb)
		if c.stats != nil {
			c.stats.AddError()
		}
		return err
	}
	// 入队成功立即返回，注意这里只是入队，并非真正的发送数据
	return nil
}

func (c *wsConn) writeControl(op ws.OpCode, payload []byte) {
	_ = c.writeFrame(op, payload)
}

func (c *wsConn) writeCloseFrame(payload []byte) error {
	hdr := ws.Header{
		Fin:    true,
		OpCode: ws.OpClose,
		Masked: c.state.ClientSide(),
		Length: int64(len(payload)),
	}
	if hdr.Masked {
		hdr.Mask = ws.NewMask()
	}
	headerSize := ws.HeaderSize(hdr)
	if headerSize < 0 {
		return ws.ErrHeaderLengthUnexpected
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

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

	// TODO: 发送失败应该入队，下次优先从队列中取数据发送。
	if err := c.conn.AsyncWrite(bb.B, func(conn gnet.Conn, err error) error {
		kkbuffer.Put(bb)
		_ = conn.Close()
		return nil
	}); err != nil {
		kkbuffer.Put(bb)
		return err
	}
	return nil
}

func (c *wsConn) nextFrame() (ws.Header, []byte, bool, error) {
	n := c.conn.InboundBuffered()
	if n < 2 {
		return ws.Header{}, nil, false, nil
	}
	buf, err := c.conn.Peek(n)
	if err != nil {
		return ws.Header{}, nil, false, err
	}

	hdr, headerLen, ok, err := parseHeader(buf)
	if err != nil || !ok {
		return ws.Header{}, nil, false, err
	}
	if hdr.Length < 0 || hdr.Length > int64(kkpacket.DefaultMaxMessageSize()) {
		return ws.Header{}, nil, false, kkerrors.ErrMaxMessageSize
	}
	if err := ws.CheckHeader(hdr, c.state); err != nil {
		return ws.Header{}, nil, false, err
	}
	totalLen := headerLen + int(hdr.Length)
	if n < totalLen {
		return ws.Header{}, nil, false, nil
	}
	frameBytes, err := c.conn.Next(totalLen)
	if err != nil {
		return ws.Header{}, nil, false, err
	}
	payload := frameBytes[headerLen:totalLen]
	if hdr.Masked {
		ws.Cipher(payload, hdr.Mask, 0)
		hdr.Masked = false
	}
	return hdr, payload, true, nil
}

func (c *wsConn) appendFragment(op ws.OpCode, payload []byte) error {
	if c.fragBuf == nil {
		c.fragOp = op
		c.fragBuf = kkbuffer.GetWithCapacity(len(payload))
	}
	if len(c.fragBuf.B)+len(payload) > kkpacket.DefaultMaxMessageSize() {
		kkbuffer.Put(c.fragBuf)
		c.fragBuf = nil
		return kkerrors.ErrMaxMessageSize
	}
	c.fragBuf.B = append(c.fragBuf.B, payload...)
	return nil
}

func (c *wsConn) takeFragment() buffers.IBuffer {
	buf := c.fragBuf
	c.fragBuf = nil
	c.fragOp = 0
	return buf
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

func parseHeader(buf []byte) (ws.Header, int, bool, error) {
	if len(buf) < 2 {
		return ws.Header{}, 0, false, nil
	}

	b0 := buf[0]
	b1 := buf[1]
	hdr := ws.Header{
		Fin:    b0&0x80 != 0,
		Rsv:    (b0 & 0x70) >> 4,
		OpCode: ws.OpCode(b0 & 0x0f),
		Masked: b1&0x80 != 0,
	}

	length := int64(b1 & 0x7f)
	headerLen := 2

	switch {
	case length < 126:
		hdr.Length = length
	case length == 126:
		if len(buf) < 4 {
			return ws.Header{}, 0, false, nil
		}
		hdr.Length = int64(binary.BigEndian.Uint16(buf[2:4]))
		headerLen = 4
	case length == 127:
		if len(buf) < 10 {
			return ws.Header{}, 0, false, nil
		}
		if buf[2]&0x80 != 0 {
			return ws.Header{}, 0, false, ws.ErrHeaderLengthMSB
		}
		hdr.Length = int64(binary.BigEndian.Uint64(buf[2:10]))
		headerLen = 10
	default:
		return ws.Header{}, 0, false, ws.ErrHeaderLengthUnexpected
	}

	if hdr.Masked {
		if len(buf) < headerLen+4 {
			return ws.Header{}, 0, false, nil
		}
		copy(hdr.Mask[:], buf[headerLen:headerLen+4])
		headerLen += 4
	}

	return hdr, headerLen, true, nil
}

func errOrDefault(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
