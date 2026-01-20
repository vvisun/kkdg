package kkpacket

import (
	"errors"
	"io"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IStreamReader provides buffered stream access for unpacking.
type IStreamReader interface {
	InboundBuffered() int
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
	Next(n int) (buf []byte, err error)
}

type IStreamPacket interface {
	// pack message to stream
	// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
	Pack(data []byte, maxSize int) (buffers.IBuffer, error)
	// unpack message from stream
	Unpack(r IStreamReader, maxSize int) (data []byte, ok bool, err error)
}

// LengthFieldStreamPacket packs and unpacks 4-byte length-prefixed frames.
type LengthFieldStreamPacket struct {
	msgPacket *packer
}

var _ IStreamPacket = (*LengthFieldStreamPacket)(nil)

// NewLengthFieldStreamPacket creates a length-field stream packet.
func NewLengthFieldStreamPacket(msgPacket *packer) *LengthFieldStreamPacket {
	return &LengthFieldStreamPacket{msgPacket: msgPacket}
}

// Pack implements IStreamPacket.
// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
func (slf *LengthFieldStreamPacket) Pack(data []byte, maxSize int) (buffers.IBuffer, error) {
	if len(data) > maxSize {
		return nil, kkerrors.ErrMaxMessageSize
	}

	dataLen := len(data)
	bb := kkbuffer.GetWithCapacity(4 + dataLen)
	bb.B = bb.B[:4+dataLen]
	GetByteOrder().PutUint32(bb.B[:4], uint32(dataLen))
	copy(bb.B[4:], data)

	return bb, nil
}

// Unpack implements IStreamPacket.
func (slf *LengthFieldStreamPacket) Unpack(r IStreamReader, maxSize int) ([]byte, bool, error) {
	if r.InboundBuffered() < 4 {
		return nil, false, nil
	}
	header, err := r.Peek(4)
	if err != nil {
		if errors.Is(err, io.ErrShortBuffer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	size := int(GetByteOrder().Uint32(header))
	if size < 0 || size > maxSize {
		return nil, false, kkerrors.ErrMaxMessageSize
	}
	if r.InboundBuffered() < 4+size {
		return nil, false, nil
	}
	_, _ = r.Discard(4)
	data, err := r.Next(size)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}
