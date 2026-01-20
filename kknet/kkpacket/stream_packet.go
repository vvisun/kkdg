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
	Pack(data []byte) (buffers.IBuffer, error)
	// unpack message from stream
	Unpack(r IStreamReader) (data []byte, ok bool, err error)
	// get max size
	GetMaxSize() int
	// set max size
	SetMaxSize(maxSize int)
}

// LengthFieldStreamPacket packs and unpacks 4-byte length-prefixed frames.
type LengthFieldStreamPacket struct {
	maxSize   int
	msgPacket *packer
}

// NewLengthFieldStreamPacket creates a length-field stream packet.
func NewLengthFieldStreamPacket(maxSize int, msgPacket *packer) *LengthFieldStreamPacket {
	return &LengthFieldStreamPacket{maxSize: maxSize, msgPacket: msgPacket}
}

// Pack implements IStreamPacket.
// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
func (p *LengthFieldStreamPacket) Pack(data []byte) (buffers.IBuffer, error) {
	if len(data) > p.maxSize {
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
func (u *LengthFieldStreamPacket) Unpack(r IStreamReader) ([]byte, bool, error) {
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
	if size < 0 || size > u.maxSize {
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

func (p *LengthFieldStreamPacket) GetMaxSize() int {
	return p.maxSize
}

func (p *LengthFieldStreamPacket) SetMaxSize(maxSize int) {
	p.maxSize = maxSize
}
