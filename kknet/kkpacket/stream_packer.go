package kkpacket

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IStreamPacker packs messages into a stream.
type IStreamPacker interface {
	// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
	Pack(data []byte) (buffers.IBuffer, error)
}

// LengthFieldPacker packs 4-byte length-prefixed frames.
type LengthFieldPacker struct {
	maxSize   int
	msgPacket *packer
}

// NewLengthFieldPacker creates a length-field packer.
func NewLengthFieldPacker(maxSize int, msgPacket *packer) *LengthFieldPacker {
	return &LengthFieldPacker{maxSize: maxSize, msgPacket: msgPacket}
}

// Pack implements IStreamPacker.
// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
func (p *LengthFieldPacker) Pack(data []byte) (buffers.IBuffer, error) {
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

func (p *LengthFieldPacker) GetMaxSize() int {
	return p.maxSize
}
