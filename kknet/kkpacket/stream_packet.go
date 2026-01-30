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

// 消息结构：[length,message]。
// length表示message的长度，占4个字节。
// message是消息对象的二进制数据，可以通过message_parser解析为消息对象。
type IStreamPacket interface {
	// get length field byte count. [length].
	LengthFieldByteCount() int

	// get message packet.
	GetMessagePacket() *PacketCodec

	// get byte count of message.
	GetBodySize(data []byte) (int, error)

	// write byte count of message to data.
	writeBodySize(data []byte, size int)

	// check packet is valid.
	// input: [length,message].
	// output: error
	CheckPacket(packet []byte) error

	// check packet is valid.
	// input: [length,message].
	// output: error
	CheckPacketBuffer(buffer buffers.IBuffer) error

	// pack message to stream.
	// input: [message].
	// output: [length,message].
	// @ return [length,message], err
	// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
	Pack(data []byte) (buffers.IBuffer, error)

	// unpack message from stream.
	// input: [length,message].
	// output: [message].
	// @ return [message], ok, err
	Unpack(data []byte) ([]byte, error)

	// 粘包拆包。return [length,message], ok, err
	UnpackFromSR(r IStreamReader) ([]byte, bool, error)
}

// LengthFieldStreamPacket packs and unpacks 4-byte length-prefixed frames.
type LengthFieldStreamPacket struct {
	lengthFieldByteCount int
	msgPacket            *PacketCodec
}

var _ IStreamPacket = (*LengthFieldStreamPacket)(nil)

// NewLengthFieldStreamPacket creates a length-field stream packet.
func NewLengthFieldStreamPacket(msgPacket *PacketCodec) *LengthFieldStreamPacket {
	return &LengthFieldStreamPacket{lengthFieldByteCount: 4, msgPacket: msgPacket}
}

// get message packet.
func (slf *LengthFieldStreamPacket) GetMessagePacket() *PacketCodec {
	return slf.msgPacket
}

// get length field byte count.
func (slf *LengthFieldStreamPacket) LengthFieldByteCount() int {
	return slf.lengthFieldByteCount
}

// byte count of message.
func (slf *LengthFieldStreamPacket) GetBodySize(data []byte) (int, error) {
	if len(data) < slf.lengthFieldByteCount {
		return 0, kkerrors.ErrDataTooShortToDecode
	}
	switch slf.lengthFieldByteCount {
	case 4:
		return int(GetByteOrder().Uint32(data)), nil
	case 2:
		return int(GetByteOrder().Uint16(data)), nil
	default:
		return 0, kkerrors.ErrInvalidLengthFieldByteCount
	}
}

func (slf *LengthFieldStreamPacket) writeBodySize(data []byte, size int) {
	switch slf.lengthFieldByteCount {
	case 4:
		GetByteOrder().PutUint32(data[:4], uint32(size))
	case 2:
		GetByteOrder().PutUint16(data[:2], uint16(size))
	}
}

// CheckPacket checks if the packet is valid.
// input: [length,data].
// output: error
func (slf *LengthFieldStreamPacket) CheckPacket(packet []byte) error {
	if len(packet) == 0 {
		return kkerrors.ErrInvalidPacket
	}
	lenPacket := len(packet)
	if lenPacket > DefaultMaxMessageSize() || lenPacket < slf.lengthFieldByteCount {
		return kkerrors.ErrMaxMessageSize
	}
	size, err := slf.GetBodySize(packet)
	if err != nil {
		return err
	}
	if lenPacket != size+slf.lengthFieldByteCount {
		return kkerrors.ErrInvalidPacket
	}
	return nil
}

// CheckPacketBuffer checks if the packet is valid.
// input: buffer [length,data].
// output: error
func (slf *LengthFieldStreamPacket) CheckPacketBuffer(buffer buffers.IBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	return slf.CheckPacket(buffer.B)
}

// pack message to stream.
// input: [message].
// output: [length,message].
// @ return [length,message], err
// 注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
func (slf *LengthFieldStreamPacket) Pack(data []byte) (buffers.IBuffer, error) {
	if len(data) > DefaultMaxMessageSize()-slf.lengthFieldByteCount {
		return nil, kkerrors.ErrMaxMessageSize
	}

	lengthFieldByteCount := slf.lengthFieldByteCount
	dataLen := len(data)
	totalLen := lengthFieldByteCount + dataLen
	bb := kkbuffer.GetWithCapacity(totalLen)
	bb.B = bb.B[:totalLen]
	slf.writeBodySize(bb.B[:lengthFieldByteCount], dataLen)
	copy(bb.B[lengthFieldByteCount:], data)

	return bb, nil
}

// unpack message from stream.
// input: [length,message].
// output: [message].
// @ return [message], ok, err
func (slf *LengthFieldStreamPacket) Unpack(data []byte) ([]byte, error) {
	lengthFieldByteCount := slf.lengthFieldByteCount
	if len(data) < lengthFieldByteCount {
		return nil, kkerrors.ErrDataTooShortToDecode
	}
	size, err := slf.GetBodySize(data)
	if err != nil {
		return nil, err
	}
	totalLen := lengthFieldByteCount + size
	if len(data) < totalLen {
		return nil, kkerrors.ErrInvalidPacket
	}
	return data[lengthFieldByteCount:totalLen], nil
}

// 粘包拆包。
// return [length,message], ok, err
func (slf *LengthFieldStreamPacket) UnpackFromSR(r IStreamReader) ([]byte, bool, error) {
	lengthFieldByteCount := slf.lengthFieldByteCount
	if r.InboundBuffered() < lengthFieldByteCount {
		return nil, false, nil
	}
	header, err := r.Peek(lengthFieldByteCount)
	if err != nil {
		if errors.Is(err, io.ErrShortBuffer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	size, err := slf.GetBodySize(header)
	if err != nil {
		return nil, false, err
	}
	totalLen := lengthFieldByteCount + size
	if r.InboundBuffered() < totalLen {
		return nil, false, nil
	}
	// _, _ = r.Discard(lengthFieldByteCount)
	data, err := r.Next(totalLen)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}
