package kkpacket

import (
	"errors"
	"io"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IStreamReader provides buffered stream access for unpacking.
type IStreamReader interface {
	InboundBuffered() int
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
	Next(n int) (buf []byte, err error)
}

// 完整包。
// 包结构：[length,message]。
// length表示message的长度，占LengthFieldByteCount()个字节。
// message是消息对象的二进制数据。
type IStreamPacket interface {
	// get length field byte count. [length].
	LengthFieldByteCount() int

	// get message codec.
	GetMessageCodec() *PacketCodec

	/**get byte count of message.
	 *@param data []byte 整包数据 [length,message] 或 一部分
	 *@return int 包体[message]的长度
	 *@return error 错误
	 */
	ReadMessageSize(data []byte) (int, error)

	/**write byte count of message to data.
	 *@param data []byte 整包数据 [length,message] 或 一部分
	 *@param size int 包体[message]的长度
	 */
	writeMessageSize(data []byte, size int)

	/**check packet is valid.
	 *@param packet []byte 整包数据 [length,message]
	 *@return error 错误
	 */
	CheckPacket(packet []byte) error

	/**check packet is valid.
	 *@param packet []byte 整包数据 [length,message]
	 *@return error 错误
	 */
	CheckPacketBuffer(buffer *kkbuffer.ByteBuffer) error

	/**pack message to stream.
	 *@param data []byte 消息数据 [message]
	 *@return *kkbuffer.ByteBuffer 整包数据 [length,message]
	 *@return error 错误
	 *注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
	 */
	Pack(data []byte) (*kkbuffer.ByteBuffer, error)

	/**unpack message from stream.
	 *@param data []byte 整包数据 [length,message]
	 *@return []byte 消息数据 [message]
	 *@return error 错误
	 */
	Unpack(data []byte) ([]byte, error)

	/**合并包的粘包拆包。通用方法，适用于任何流式协议。
	 *@param data []byte 数据. [length,message][length,message]...
	 *@param recvs [][]byte 接收缓冲区. 用于复用，避免分配新的内存。[length,message][length,message]...
	 *@return [][]byte 数据包. [length,message][length,message]...
	 *@return []byte 剩余数据. 不完整的[length,message]。下次收到数据时，拼接到后面继续解析。
	 *@return error 错误
	 */
	Split(data []byte, recvs [][]byte) ([][]byte, []byte, error)

	/**流式粘包拆包。for gnet
	 *@param r IStreamReader 流读取器
	 *@return []byte 整包数据[length,message]
	 *@return bool 是否完整
	 *@return error 错误
	 */
	SplitSR(r IStreamReader) ([]byte, bool, error)
}

// [length,message]流式包。
type LengthFieldStreamPacket struct {
	lengthFieldByteCount int          // [length]部分的字节数。该部分用于表示包体[message]的长度。
	msgPacket            *PacketCodec // 消息包类型。用于编码解码[message]部分。
}

var _ IStreamPacket = (*LengthFieldStreamPacket)(nil)

// NewLengthFieldStreamPacket creates a length-field stream packet.
func NewLengthFieldStreamPacket(msgPacket *PacketCodec) *LengthFieldStreamPacket {
	return &LengthFieldStreamPacket{lengthFieldByteCount: 4, msgPacket: msgPacket}
}

// get message packet.
func (slf *LengthFieldStreamPacket) GetMessageCodec() *PacketCodec {
	return slf.msgPacket
}

// get length field byte count.
func (slf *LengthFieldStreamPacket) LengthFieldByteCount() int {
	return slf.lengthFieldByteCount
}

/**get byte count of message.
 *@param data []byte 整包数据 [length,message] 或 一部分
 *@return int 包体[message]的长度
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) ReadMessageSize(data []byte) (int, error) {
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

/**write byte count of message to data.
 *@param data []byte 整包数据 [length,message] 或 一部分
 *@param size int 包体[message]的长度
 */
func (slf *LengthFieldStreamPacket) writeMessageSize(data []byte, size int) {
	switch slf.lengthFieldByteCount {
	case 4:
		GetByteOrder().PutUint32(data[:4], uint32(size))
	case 2:
		GetByteOrder().PutUint16(data[:2], uint16(size))
	}
}

/**check packet is valid.
 *@param packet []byte 整包数据 [length,message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) CheckPacket(packet []byte) error {
	if len(packet) == 0 {
		return kkerrors.ErrInvalidPacket
	}
	totalLen := len(packet)
	if totalLen > DefaultMaxMessageSize() {
		return kkerrors.ErrMaxMessageSize
	}
	if totalLen < slf.lengthFieldByteCount {
		return kkerrors.ErrDataTooShortToDecode
	}
	messageLen, err := slf.ReadMessageSize(packet)
	if err != nil {
		return err
	}
	if totalLen != messageLen+slf.lengthFieldByteCount {
		return kkerrors.ErrInvalidPacket
	}
	return nil
}

/**check packet is valid.
 *@param buffer *kkbuffer.ByteBuffer 整包数据 [length,message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) CheckPacketBuffer(buffer *kkbuffer.ByteBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	return slf.CheckPacket(buffer.B)
}

/**pack message to stream.
 *@param data []byte 消息数据 [message]
 *@return *kkbuffer.ByteBuffer 整包数据 [length,message]
 *@return error 错误
 *注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
 */
func (slf *LengthFieldStreamPacket) Pack(data []byte) (*kkbuffer.ByteBuffer, error) {
	if len(data) > DefaultMaxMessageSize()-slf.lengthFieldByteCount {
		return nil, kkerrors.ErrMaxMessageSize
	}

	lfb := slf.lengthFieldByteCount
	messageLen := len(data)
	totalLen := lfb + messageLen
	bb := kkbuffer.GetWithCapacity(totalLen)
	bb.B = bb.B[:totalLen]
	slf.writeMessageSize(bb.B[:lfb], messageLen)
	copy(bb.B[lfb:], data)

	return bb, nil
}

/**unpack message from stream.
 *@param data []byte 整包数据 [length,message]
 *@return []byte 消息数据 [message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) Unpack(data []byte) ([]byte, error) {
	lfb := slf.lengthFieldByteCount
	if len(data) < lfb {
		return nil, kkerrors.ErrDataTooShortToDecode
	}
	messageLen, err := slf.ReadMessageSize(data)
	if err != nil {
		return nil, err
	}
	totalLen := lfb + messageLen
	if len(data) < totalLen {
		return nil, kkerrors.ErrInvalidPacket
	}
	return data[lfb:totalLen], nil
}

/**批量拆分数据包。通用方法，适用于任何流式协议。
 *@param data []byte 数据. [length,message][length,message]...
 *@param recvs 接收缓冲区，用于复用，避免分配新的内存 [length,message][length,message]...
 *@return [][]byte 数据包 [length,message][length,message]...
 *@return []byte 剩余数据. 不完整的[length,message]。下次收到数据时，拼接到后面继续解析。
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) Split(data []byte, recvs [][]byte) ([][]byte, []byte, error) {
	if len(data) == 0 {
		return recvs[:0], nil, nil
	}

	if recvs == nil {
		recvs = make([][]byte, 0, 8)
	}

	packets := recvs[:0]

	lfb := slf.lengthFieldByteCount
	dataLen := len(data)
	var errRet error = nil
	var leftData []byte = nil

	pos := 0
	for {
		if dataLen-pos < lfb {
			leftData = data[pos:]
			break // 数据不足，无法解析长度字段
		}
		messageLen, err := slf.ReadMessageSize(data[pos:])
		if err != nil { // 解析长度字段失败
			errRet = err
			leftData = data[pos:]
			break
		}
		totalLen := lfb + messageLen // [length,message]的长度
		if totalLen > DefaultMaxMessageSize() {
			errRet = kkerrors.ErrMaxMessageSize // 包体超过了最大长度
			leftData = data[pos:]
			break
		}
		if dataLen-pos < totalLen { // 包体未接收完整
			leftData = data[pos:]
			break
		}

		packets = append(packets, data[pos:pos+totalLen])
		pos += totalLen // 移动到下一个包的开始位置
		if pos >= dataLen {
			break // 数据已全部处理完毕
		}
	}
	return packets, leftData, errRet
}

/**流式粘包拆包。for gnet
 *@param r IStreamReader 流读取器
 *@return []byte 整包数据[length,message]
 *@return bool 是否完整
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) SplitSR(r IStreamReader) ([]byte, bool, error) {
	lfb := slf.lengthFieldByteCount
	if r.InboundBuffered() < lfb {
		return nil, false, nil
	}
	header, err := r.Peek(lfb)
	if err != nil {
		if errors.Is(err, io.ErrShortBuffer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	messageLen, err := slf.ReadMessageSize(header)
	if err != nil {
		return nil, false, err
	}
	totalLen := lfb + messageLen
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
