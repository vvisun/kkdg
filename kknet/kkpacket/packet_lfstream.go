package kkpacket

import (
	"errors"
	"io"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// IStreamReader provides buffered stream access for unpacking.
type IStreamReader interface {
	InboundBuffered() int
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
	Next(n int) (buf []byte, err error)
}

// [length,message]流式包。
// [length,message] = [length,head,body]
// [message] = [head,body]
type LengthFieldStreamPacket struct {
	lfbCount      int // [length]部分的字节数。该部分用于表示包体[message]的长度。
	maxPacketSize int // 整包[length,message]最大长度（字节数）
}

// NewLengthFieldStreamPacket creates a length-field stream packet.
//
//	@param lfb [length]部分的字节数。该部分用于表示包体[message]的长度。
//	@param maxPacketSize 整包[length,message]最大长度（字节数）
func NewLengthFieldStreamPacket(lfb int, maxPacketSize int) IPacket {
	if lfb != 2 && lfb != 4 {
		kklog.PanicLog("length field byte count must be 2 or 4")
	}
	if maxPacketSize <= 0 {
		kklog.PanicLog("max packet size must be greater than 0")
	}
	// 2字节长度字段，最大长度为65535 (2^16-1)，超过则溢出
	if lfb == 2 && maxPacketSize > 65535 {
		kklog.PanicLog("max packet size must be less than 65535")
	}
	// 4字节长度字段，最大长度为4294967295 (2^32-1)，超过则溢出
	if lfb == 4 && maxPacketSize > 4294967295 {
		kklog.PanicLog("max packet size must be less than 4294967295")
	}
	return &LengthFieldStreamPacket{
		lfbCount:      lfb,
		maxPacketSize: maxPacketSize,
	}
}

// get length field byte count.
func (slf *LengthFieldStreamPacket) LengthFieldByteCount() int {
	return slf.lfbCount
}

// get max packet size. 整包[length,message]最大长度（字节数）
func (slf *LengthFieldStreamPacket) MaxPacketSize() int {
	return slf.maxPacketSize
}

// length field bytes. packet = [length,message]
func (slf *LengthFieldStreamPacket) LengthFieldBytes(packet []byte) []byte {
	return packet[:slf.lfbCount]
}

// get message bytes. packet = [length,message]
func (slf *LengthFieldStreamPacket) MessageBytes(packet []byte) ([]byte, error) {
	if len(packet) < slf.lfbCount {
		return nil, kkerrors.ErrPktDataTooShortToDecode
	}
	return packet[slf.lfbCount:], nil
}

/**get byte count of message.
 *@param packet []byte 整包数据 [length,message] 或 一部分
 *@return int 包体[message]的长度
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) ReadMessageSize(packet []byte) (int, error) {
	if len(packet) < slf.lfbCount {
		return 0, kkerrors.ErrPktDataTooShortToDecode
	}
	switch slf.lfbCount {
	case 4:
		return int(GetByteOrder().Uint32(packet)), nil
	case 2:
		return int(GetByteOrder().Uint16(packet)), nil
	default:
		return 0, kkerrors.ErrPktInvalidLengthFieldByteCount
	}
}

/**write byte count of message to packet.
 *@param packet []byte 整包数据 [length,message] 或 一部分
 *@param size int 包体[message]的长度
 */
func (slf *LengthFieldStreamPacket) WriteMessageSize(packet []byte, size int) {
	switch slf.lfbCount {
	case 4:
		GetByteOrder().PutUint32(packet[:4], uint32(size))
	case 2:
		GetByteOrder().PutUint16(packet[:2], uint16(size))
	}
}

/**check packet is valid.
 *@param packet []byte 整包数据 [length,message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) CheckPacket(packet []byte) error {
	totalLen := len(packet)
	if totalLen < slf.lfbCount {
		return kkerrors.ErrPktDataTooShortToDecode
	}
	if totalLen > slf.maxPacketSize {
		return kkerrors.ErrPktMaxMessageSize
	}
	messageLen, err := slf.ReadMessageSize(packet)
	if err != nil {
		return err
	}
	if totalLen != messageLen+slf.lfbCount {
		return kkerrors.ErrClusterInvalidPacket
	}
	return nil
}

/**check packet is valid.
 *@param packetBB *kkbuffer.ByteBuffer 整包数据 [length,message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) CheckPacketBuffer(packetBB *kkbuffer.ByteBuffer) error {
	if packetBB == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	return slf.CheckPacket(packetBB.B)
}

/**pack message to stream.
 *@param messageBytes []byte 消息数据 [message]
 *@return *kkbuffer.ByteBuffer 整包数据 [length,message]
 *@return error 错误
 *注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
 */
func (slf *LengthFieldStreamPacket) Pack(messageBytes []byte) (*kkbuffer.ByteBuffer, error) {
	if len(messageBytes) > slf.maxPacketSize-slf.lfbCount {
		return nil, kkerrors.ErrPktMaxMessageSize
	}

	lfb := slf.lfbCount
	messageLen := len(messageBytes)
	totalLen := lfb + messageLen
	bb := kkbuffer.GetWithCapacity(totalLen)
	bb.B = bb.B[:totalLen]
	slf.WriteMessageSize(bb.B[:lfb], messageLen)
	copy(bb.B[lfb:], messageBytes)

	return bb, nil
}

/**unpack message from stream.
 *@param packet []byte 整包数据 [length,message]
 *@return []byte 消息数据 [message]
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) Unpack(packet []byte) ([]byte, error) {
	messageLen, err := slf.ReadMessageSize(packet)
	if err != nil {
		return nil, err
	}
	lfb := slf.lfbCount
	totalLen := lfb + messageLen
	if len(packet) < totalLen {
		return nil, kkerrors.ErrClusterInvalidPacket
	}
	return packet[lfb:totalLen], nil
}

/**批量拆分数据包。通用方法，适用于任何流式协议。
 *@param packets []byte 数据. [length,message][length,message]...
 *@param recvs 接收缓冲区，用于实现0分配，会自动扩容。[length,message][length,message]...
 *@return [][]byte 拆分后的数据包. [length,message][length,message]...
 *@return []byte 剩余数据. 不完整的[length,message]。下次收到数据时，拼接到后面继续解析。
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) Split(packets []byte, recvs [][]byte) ([][]byte, []byte, error) {
	if len(packets) == 0 {
		return recvs[:0], nil, nil
	}

	bufList := recvs[:0]

	lfb := slf.lfbCount
	dataLen := len(packets)
	var errRet error = nil
	var leftData []byte = nil

	pos := 0
	for {
		if dataLen-pos < lfb {
			leftData = packets[pos:]
			break // 数据不足，无法解析长度字段
		}
		messageLen, err := slf.ReadMessageSize(packets[pos:])
		if err != nil { // 解析长度字段失败
			errRet = err
			leftData = packets[pos:]
			break
		}
		totalLen := lfb + messageLen // [length,message]的长度
		if totalLen > slf.maxPacketSize {
			errRet = kkerrors.ErrPktMaxMessageSize // 包体超过了最大长度
			leftData = packets[pos:]
			break
		}
		if dataLen-pos < totalLen { // 包体未接收完整
			leftData = packets[pos:]
			break
		}

		bufList = append(bufList, packets[pos:pos+totalLen])
		pos += totalLen // 移动到下一个包的开始位置
		if pos >= dataLen {
			break // 数据已全部处理完毕
		}
	}
	return bufList, leftData, errRet
}

/**流式粘包拆包。for gnet
 *@param r IStreamReader 流读取器
 *@return []byte 整包数据[length,message]
 *@return bool 是否完整
 *@return error 错误
 */
func (slf *LengthFieldStreamPacket) SplitSR(r IStreamReader) ([]byte, bool, error) {
	// 1. 检查是否收到完整的长度字段
	lfb := slf.lfbCount
	if r.InboundBuffered() < lfb {
		return nil, false, nil //尚未收到完整的长度字段
	}
	header, err := r.Peek(lfb)
	if err != nil {
		if errors.Is(err, io.ErrShortBuffer) {
			return nil, false, nil //尚未收到完整的长度字段
		}
		return nil, false, err // 解析长度字段失败
	}

	// 2. 解析长度字段
	messageLen, err := slf.ReadMessageSize(header)
	if err != nil {
		return nil, false, err // 解析长度字段失败
	}

	// 3. 检查是否收到完整的数据包
	totalLen := lfb + messageLen
	if r.InboundBuffered() < totalLen {
		return nil, false, nil //尚未收到完整的数据包
	}

	// 4. 取出完整的数据包
	data, err := r.Next(totalLen)
	if err != nil {
		return nil, false, err // 取出数据包失败
	}

	// 5. 成功收到完整的数据包
	return data, true, nil
}
