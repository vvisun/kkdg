package kkpacket

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// 完整包。
// 包结构：[length,message] = [length,head,body]
// length表示message的长度，占LengthFieldByteCount()个字节。
// [message] = [head,body]
type IPacket interface {
	// get length field byte count. [length].
	LengthFieldByteCount() int

	// length field bytes. packet = [length,message]
	LengthFieldBytes(packet []byte) []byte

	// get message bytes. packet = [length,message]
	MessageBytes(packet []byte) ([]byte, error)

	/**get byte count of message.
	 *@param packet []byte 整包数据 [length,message] 或 一部分
	 *@return int 包体[message]的长度
	 *@return error 错误
	 */
	ReadMessageSize(packet []byte) (int, error)

	/**write byte count of message to packet.
	 *@param packet []byte 整包数据 [length,message] 或 一部分
	 *@param size int 包体[message]的长度
	 */
	writeMessageSize(packet []byte, size int)

	/**check packet is valid.
	 *@param packet []byte 整包数据 [length,message]
	 *@return error 错误
	 */
	CheckPacket(packet []byte) error

	/**check packet is valid.
	 *@param packetBB *kkbuffer.ByteBuffer 整包数据 [length,message]
	 *@return error 错误
	 */
	CheckPacketBuffer(packetBB *kkbuffer.ByteBuffer) error

	/**pack message to stream.
	 *@param messageBytes []byte 消息数据 [message]
	 *@return *kkbuffer.ByteBuffer 整包数据 [length,message]
	 *@return error 错误
	 *注意：外部需记得释放缓冲区！！！否则缓冲区得不到回收，性能反而更低！！！
	 */
	Pack(messageBytes []byte) (*kkbuffer.ByteBuffer, error)

	/**unpack message from stream.
	 *@param packet []byte 整包数据 [length,message]
	 *@return []byte 消息数据 [message]
	 *@return error 错误
	 */
	Unpack(packet []byte) ([]byte, error)

	/**合并包的粘包拆包。通用方法，适用于任何流式协议。
	 *@param packets []byte 数据. [length,message][length,message]...
	 *@param recvs [][]byte 接收缓冲区. 用于复用，避免分配新的内存，会自动扩容。[length,message][length,message]...
	 *@return [][]byte 拆分后的数据包. [length,message][length,message]...
	 *@return []byte 剩余数据. 不完整的[length,message]。下次收到数据时，拼接到后面继续解析。
	 *@return error 错误
	 */
	Split(packets []byte, recvs [][]byte) ([][]byte, []byte, error)

	/**流式粘包拆包。for gnet
	 *@param r IStreamReader 流读取器
	 *@return []byte 整包数据[length,message]
	 *@return bool 是否完整
	 *@return error 错误
	 */
	SplitSR(r IStreamReader) ([]byte, bool, error)
}
