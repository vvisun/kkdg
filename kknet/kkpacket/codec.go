package kkpacket

import (
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

/**编码包。
 *注意：外部需记得释放*kkbuffer.ByteBuffer！！！否则*kkbuffer.ByteBuffer得不到回收，性能反而更低！！！
 *@param v *T 消息对象（object）
 *@param stream IPacket 流包类型
 *@param messagePacket *MessagePacket 消息包
 *@return *kkbuffer.ByteBuffer 包数据[length,message]
 *@return error 错误
 */
func EncodeStream(v any, stream IPacket, messagePacket *MessagePacket) (*kkbuffer.ByteBuffer, error) {
	msgID := messagePacket.GetRouter().GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrPktMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()
	headSize := messagePacket.GetHead().GetSize()

	bb, err := messagePacket.GetBodyCodec().MarshalAppend(v, lfbCount+headSize)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrPktEncodeFailed
	}

	stream.WriteMessageSize(bb.B, len(bb.B)-lfbCount)

	messageBytes, err := stream.MessageBytes(bb.B)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	headBytes, err := messagePacket.HeadBytes(messageBytes)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	err = messagePacket.GetHead().Marshal(headBytes, GetByteOrder(), int(msgID))
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}

	return bb, nil
}

/**解码包。
 *@param bb *kkbuffer.ByteBuffer 包数据[length,message]
 *@param stream IPacket 流包类型
 *@param messagePacket *MessagePacket 消息包
 *@return any 消息对象
 *@return error 错误
 */
func DecodeStream(bb *kkbuffer.ByteBuffer, stream IPacket, messagePacket *MessagePacket) (any, error) {
	v, err := DecodePacket(bb.B, stream, messagePacket)
	kkbuffer.Put(bb)
	if err != nil {
		return nil, err
	}
	return v, nil
}

/**解码包。
 *@param packet []byte 包数据[length,message]
 *@param stream IPacket 流包类型
 *@param messagePacket *MessagePacket 消息包
 *@return any 消息对象
 *@return error 错误
 */
func DecodePacket(packet []byte, stream IPacket, messagePacket *MessagePacket) (any, error) {
	messageBytes, err := stream.MessageBytes(packet)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := messagePacket.BodyBytes(messageBytes)
	if err != nil {
		return nil, err
	}
	msgId, err := messagePacket.GetMsgID(messageBytes)
	if err != nil {
		return nil, err
	}
	msgType := messagePacket.GetRouter().GetMsgType(msgId)
	if msgType == nil {
		return nil, kkerrors.ErrPktMsgTypeNotRegistered
	}
	v := reflect.New(msgType.Elem()).Interface()
	err = messagePacket.GetBodyCodec().Unmarshal(bodyBytes, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

/**编码完整包。
 *@param v any 消息对象
 *@param fullPacket *FullPacket 完整包工具
 *@return *kkbuffer.ByteBuffer 完整包数据[length,message]
 *@return error 错误
 */
func EncodeFullPacket(v any, fullPacket *FullPacket) (*kkbuffer.ByteBuffer, error) {
	return EncodeStream(v, fullPacket.GetStreamTool(), fullPacket.GetMessageTool())
}

/**解码完整包。
 *@param bb *kkbuffer.ByteBuffer 完整包数据[length,message]
 *@param fullPacket *FullPacket 完整包工具
 *@return any 消息对象
 *@return error 错误
 */
func DecodeFullPacket(bb *kkbuffer.ByteBuffer, fullPacket *FullPacket) (any, error) {
	return DecodeStream(bb, fullPacket.GetStreamTool(), fullPacket.GetMessageTool())
}
