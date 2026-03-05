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
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}

	lfbCount := stream.LengthFieldByteCount()
	headSize := messagePacket.GetHead().GetSize()

	bb, err := messagePacket.GetBodyCodec().MarshalAppend(v, lfbCount+headSize)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrEncodeFailed
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
	messageBytes, err := stream.MessageBytes(bb.B)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	bodyBytes, err := messagePacket.BodyBytes(messageBytes)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	msgId, err := messagePacket.GetMsgID(messageBytes)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	msgType := messagePacket.GetRouter().GetMsgType(msgId)
	if msgType == nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	v := reflect.New(msgType.Elem()).Interface()
	err = messagePacket.GetBodyCodec().Unmarshal(bodyBytes, &v)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	kkbuffer.Put(bb)
	return v, nil
}

/**编码消息。
 *@param v any 消息对象
 *@param messagePacket *MessagePacket 消息包
 *@return []byte 消息数据[message]
 *@return error 错误
 */
func EncodeMessage(v any, messagePacket *MessagePacket) (*kkbuffer.ByteBuffer, error) {
	msgID := messagePacket.GetRouter().GetMsgID(v)
	if msgID == 0 {
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	offset := messagePacket.GetHead().GetSize()
	bb, err := messagePacket.GetBodyCodec().MarshalAppend(v, offset)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	headBytes, err := messagePacket.HeadBytes(bb.B)
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

/**解码消息。
 *@param bb *kkbuffer.ByteBuffer 消息数据[message]
 *@param messagePacket *MessagePacket 消息包
 *@return any 消息对象
 *@return error 错误
 */
func DecodeMessage(bb *kkbuffer.ByteBuffer, messagePacket *MessagePacket) (any, error) {
	headBytes, err := messagePacket.HeadBytes(bb.B)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	msgId, err := messagePacket.GetMsgID(headBytes)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	msgType := messagePacket.GetRouter().GetMsgType(msgId)
	if msgType == nil {
		kkbuffer.Put(bb)
		return nil, kkerrors.ErrMsgTypeNotRegistered
	}
	bodyBytes, err := messagePacket.BodyBytes(bb.B)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	v := reflect.New(msgType.Elem()).Interface()
	err = messagePacket.GetBodyCodec().Unmarshal(bodyBytes, &v)
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	kkbuffer.Put(bb)
	return v, nil
}
