package kkpacket

import (
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
