package kkpacket

import (
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MSGID = uint32 // 消息ID

//--------------------------------------------------

// msgMeta 消息元数据
// 用于存储消息ID、消息类型、消息路由和消息编码器
type msgMeta[T any] struct {
	msgID         MSGID          // 消息ID
	msgType       reflect.Type   // 消息类型
	messagePacket *MessagePacket // 消息包
	msgRoute      string         // 消息路由
}

func newMsgMeta[T any](id MSGID, route string, messagePacket *MessagePacket) *msgMeta[T] {
	return &msgMeta[T]{
		msgID:         id,
		msgType:       reflect.TypeFor[T](),
		msgRoute:      route,
		messagePacket: messagePacket,
	}
}

func (m *msgMeta[T]) GetMsgID() MSGID {
	return m.msgID
}

func (m *msgMeta[T]) GetMsgType() reflect.Type {
	return m.msgType
}

func (m *msgMeta[T]) GetMsgRoute() string {
	return m.msgRoute
}

func (m *msgMeta[T]) GetCodec() kkcodec.ICodec {
	return m.messagePacket.GetBodyCodec()
}

func (m *msgMeta[T]) Unmarshal(data []byte) (*T, error) {
	var v T
	err := m.messagePacket.GetBodyCodec().Unmarshal(data, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (m *msgMeta[T]) Marshal(v *T) ([]byte, error) {
	if v == nil {
		return nil, kkerrors.ErrInvalidMessage
	}
	return m.messagePacket.GetBodyCodec().Marshal(v)
}

func (m *msgMeta[T]) MarshalAppend(v *T, offset int) (*kkbuffer.ByteBuffer, error) {
	if v == nil {
		return nil, kkerrors.ErrInvalidMessage
	}
	return m.messagePacket.GetBodyCodec().MarshalAppend(v, offset)
}

func (m *msgMeta[T]) EncodeStream(v *T, stream IPacket) (*kkbuffer.ByteBuffer, error) {
	if v == nil {
		return nil, kkerrors.ErrInvalidMessage
	}
	return EncodeStream(v, stream, m.messagePacket)
}

func (m *msgMeta[T]) DecodeStream(bb *kkbuffer.ByteBuffer, stream IPacket) (*T, error) {
	if bb == nil {
		return nil, kkerrors.ErrInvalidMessage
	}
	messageBytes, err := stream.MessageBytes(bb.B)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := m.messagePacket.BodyBytes(messageBytes)
	if err != nil {
		return nil, err
	}
	var v T
	err = m.messagePacket.GetBodyCodec().Unmarshal(bodyBytes, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

//--------------------------------------------------

type MsgRouter struct {
	typeToId  map[reflect.Type]MSGID
	idToType  map[MSGID]reflect.Type
	idToRoute map[MSGID]string
}

func NewMsgRouter() *MsgRouter {
	return &MsgRouter{
		typeToId:  make(map[reflect.Type]MSGID),
		idToType:  make(map[MSGID]reflect.Type),
		idToRoute: make(map[MSGID]string),
	}
}

func (r *MsgRouter) Register(id MSGID, msgPtr any, route string) error {
	if id == 0 {
		kklog.Errorf("message id is 0")
		return kkerrors.ErrInvalidMsgID
	}
	msgType := reflect.TypeOf(msgPtr)
	if msgType == nil || !xreflect.IsPointer(msgPtr) {
		kklog.Errorf("message pointer required, got %v", msgType)
		return kkerrors.ErrInvalidMessage
	}
	r.typeToId[reflect.TypeOf(msgPtr)] = id
	r.idToType[id] = reflect.TypeOf(msgPtr)
	r.idToRoute[id] = route
	return nil
}

func (r *MsgRouter) GetMsgID(msgPtr any) MSGID {
	msgType := reflect.TypeOf(msgPtr)
	id, ok := r.typeToId[msgType]
	if !ok {
		kklog.Debugf("message %v is not registered", msgType)
		return 0
	}
	return id
}

func (r *MsgRouter) GetMsgType(id MSGID) reflect.Type {
	tp, ok := r.idToType[id]
	if !ok {
		kklog.Debugf("message id %v is not registered", id)
		return nil
	}
	return tp
}

func (r *MsgRouter) GetMsgRoute(id MSGID) string {
	route, ok := r.idToRoute[id]
	if !ok {
		kklog.Debugf("message id %v is not registered", id)
		return ""
	}
	return route
}
