package kkpacket

import (
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type msgMeta struct {
	msgID    MSGID
	msgType  reflect.Type
	msgRoute string
}

type MSGID = uint32 // 消息ID

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
