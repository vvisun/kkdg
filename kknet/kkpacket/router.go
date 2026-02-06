package kkpacket

import (
	"reflect"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MsgMeta struct {
	ID           MSGID
	Type         reflect.Type
	Route        string
	streamPacket IStreamPacket
}

type MSGID = uint32 // 消息ID

type Router struct {
	typeToId  map[reflect.Type]MSGID
	idToType  map[MSGID]reflect.Type
	idToRoute map[MSGID]string
}

func NewRouter() *Router {
	return &Router{
		typeToId:  make(map[reflect.Type]MSGID),
		idToType:  make(map[MSGID]reflect.Type),
		idToRoute: make(map[MSGID]string),
	}
}

func (r *Router) Register(id MSGID, msg any, route string) error {
	if id == 0 {
		kklog.Errorf("message id is 0")
		return kkerrors.ErrInvalidMsgID
	}
	msgType := reflect.TypeOf(msg)
	if msgType == nil || !xreflect.IsPointer(msg) {
		kklog.Errorf("message pointer required, got %v", msgType)
		return kkerrors.ErrInvalidMessage
	}
	r.typeToId[reflect.TypeOf(msg)] = id
	r.idToType[id] = reflect.TypeOf(msg)
	r.idToRoute[id] = route
	return nil
}

func (r *Router) GetMsgID(msg any) MSGID {
	msgType := reflect.TypeOf(msg)
	id, ok := r.typeToId[msgType]
	if !ok {
		kklog.Errorf("message %v is not registered", msgType)
		return 0
	}
	return id
}

func (r *Router) GetMsgType(id MSGID) reflect.Type {
	tp, ok := r.idToType[id]
	if !ok {
		kklog.Errorf("message id %v is not registered", id)
		return nil
	}
	return tp
}

func (r *Router) GetMsgRoute(id MSGID) string {
	route, ok := r.idToRoute[id]
	if !ok {
		kklog.Errorf("message id %v is not registered", id)
		return ""
	}
	return route
}
