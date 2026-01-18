package kkpacket

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MSGID = uint32 // 消息ID

var (
	typeToId  map[reflect.Type]MSGID = make(map[reflect.Type]MSGID)
	idToType  map[MSGID]reflect.Type = make(map[MSGID]reflect.Type)
	idToRoute map[MSGID]string       = make(map[MSGID]string)
)

var (
	msgMutex sync.Mutex
)

/*
*
注册消息。非线程安全

	@param id MSGID 消息ID
	@param msg any 消息类型
	@param route string 消息路由
	@return error 错误
*/
func RegisterMsg[T any](id MSGID, msg *T, route string) error {
	if id == 0 {
		kklog.Errorf("message id is 0")
		return kkerrors.ErrInvalidMsgID
	}
	msgType := reflect.TypeOf(msg)
	if msgType == nil || !xreflect.IsPointer(msg) {
		kklog.Errorf("message pointer required, got %v", msgType)
		return kkerrors.ErrInvalidMessage
	}
	typeToId[reflect.TypeOf(msg)] = id
	idToType[id] = reflect.TypeOf(msg)
	idToRoute[id] = route
	return nil
}

/*
*
*
注册消息。线程安全

	@param id MSGID 消息ID
	@param msg any 消息类型
	@param route string 消息路由
	@return error 错误
*/
func RegisterMsgSafe[T any](id MSGID, msg *T, route string) error {
	msgMutex.Lock()
	defer msgMutex.Unlock()
	return RegisterMsg(id, msg, route)
}

func GetMsgID(msg any) MSGID {
	msgType := reflect.TypeOf(msg)
	id, ok := typeToId[msgType]
	if !ok {
		kklog.Errorf("message %v is not registered", msgType)
		return 0
	}
	return id
}

func GetMsgType(id MSGID) reflect.Type {
	tp, ok := idToType[id]
	if !ok {
		kklog.Errorf("message id %v is not registered", id)
		return nil
	}
	return tp
}

func GetMsgRoute(id MSGID) string {
	route, ok := idToRoute[id]
	if !ok {
		kklog.Errorf("message id %v is not registered", id)
		return ""
	}
	return route
}
