package kkpacket

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MSGID = uint32 // 消息ID

//--------------------------------------------------

/*
*维护消息ID与消息类型、消息路由的映射关系。

	 *消息ID: 消息的唯一标识
	 *消息类型: 消息结构体
	 *消息路由: 标识消息应发往的目标节点类型(nodeType)。

		 *例如:

		 	type Msg1Req struct {}
			type Msg1Resp struct {}
			type MsgBroadcast struct {}
			type ChatMsgReq struct {}
			type ChatMsgResp struct {}

			  消息ID -> 消息类型 -> 消息路由
			  1001 -> *Msg1Req -> "game"
			  1002 -> *Msg1Resp -> "game"
			  1003 -> *MsgBroadcast -> "gate"
			  1004 -> *ChatMsgReq -> "chat"
			  1005 -> *ChatMsgResp -> "chat"
			  ...

*
*/
type MsgRouter struct {
	mu        sync.RWMutex //一般在初始化阶段就应该完成注册了，所以后面的Get操作都是读操作，不需要加锁
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
		return kkerrors.ErrPktInvalidMsgID
	}

	if !xreflect.IsPointer(msgPtr) {
		kklog.Errorf("message pointer required, got %T", msgPtr)
		return kkerrors.ErrPktInvalidMessage
	}
	msgType := reflect.TypeOf(msgPtr)
	if msgType == nil {
		kklog.Errorf("message pointer required, got %v", msgType)
		return kkerrors.ErrPktInvalidMessage
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.idToType[id]; ok && r.idToType[id] != msgType {
		kklog.Errorf("message id %v is already registered with different type %v", id, r.idToType[id])
		return kkerrors.ErrPktMsgIDAlreadyRegistered
	}

	r.typeToId[msgType] = id
	r.idToType[id] = msgType
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

func (r *MsgRouter) GetMsgRoute(id MSGID) (string, error) {
	route, ok := r.idToRoute[id]
	if !ok {
		kklog.Debugf("message id %v is not registered", id)
		return "", kkerrors.ErrPktMsgIDNotRegistered
	}
	return route, nil
}
