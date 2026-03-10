package actorremotes

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

var msgCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)

func SetMsgCodec(codec kkcodec.ICodec) {
	if codec == nil {
		kklog.Errorf("[kkactor] SetMsgCodec codec is nil, use default codec")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	msgCodec = codec
}

type messageRegistry struct {
	mu         sync.RWMutex
	typeToName map[reflect.Type]string
	nameToType map[string]reflect.Type
}

var defaultMessageRegistry = &messageRegistry{
	typeToName: make(map[reflect.Type]string),
	nameToType: make(map[string]reflect.Type),
}

func RegisterMessage(msg any) error {
	return defaultMessageRegistry.Register(msg)
}

func (r *messageRegistry) Register(msg any) error {
	if msg == nil {
		return kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}
	typ := reflect.TypeOf(msg)
	typeName := xreflect.TypeName(typ)
	if typeName == "" {
		return kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}

	r.mu.Lock()
	r.typeToName[typ] = typeName
	r.nameToType[typeName] = typ
	r.mu.Unlock()
	return nil
}

func EncodeMessage(msg any) (string, []byte, error) {
	if msg == nil {
		return "", nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}
	typ := reflect.TypeOf(msg)

	defaultMessageRegistry.mu.RLock()
	typeName, ok := defaultMessageRegistry.typeToName[typ]
	defaultMessageRegistry.mu.RUnlock()
	if !ok {
		return "", nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}

	payload, err := msgCodec.Marshal(msg)
	if err != nil {
		return "", nil, err
	}
	return typeName, payload, nil
}

func DecodeMessage(typeName string, payload []byte) (any, error) {
	defaultMessageRegistry.mu.RLock()
	typ, ok := defaultMessageRegistry.nameToType[typeName]
	defaultMessageRegistry.mu.RUnlock()
	if !ok {
		return nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}

	var target reflect.Value
	if typ.Kind() == reflect.Ptr {
		target = reflect.New(typ.Elem())
	} else {
		target = reflect.New(typ)
	}
	if err := msgCodec.Unmarshal(payload, target.Interface()); err != nil {
		return nil, err
	}
	if typ.Kind() == reflect.Ptr {
		return target.Interface(), nil
	}
	return target.Elem().Interface(), nil
}
