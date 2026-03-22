package actortrans

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

// 远程Actor消息注册表
type MessageRegistry struct {
	mu         sync.RWMutex
	codec      kkcodec.ICodec
	typeToName map[reflect.Type]string
	nameToType map[string]reflect.Type
}

func (r *MessageRegistry) Register(msg any) error {
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

func NewMessageRegistry(codec kkcodec.ICodec) *MessageRegistry {
	if codec == nil {
		kklog.Warn("[actortrans] codec is nil, use default codec: %s", "json")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}
	return &MessageRegistry{
		codec:      codec,
		typeToName: make(map[reflect.Type]string),
		nameToType: make(map[string]reflect.Type),
	}
}

//-------------------------------------------------------------------------

func EncodeMessage(registry *MessageRegistry, msg any) (string, []byte, error) {
	if msg == nil {
		return "", nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}
	typ := reflect.TypeOf(msg)

	registry.mu.RLock()
	typeName, ok := registry.typeToName[typ]
	registry.mu.RUnlock()

	if !ok {
		return "", nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}

	payload, err := registry.codec.Marshal(msg)
	if err != nil {
		return "", nil, err
	}
	return typeName, payload, nil
}

func DecodeMessage(registry *MessageRegistry, typeName string, payload []byte) (any, error) {
	registry.mu.RLock()
	typ, ok := registry.nameToType[typeName]
	registry.mu.RUnlock()

	if !ok {
		return nil, kkerrors.ErrActorRemoteMsgTypeNotRegistered
	}

	var target reflect.Value
	if typ.Kind() == reflect.Ptr {
		target = reflect.New(typ.Elem())
	} else {
		target = reflect.New(typ)
	}
	if err := registry.codec.Unmarshal(payload, target.Interface()); err != nil {
		return nil, err
	}
	if typ.Kind() == reflect.Ptr {
		return target.Interface(), nil
	}
	return target.Elem().Interface(), nil
}
