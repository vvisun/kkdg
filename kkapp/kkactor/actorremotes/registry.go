package actorremotes

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/xreflect"
)

//-------------------------------------------------------------------------

type MessageRegistry struct {
	mu         sync.RWMutex
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

func NewMessageRegistry() *MessageRegistry {
	return &MessageRegistry{
		typeToName: make(map[reflect.Type]string),
		nameToType: make(map[string]reflect.Type),
	}
}

//-------------------------------------------------------------------------

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
