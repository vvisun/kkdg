package kkeventbus

import (
	"errors"
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	ErrRegisterInvalidMsg     = errors.New("kkeventbus: register invalid message")
	ErrRegisterInvalidTopic   = errors.New("kkeventbus: register invalid topic")
	ErrRegistryNil            = errors.New("kkeventbus: registry is nil")
	ErrPayloadNil             = errors.New("kkeventbus: payload is nil")
	ErrRegisterDuplicateTopic = errors.New("kkeventbus: register duplicate topic")
)

// MessageRegistry 载荷解析器
type MessageRegistry struct {
	mu         sync.RWMutex
	codec      kkcodec.ICodec
	typeToName map[reflect.Type]string
	nameToType map[string]reflect.Type
}

func (r *MessageRegistry) GetCodec() kkcodec.ICodec {
	return r.codec
}

// 同一topic只能注册一种类型的消息，重复注册且类型与已注册类型不一致则返回错误
// 允许不同topic注册相同类型的消息
func (r *MessageRegistry) Register(topic string, msg any) error {
	if msg == nil {
		return ErrRegisterInvalidMsg
	}
	if topic == "" {
		return ErrRegisterInvalidTopic
	}

	typ := reflect.TypeOf(msg)

	r.mu.Lock()
	defer r.mu.Unlock()
	if oldType, ok := r.nameToType[topic]; ok && oldType != typ {
		return ErrRegisterDuplicateTopic
	}
	r.typeToName[typ] = topic
	r.nameToType[topic] = typ
	return nil
}

func NewMessageRegistry(codec kkcodec.ICodec) *MessageRegistry {
	if codec == nil {
		kklog.Warn("[kkeventbus] codec is nil, use default codec: %s", "json")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}
	return &MessageRegistry{
		codec:      codec,
		typeToName: make(map[reflect.Type]string),
		nameToType: make(map[string]reflect.Type),
	}
}

//-------------------------------------------------------------------------

// 编码载荷
func EncodeMessage(registry *MessageRegistry, msg any) ([]byte, error) {
	if registry == nil {
		return nil, ErrRegistryNil
	}
	if msg == nil {
		return nil, ErrRegisterInvalidMsg
	}

	typ := reflect.TypeOf(msg)

	registry.mu.RLock()
	_, ok := registry.typeToName[typ]
	registry.mu.RUnlock()

	if !ok {
		return nil, ErrRegisterInvalidMsg
	}

	payload, err := registry.codec.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// 解码载荷
func DecodeMessage(registry *MessageRegistry, topic string, payload []byte) (any, error) {
	if registry == nil {
		return nil, ErrRegistryNil
	}
	if topic == "" {
		return nil, ErrRegisterInvalidTopic
	}
	if payload == nil {
		return nil, ErrPayloadNil
	}

	registry.mu.RLock()
	typ, ok := registry.nameToType[topic]
	registry.mu.RUnlock()

	if !ok {
		return nil, ErrRegisterInvalidTopic
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
