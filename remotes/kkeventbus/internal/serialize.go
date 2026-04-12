package internal

import (
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/utils/xtime"
	"github.com/vvisun/kkdg/utils/xuuid"
)

type SefializeData struct {
	ID        string `json:"id"`        // 事件ID
	Topic     string `json:"topic"`     // 事件主题
	Payload   []byte `json:"payload"`   // 事件载荷
	Timestamp int64  `json:"timestamp"` // 事件时间
}

// 序列化
func Serialize(registry *kkeventbus.MessageRegistry, topic string, payload any) ([]byte, error) {
	if registry == nil {
		return nil, kkeventbus.ErrRegistryNil
	}

	payloadBytes, err := kkeventbus.EncodeMessage(registry, payload)
	if err != nil {
		return nil, err
	}
	return registry.GetCodec().Marshal(&SefializeData{
		ID:        xuuid.UUID(),
		Topic:     topic,
		Payload:   payloadBytes,
		Timestamp: xtime.Now().UnixNano(),
	})
}

// 反序列化
func Deserialize(registry *kkeventbus.MessageRegistry, v []byte) (*kkeventbus.Event, error) {
	if registry == nil {
		return nil, kkeventbus.ErrRegistryNil
	}

	d := &SefializeData{}

	err := registry.GetCodec().Unmarshal(v, d)
	if err != nil {
		return nil, err
	}

	payload, err := kkeventbus.DecodeMessage(registry, d.Topic, d.Payload)
	if err != nil {
		return nil, err
	}

	return &kkeventbus.Event{
		ID:        d.ID,
		Topic:     d.Topic,
		Payload:   payload,
		Timestamp: xtime.UnixNano(d.Timestamp),
	}, nil
}
