package nats

import (
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/utils/kkcodec/json"
	xvalue "github.com/vvisun/kkdg/utils/value"
	"github.com/vvisun/kkdg/utils/xconv"
	"github.com/vvisun/kkdg/utils/xtime"
	"github.com/vvisun/kkdg/utils/xuuid"
)

type data struct {
	ID        string `json:"id"`        // 事件ID
	Topic     string `json:"topic"`     // 事件主题
	Payload   []byte `json:"payload"`   // 事件载荷
	Timestamp int64  `json:"timestamp"` // 事件时间
}

// 序列化
func serialize(topic string, payload any) ([]byte, error) {
	return json.Marshal(&data{
		ID:        xuuid.UUID(),
		Topic:     topic,
		Payload:   xconv.Bytes(payload),
		Timestamp: xtime.Now().UnixNano(),
	})
}

// 反序列化
func deserialize(v []byte) (*kkeventbus.Event, error) {
	d := &data{}

	err := json.Unmarshal(v, d)
	if err != nil {
		return nil, err
	}

	return &kkeventbus.Event{
		ID:        d.ID,
		Topic:     d.Topic,
		Payload:   xvalue.NewValue(d.Payload),
		Timestamp: xtime.UnixNano(d.Timestamp),
	}, nil
}
