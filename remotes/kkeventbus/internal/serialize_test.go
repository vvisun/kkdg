package internal

import (
	"bytes"
	"testing"

	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func TestSerialize_deserialize_roundtrip(t *testing.T) {
	topic := "evt.order"
	payload := []byte("hello-世界")
	registry := kkeventbus.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeJson))
	if err := registry.Register(topic, payload); err != nil {
		t.Fatal(err)
	}
	raw, err := Serialize(registry, topic, payload)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := Deserialize(registry, raw)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Topic != topic {
		t.Fatalf("Topic %q", ev.Topic)
	}
	if !bytes.Equal(ev.Payload.([]byte), payload) {
		t.Fatalf("payload got %q want %q", ev.Payload.([]byte), payload)
	}
	if ev.ID == "" {
		t.Fatal("empty ID")
	}
}

func TestSerialize_deserialize_nilPayload(t *testing.T) {
	registry := kkeventbus.NewMessageRegistry(kkcodec.GetCodec(kkcodec.CodecTypeJson))
	if err := registry.Register("t", nil); err == nil {
		t.Fatal(err)
	}
	raw, err := Serialize(registry, "t", nil)
	if err == nil {
		t.Fatal(err)
	}
	ev, err := Deserialize(registry, raw)
	if err == nil {
		t.Fatal(err)
	}
	if ev != nil {
		t.Fatal("unexpected payload")
	}
}
