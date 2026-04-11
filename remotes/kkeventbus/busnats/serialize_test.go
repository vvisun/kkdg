package nats

import (
	"bytes"
	"testing"
)

func TestSerialize_deserialize_roundtrip(t *testing.T) {
	topic := "evt.order"
	payload := []byte("hello-世界")
	raw, err := serialize(topic, payload)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := deserialize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Topic != topic {
		t.Fatalf("Topic %q", ev.Topic)
	}
	if !bytes.Equal(ev.Payload.Bytes(), payload) {
		t.Fatalf("payload got %q want %q", ev.Payload.Bytes(), payload)
	}
	if ev.ID == "" {
		t.Fatal("empty ID")
	}
}

func TestSerialize_deserialize_nilPayload(t *testing.T) {
	raw, err := serialize("t", nil)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := deserialize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if b := ev.Payload.Bytes(); len(b) != 0 {
		t.Fatalf("unexpected payload %q", b)
	}
}
