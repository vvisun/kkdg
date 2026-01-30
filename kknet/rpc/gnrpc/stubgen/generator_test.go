package stubgen

import (
	"strings"
	"testing"
)

func TestGenerate_Basic(t *testing.T) {
	spec := Spec{
		Package:     "pbbase",
		ServiceName: "pbbase.StringService",
		GoServiceName: "StringService",
		Methods: []MethodSpec{
			{
				Name:     "Echo",
				Request:  "github.com/vvisun/kkdg/proto/pbbase.String",
				Response: "github.com/vvisun/kkdg/proto/pbbase.String",
			},
		},
	}
	out, err := Generate(spec)
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, "type StringServiceServer interface") {
		t.Fatalf("missing server interface")
	}
	if !strings.Contains(s, "func RegisterStringServiceServer") {
		t.Fatalf("missing register func")
	}
	if !strings.Contains(s, "type StringServiceClient struct") {
		t.Fatalf("missing client type")
	}
	if !strings.Contains(s, `gnrpc.FullMethodName("pbbase.StringService", "Echo")`) {
		t.Fatalf("missing full method name usage")
	}
}

