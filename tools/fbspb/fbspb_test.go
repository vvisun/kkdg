package fbspb

import (
	"bytes"
	"strings"
	"testing"
)

func TestFBS2Proto(t *testing.T) {
	fbs := `
namespace Test;
enum E { A, B, C }
table T {
  id: int = 0;
  name: string;
}
`
	sc, err := ParseFBS(strings.NewReader(fbs))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := FBS2Proto(sc, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "syntax = \"proto3\"") {
		t.Error("missing syntax")
	}
	if !strings.Contains(out, "package Test") {
		t.Error("missing package")
	}
	if !strings.Contains(out, "message T") {
		t.Error("missing message T")
	}
	if !strings.Contains(out, "int32 id = 1") {
		t.Error("missing field id")
	}
}

func TestProto2FBS(t *testing.T) {
	proto := `
syntax = "proto3";
package Test;
message M {
  int32 x = 1;
  string s = 2;
}
`
	sc, err := ParseProto(strings.NewReader(proto))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Proto2FBS(sc, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "namespace Test") {
		t.Error("missing namespace")
	}
	if !strings.Contains(out, "table M") {
		t.Error("missing table M")
	}
	if !strings.Contains(out, "x: int") {
		t.Error("missing field x")
	}
}
