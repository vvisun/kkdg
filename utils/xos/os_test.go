package xos_test

import (
	"testing"

	xfile "github.com/vvisun/kkdg/utils/xos"
)

func TestWriteFile(t *testing.T) {
	err := xfile.WriteFile("./run/test.txt", []byte("hello world"))
	if err != nil {
		t.Fatalf("write file failed: %v", err)
	}
}
