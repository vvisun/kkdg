package xbuffer_test

import (
	"testing"

	buffer "github.com/vvisun/kkdg/utils/xbuffer"
)

func Test_BytesPool(t *testing.T) {
	p := buffer.NewBytesPoolWithCapacity(1024)
	b := p.Get(3)
	t.Log(b.Bytes())
	p.Put(b)
}
