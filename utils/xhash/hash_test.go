package xhash_test

import (
	"testing"

	"github.com/vvisun/kkdg/utils/xhash"
)

func TestSHA256(t *testing.T) {
	t.Log(xhash.SHA256("abc"))
}
