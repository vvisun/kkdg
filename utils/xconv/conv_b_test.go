package xconv_test

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/utils/xconv"
)

func TestB(t *testing.T) {
	fmt.Println(xconv.B("4MB"))
	fmt.Println(xconv.B("4"))
	fmt.Println(xconv.B("4B"))
	fmt.Println(xconv.B("4M"))
	fmt.Println(xconv.B("AM"))
	fmt.Println(xconv.B("44M"))
	fmt.Println(xconv.B("44MB"))
	fmt.Println(xconv.B("-44MB"))
	fmt.Println(xconv.B("44TB"))
}
