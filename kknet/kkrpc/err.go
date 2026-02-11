package kkrpc

import (
	"errors"
	"fmt"
)

func ErrRpc(code int32, msg string) error {
	if code == 0 {
		return nil
	}
	if msg == "" {
		return fmt.Errorf("rpc error: code=%d", code)
	}
	return errors.New(msg)
}
