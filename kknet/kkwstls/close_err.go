package kkwstls

import (
	"errors"
	"io"
	"net"
	"syscall"
)

func isEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

// isExpectedCloseErr returns true for errors that commonly indicate an expected
// connection close (peer closed, connection reset, etc.) and should not be
// counted as AddError in stats.
func isExpectedCloseErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	return false
}
