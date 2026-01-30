package gnrpc

import (
	"errors"
	"fmt"
)

// Code is a gRPC-like status code (subset).
type Code int32

const (
	CodeOK               Code = 0
	CodeCanceled         Code = 1
	CodeUnknown          Code = 2
	CodeInvalidArgument  Code = 3
	CodeDeadlineExceeded Code = 4
	CodeNotFound         Code = 5
	CodeAlreadyExists    Code = 6
	CodePermissionDenied Code = 7
	CodeResourceExhausted Code = 8
	CodeFailedPrecondition Code = 9
	CodeAborted          Code = 10
	CodeOutOfRange       Code = 11
	CodeUnimplemented    Code = 12
	CodeInternal         Code = 13
	CodeUnavailable      Code = 14
	CodeDataLoss         Code = 15
	CodeUnauthenticated  Code = 16
)

// StatusError represents a remote or local RPC error with a code.
type StatusError struct {
	Code Code
	Msg  string
}

func (e StatusError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("gnrpc: code=%d", e.Code)
	}
	return fmt.Sprintf("gnrpc: code=%d msg=%s", e.Code, e.Msg)
}

func Status(code Code, msg string) error {
	if code == CodeOK {
		return nil
	}
	return StatusError{Code: code, Msg: msg}
}

func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var se StatusError
	if errors.As(err, &se) {
		return se.Code
	}
	if errors.Is(err, ErrDeadlineExceeded) {
		return CodeDeadlineExceeded
	}
	if errors.Is(err, contextCanceled()) {
		return CodeCanceled
	}
	if errors.Is(err, ErrMethodNotFound) {
		return CodeNotFound
	}
	return CodeUnknown
}

func MsgOf(err error) string {
	if err == nil {
		return ""
	}
	var se StatusError
	if errors.As(err, &se) {
		return se.Msg
	}
	return err.Error()
}

