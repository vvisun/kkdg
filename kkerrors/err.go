package kkerrors

import "errors"

var (
	ErrNotFoundIPAddress = errors.New("not found ip address")
	ErrUnexpectedEOF     = errors.New("unexpected EOF")
	ErrInvalidWhence     = errors.New("invalid whence")
	ErrNegativePosition  = errors.New("negative position")
)
