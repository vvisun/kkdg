package hubproto

import "errors"

type ErrorCode = int32

const (
	ErrorCodeSuccess ErrorCode = 0 + iota
	ErrorCodeFailed
)

var (
	ErrNotAuthed = errors.New("not authed")
)
