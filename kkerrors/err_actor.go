package kkerrors

import "errors"

var (
	// actor 未找到
	ErrActorNotFound = errors.New("actor not found")
)
