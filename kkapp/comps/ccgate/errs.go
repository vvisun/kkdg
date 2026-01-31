package ccgate

import "errors"

var (
	ErrClusterNotInitialized = errors.New("ccgate: cluster not initialized")
	ErrEmptySessionID        = errors.New("ccgate: empty sessionID")
	ErrEmptyMsgBytes         = errors.New("ccgate: empty msgBytes")
)
