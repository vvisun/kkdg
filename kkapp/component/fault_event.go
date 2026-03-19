package component

import (
	"fmt"
	"runtime"
)

func normalizeFailureReason(reason any) (reasonStr string, isPanic bool) {
	if reason == nil {
		return "", false
	}

	// protoactor-go typically passes the panic value as the "reason".
	// runtime.Error is a common case.
	if _, ok := reason.(runtime.Error); ok {
		return fmt.Sprintf("%v", reason), true
	}

	// An error is not necessarily a panic, but still provides useful content.
	if _, ok := reason.(error); ok {
		return fmt.Sprintf("%v", reason), false
	}

	// For non-error/non-runtime.Error types, we treat it as a panic-ish payload.
	return fmt.Sprintf("%v", reason), true
}
