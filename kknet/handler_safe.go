package kknet

import "github.com/vvisun/kkdg/utils/kklog"

// SafeHandlerCall runs fn and recovers from panics.
// It logs the panic and increments stats errors when available.
func SafeHandlerCall(logger kklog.ILogger, stats *Stats, label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if stats != nil {
				stats.AddError()
			}
			if logger != nil {
				logger.Errorf("%s handler panic: %v", label, r)
			}
		}
	}()
	fn()
}
