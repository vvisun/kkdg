package xcall

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/vvisun/kkdg/utils/kklog"
)

// Try 尝试执行函数，如果执行过程中发生异常，则调用catchFn
// @param tryFn 要执行的函数
// @param catchFn 发生异常时调用的函数
// @return bool 是否发生异常
func Try(tryFn func(), catchFn func(errString string)) bool {
	var hasException = true
	func() {
		defer catchError(catchFn)
		tryFn()
		hasException = false
	}()
	return hasException
}

func catchError(catch func(errString string)) {
	if r := recover(); r != nil {
		catch(fmt.Sprint(r))
	}
}

// SafeCall 安全地调用函数
func SafeCall(fn func()) {
	if fn == nil {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			switch err.(type) {
			case runtime.Error:
				kklog.Errorf("[panic] runtime error: %v", err)
			default:
				kklog.Errorf("[panic] error: %v", err)
			}
		}
	}()

	fn()
}

// Go 执行单个协程
func Go(fn func()) {
	go SafeCall(fn)
}

// GoWithTimeout 执行多个协程（附带超时时间）
func GoWithTimeout(timeout time.Duration, fns ...func()) {
	NewGoroutines().Add(fns...).Run(context.Background(), timeout)
}

// GoWithDeadline 执行多个协程（附带最后期限）
func GoWithDeadline(deadline time.Time, fns ...func()) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	NewGoroutines().Add(fns...).Run(ctx)
}
