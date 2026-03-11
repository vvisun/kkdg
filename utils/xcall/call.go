package xcall

import (
	"context"
	"runtime"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/vvisun/kkdg/utils/kklog"
)

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

// AntsGo 使用 ants 提交协程，提交失败时，退避到普通协程
// @param fn 要执行的函数
func AntsGo(fn func()) {
	err := ants.Submit(fn)
	// 提交失败时，退避到普通协程
	if err != nil {
		go fn()
	}
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
