package kklog

import "fmt"

// 启动期间发生致命错误时直接panic，避免影响后续逻辑，产生不可预测的运行期错误
func PanicErr(err error) {
	if err == nil {
		return
	}
	Errorf("启动出错: %v", err)
	panic(err)
}

// 启动期间发生致命错误时直接panic，避免影响后续逻辑，产生不可预测的运行期错误
func PanicLog(format string, a ...interface{}) {
	Errorf(format, a...)
	panic(fmt.Sprintf(format, a...))
}
