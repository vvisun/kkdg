package kknet

import "github.com/vvisun/kkdg/utils/kklog"

type ConnStatus = int32 //连接状态

const (
	ConnStatusInit         ConnStatus = iota //初始状态
	ConnStatusConnecting                     //连接中
	ConnStatusConnected                      //已连接
	ConnStatusReconnecting                   //重连中
	ConnStatusReconnected                    //已重连
	ConnStatusClosing                        //关闭中
	ConnStatusClosed                         //已关闭
)

// IsConnected 判断连接状态是否为连接已建立
func IsConnected(status ConnStatus) bool {
	return status == ConnStatusConnected || status == ConnStatusReconnected
}

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
