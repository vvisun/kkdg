package kklog

type ELogLevel int

const (
	LOG_LEVEL_DEBUG ELogLevel = 0 // 调试日志
	LOG_LEVEL_INFO  ELogLevel = 1 // 信息日志
	LOG_LEVEL_WARN  ELogLevel = 2 // 警告日志
	LOG_LEVEL_ERROR ELogLevel = 3 // 错误日志
	LOG_LEVEL_FATAL ELogLevel = 4 // 严重错误日志
	LOG_LEVEL_PANIC ELogLevel = 5 // 恐慌日志
)

// ILogger is a minimal debug logger interface.
type ILogger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Panicf(format string, args ...any)
	Debug(args ...any)
	Info(args ...any)
	Warn(args ...any)
	Error(args ...any)
	Fatal(args ...any)
	Panic(args ...any)
}
