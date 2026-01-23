package kklog

import "sync"

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

var (
	defaultLogger ILogger = Stdout()
	defMutex              = &sync.Mutex{}
	defLogLevel           = LOG_LEVEL_DEBUG
)

func SetDefaultLogger(l ILogger) {
	defMutex.Lock()
	defer defMutex.Unlock()
	defaultLogger = l
}

func SetDefaultLogLevel(level ELogLevel) {
	defMutex.Lock()
	defer defMutex.Unlock()
	defLogLevel = level
}

func Debugf(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_DEBUG {
		return
	}
	defaultLogger.Debugf(format, args...)
}

func Infof(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_INFO {
		return
	}
	defaultLogger.Infof(format, args...)
}

func Warnf(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_WARN {
		return
	}
	defaultLogger.Warnf(format, args...)
}

func Errorf(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_ERROR {
		return
	}
	defaultLogger.Errorf(format, args...)
}

func Fatalf(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_FATAL {
		return
	}
	defaultLogger.Fatalf(format, args...)
}

func Panicf(format string, args ...any) {
	if defLogLevel > LOG_LEVEL_PANIC {
		return
	}
	defaultLogger.Panicf(format, args...)
}

func Debug(args ...any) {
	if defLogLevel > LOG_LEVEL_DEBUG {
		return
	}
	defaultLogger.Debug(args...)
}

func Info(args ...any) {
	if defLogLevel > LOG_LEVEL_INFO {
		return
	}
	defaultLogger.Info(args...)
}

func Warn(args ...any) {
	if defLogLevel > LOG_LEVEL_WARN {
		return
	}
	defaultLogger.Warn(args...)
}

func Error(args ...any) {
	if defLogLevel > LOG_LEVEL_ERROR {
		return
	}
	defaultLogger.Error(args...)
}

func Fatal(args ...any) {
	if defLogLevel > LOG_LEVEL_FATAL {
		return
	}
	defaultLogger.Fatal(args...)
}

func Panic(args ...any) {
	if defLogLevel > LOG_LEVEL_PANIC {
		return
	}
	defaultLogger.Panic(args...)
}
