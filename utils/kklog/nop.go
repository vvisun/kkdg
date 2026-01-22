package kklog

type nopLogger struct{}

func (nopLogger) Debugf(format string, args ...any) {}
func (nopLogger) Infof(format string, args ...any)  {}
func (nopLogger) Warnf(format string, args ...any)  {}
func (nopLogger) Errorf(format string, args ...any) {}
func (nopLogger) Fatalf(format string, args ...any) {}
func (nopLogger) Panicf(format string, args ...any) {}

var nopLoggerInstance = nopLogger{}

func Nop() ILogger { return nopLoggerInstance }
