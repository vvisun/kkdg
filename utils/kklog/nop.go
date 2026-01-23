package kklog

type nopLogger struct{}

func (nopLogger) Debugf(format string, args ...any) {}
func (nopLogger) Infof(format string, args ...any)  {}
func (nopLogger) Warnf(format string, args ...any)  {}
func (nopLogger) Errorf(format string, args ...any) {}
func (nopLogger) Fatalf(format string, args ...any) {}
func (nopLogger) Panicf(format string, args ...any) {}

var nopLoggerInstance = nopLogger{}

func (nopLogger) Debug(args ...any) {}
func (nopLogger) Info(args ...any)  {}
func (nopLogger) Warn(args ...any)  {}
func (nopLogger) Error(args ...any) {}
func (nopLogger) Fatal(args ...any) {}
func (nopLogger) Panic(args ...any) {}

func Nop() ILogger { return nopLoggerInstance }
