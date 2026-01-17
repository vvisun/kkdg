package kklog

// nopLogger is a no-op logger implementation.
type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}
func (nopLogger) Fatalf(string, ...any) {}

var nopLoggerInstance = nopLogger{}

func Nop() ILogger { return nopLoggerInstance }
