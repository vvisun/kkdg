package kktcp

import "github.com/panjf2000/gnet/v2/pkg/logging"

// gnetNopLogger implements logging.Logger and drops all gnet log output.
var gnetNopLogger = &gnetNopLog{}

type gnetNopLog struct{}

func (gnetNopLog) Debugf(format string, args ...any) {}
func (gnetNopLog) Infof(format string, args ...any)  {}
func (gnetNopLog) Warnf(format string, args ...any)  {}
func (gnetNopLog) Errorf(format string, args ...any) {}
func (gnetNopLog) Fatalf(format string, args ...any) {}

func init() {
	// Ensure type implements interface.
	_ = logging.Logger(gnetNopLogger)
}
