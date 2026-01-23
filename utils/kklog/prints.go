package kklog

import "fmt"

// printsLogger is a logger implementation that prints to stdout.
type printsLogger struct{}

func (printsLogger) Debugf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func (printsLogger) Infof(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func (printsLogger) Warnf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func (printsLogger) Errorf(format string, args ...any) {
	fmt.Printf("-----------[error-----------\n")
	fmt.Printf(format+"\n", args...)
	fmt.Printf("-----------error]-----------\n")
}

func (printsLogger) Fatalf(format string, args ...any) {
	fmt.Printf("-----------[fatal-----------\n")
	fmt.Printf(format+"\n", args...)
	fmt.Printf("-----------fatal]-----------\n")
}

func (printsLogger) Panicf(format string, args ...any) {
	fmt.Printf("-----------[panic-----------\n")
	fmt.Printf(format+"\n", args...)
	fmt.Printf("-----------panic]-----------\n")
}

var printsLoggerInstance = printsLogger{}

// Stdout returns a logger that prints to stdout (debug-only convenience).
func Stdout() ILogger { return printsLoggerInstance }

func (printsLogger) Debug(args ...any) {
	fmt.Println(args...)
}

func (printsLogger) Info(args ...any) {
	fmt.Println(args...)
}

func (printsLogger) Warn(args ...any) {
	fmt.Println(args...)
}

func (printsLogger) Error(args ...any) {
	fmt.Printf("-----------[error-----------\n")
	fmt.Println(args...)
	fmt.Printf("-----------error]-----------\n")
}

func (printsLogger) Fatal(args ...any) {
	fmt.Printf("-----------[fatal-----------\n")
	fmt.Println(args...)
	fmt.Printf("-----------fatal]-----------\n")
}

func (printsLogger) Panic(args ...any) {
	fmt.Printf("-----------[panic-----------\n")
	fmt.Println(args...)
	fmt.Printf("-----------panic]-----------\n")
}
