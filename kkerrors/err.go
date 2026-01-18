package kkerrors

import "fmt"

// 格式化错误
// mod: 模块名
// format: 错误格式
// args: 错误参数
func FormatErrorStr(mod string, format string, args ...any) error {
	return fmt.Errorf("[%s] "+format, append([]any{mod}, args...)...)
}

// 格式化错误
// mod: 模块名
// err: 错误
// format: 错误格式
// args: 错误参数
func FormatErrorErr(mod string, err error, format string, args ...any) error {
	return fmt.Errorf("[%s] "+format+": %w", append(append([]any{mod}, args...), err)...)
}

// 包装错误
// err: 错误
// context: 错误上下文
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}
