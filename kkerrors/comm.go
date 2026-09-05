package kkerrors

import (
	"errors"
	"fmt"
)

// 格式化错误
//
//	@param mod: 模块名
//	@param format: 错误格式
//	@param args: 错误参数
//	@return error: 错误
func FormatErrorStr(mod string, format string, args ...any) error {
	return fmt.Errorf("[%s] "+format, append([]any{mod}, args...)...)
}

// 格式化错误
//
//	@param mod: 模块名
//	@param err: 错误
//	@param format: 错误格式
//	@param args: 错误参数
//	@return error: 错误
func FormatErrorErr(mod string, err error, format string, args ...any) error {
	return fmt.Errorf("[%s] "+format+": %w", append(append([]any{mod}, args...), err)...)
}

// 创建错误
//
//	@param text: 错误文本
//	@return error: 错误
//
// @return error: 错误
func Error(text string) error {
	return errors.New(text)
}

// 创建错误
//
//	@param format: 错误格式
//	@param a: 错误参数
//	@return error: 错误
func Errorf(format string, a ...interface{}) error {
	return Error(fmt.Sprintf(format, a...))
}

// 包装错误
//
//	@param err: 错误
//	@param context: 错误上下文
//	@return error: 错误
func Wrap(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}

// 包装错误
//
//	@param err: 错误
//	@param format: 错误格式
//	@param a: 错误参数
//	@return error: 错误
func Wrapf(err error, format string, a ...interface{}) error {
	text := fmt.Sprintf(format, a...)
	return Wrap(err, text)
}
