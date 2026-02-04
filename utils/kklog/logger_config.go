package kklog

import (
	"time"

	"go.uber.org/zap/zapcore"
)

type (
	Config struct {
		LogLevel        string `json:"level"`             // 输出日志等级
		StackLevel      string `json:"stack_level"`       // 堆栈输出日志等级
		EnableConsole   bool   `json:"enable_console"`    // 是否控制台输出
		EnableWriteFile bool   `json:"enable_write_file"` // 是否输出文件(必需配置FilePath)
		MaxAge          int    `json:"max_age"`           // 最大保留天数(达到限制，则会被清理)
		TimeFormat      string `json:"time_format"`       // 打印时间输出格式
		PrintCaller     bool   `json:"print_caller"`      // 是否打印调用函数
		RotationTime    int    `json:"rotation_time"`     // 日期分割时间(秒)
		FileLinkPath    string `json:"file_link_path"`    // 日志文件连接路径
		FilePathFormat  string `json:"file_path_format"`  // 日志文件路径格式
		IncludeStdout   bool   `json:"include_stdout"`    // 是否包含os.stdout输出
		IncludeStderr   bool   `json:"include_stderr"`    // 是否包含os.stderr输出
	}
)

func defaultConsoleConfig() *Config {
	config := &Config{
		LogLevel:        "debug",
		StackLevel:      "error",
		EnableConsole:   true,
		EnableWriteFile: false,
		MaxAge:          7,
		TimeFormat:      "15:04:05.000", //2006-01-02 15:04:05.000
		PrintCaller:     true,
		RotationTime:    86400,
		FileLinkPath:    "logs/debug.log",
		FilePathFormat:  "logs/debug_%Y%m%d%H%M.log",
		IncludeStdout:   false,
		IncludeStderr:   false,
	}
	return config
}

func NewConfig(jsonConfig Config) (*Config, error) {
	config := &Config{}
	*config = jsonConfig
	return config, nil
}

func NewConfigWithName(refLoggerName string) (*Config, error) {
	return NewConfig(Config{})
}

func (c *Config) TimeEncoder() zapcore.TimeEncoder {
	return func(time time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(time.Format(c.TimeFormat))
	}
}
