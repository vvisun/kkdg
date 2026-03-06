package kkdb

import (
	"errors"

	"github.com/vvisun/kkdg/utils/kklog"
)

type DBOption struct {
	DriverName  string
	StoreEngine string
	Charset     string
	Dsn         string //"DB用户名:DB密码@tcp(127.0.0.1:3306)/ddqp?charset=utf8mb4"
	MaxIdle     int
	MaxOpen     int
	ShowSql     bool //是否显示sql日志打印
}

func DefaultDBOption() DBOption {
	return DBOption{
		DriverName:  "mysql",
		StoreEngine: "InnoDB",
		Charset:     "utf8mb4",
		Dsn:         "root:LIKEsql123@tcp(127.0.0.1:3306)/ddqp?charset=utf8mb4",
		MaxIdle:     10,
		MaxOpen:     10,
		ShowSql:     false,
	}
}

func CheckOption(opt *DBOption) error {
	if opt.DriverName == "" {
		return errors.New("driver name is required")
	}
	if opt.StoreEngine == "" {
		return errors.New("store engine is required")
	}
	if opt.Charset == "" {
		return errors.New("charset is required")
	}
	if opt.Dsn == "" {
		return errors.New("dsn is required")
	}
	if opt.MaxIdle <= 0 {
		opt.MaxIdle = 10
		kklog.Warn("max idle is required, set to 10")
	}
	if opt.MaxOpen <= 0 {
		opt.MaxOpen = 10
		kklog.Warn("max open is required, set to 10")
	}
	return nil
}

func ApplyOptions(opts ...func(o *DBOption)) DBOption {
	cfg := DefaultDBOption()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func WithDriverName(driverName string) func(o *DBOption) {
	return func(o *DBOption) {
		o.DriverName = driverName
	}
}

func WithStoreEngine(storeEngine string) func(o *DBOption) {
	return func(o *DBOption) {
		o.StoreEngine = storeEngine
	}
}

func WithCharset(charset string) func(o *DBOption) {
	return func(o *DBOption) {
		o.Charset = charset
	}
}

func WithDsn(dsn string) func(o *DBOption) {
	return func(o *DBOption) {
		o.Dsn = dsn
	}
}

func WithMaxIdle(maxIdle int) func(o *DBOption) {
	return func(o *DBOption) {
		o.MaxIdle = maxIdle
	}
}

func WithMaxOpen(maxOpen int) func(o *DBOption) {
	return func(o *DBOption) {
		o.MaxOpen = maxOpen
	}
}

func WithShowSql(showSql bool) func(o *DBOption) {
	return func(o *DBOption) {
		o.ShowSql = showSql
	}
}
