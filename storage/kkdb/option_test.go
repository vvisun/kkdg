package kkdb

import (
	"testing"
)

func TestDefaultDBOption(t *testing.T) {
	opt := DefaultDBOption()
	if opt.DriverName != "mysql" {
		t.Errorf("DefaultDBOption DriverName = %s, want mysql", opt.DriverName)
	}
	if opt.StoreEngine != "InnoDB" {
		t.Errorf("DefaultDBOption StoreEngine = %s, want InnoDB", opt.StoreEngine)
	}
	if opt.Charset != "utf8mb4" {
		t.Errorf("DefaultDBOption Charset = %s, want utf8mb4", opt.Charset)
	}
	if opt.Dsn == "" {
		t.Error("DefaultDBOption Dsn should be non-empty")
	}
	if opt.MaxIdle != 10 {
		t.Errorf("DefaultDBOption MaxIdle = %d, want 10", opt.MaxIdle)
	}
	if opt.MaxOpen != 10 {
		t.Errorf("DefaultDBOption MaxOpen = %d, want 10", opt.MaxOpen)
	}
	if opt.ShowSql != false {
		t.Errorf("DefaultDBOption ShowSql = %t, want false", opt.ShowSql)
	}
}

func TestApplyOptions(t *testing.T) {
	opt := ApplyOptions(
		WithDriverName("postgres"),
		WithStoreEngine("default"),
		WithCharset("utf8"),
		WithDsn("host=localhost dbname=test"),
		WithMaxIdle(5),
		WithMaxOpen(20),
		WithShowSql(true),
	)
	if opt.DriverName != "postgres" {
		t.Errorf("ApplyOptions DriverName = %s, want postgres", opt.DriverName)
	}
	if opt.StoreEngine != "default" {
		t.Errorf("ApplyOptions StoreEngine = %s, want default", opt.StoreEngine)
	}
	if opt.Charset != "utf8" {
		t.Errorf("ApplyOptions Charset = %s, want utf8", opt.Charset)
	}
	if opt.Dsn != "host=localhost dbname=test" {
		t.Errorf("ApplyOptions Dsn = %s, want host=localhost dbname=test", opt.Dsn)
	}
	if opt.MaxIdle != 5 {
		t.Errorf("ApplyOptions MaxIdle = %d, want 5", opt.MaxIdle)
	}
	if opt.MaxOpen != 20 {
		t.Errorf("ApplyOptions MaxOpen = %d, want 20", opt.MaxOpen)
	}
	if !opt.ShowSql {
		t.Error("ApplyOptions ShowSql = false, want true")
	}
}

func TestApplyOptions_nilOptIgnored(t *testing.T) {
	opt := ApplyOptions(
		WithDsn("custom_dsn"),
		nil,
		WithMaxIdle(3),
	)
	if opt.Dsn != "custom_dsn" || opt.MaxIdle != 3 {
		t.Errorf("ApplyOptions with nil: Dsn=%s MaxIdle=%d", opt.Dsn, opt.MaxIdle)
	}
}

func TestCheckOption(t *testing.T) {
	t.Run("empty_driver_name", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.DriverName = ""
		err := CheckOption(&opt)
		if err == nil {
			t.Error("CheckOption(empty DriverName) want error, got nil")
		}
	})
	t.Run("empty_store_engine", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.StoreEngine = ""
		err := CheckOption(&opt)
		if err == nil {
			t.Error("CheckOption(empty StoreEngine) want error, got nil")
		}
	})
	t.Run("empty_charset", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.Charset = ""
		err := CheckOption(&opt)
		if err == nil {
			t.Error("CheckOption(empty Charset) want error, got nil")
		}
	})
	t.Run("empty_dsn", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.Dsn = ""
		err := CheckOption(&opt)
		if err == nil {
			t.Error("CheckOption(empty Dsn) want error, got nil")
		}
	})
	t.Run("valid", func(t *testing.T) {
		opt := DefaultDBOption()
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(valid) err = %v", err)
		}
	})
	t.Run("max_idle_zero_fixed", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.MaxIdle = 0
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(zero MaxIdle) err = %v", err)
		}
		if opt.MaxIdle != 10 {
			t.Errorf("CheckOption should fix MaxIdle to 10, got %d", opt.MaxIdle)
		}
	})
	t.Run("max_open_negative_fixed", func(t *testing.T) {
		opt := DefaultDBOption()
		opt.MaxOpen = -1
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(negative MaxOpen) err = %v", err)
		}
		if opt.MaxOpen != 10 {
			t.Errorf("CheckOption should fix MaxOpen to 10, got %d", opt.MaxOpen)
		}
	})
}
