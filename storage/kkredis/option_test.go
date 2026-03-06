package kkredis

import (
	"testing"
)

func TestDefaultRedisOption(t *testing.T) {
	opt := DefaultRedisOption()
	if opt.Addr != "127.0.0.1:6379" {
		t.Errorf("DefaultRedisOption Addr = %s, want 127.0.0.1:6379", opt.Addr)
	}
	if opt.DB != 0 {
		t.Errorf("DefaultRedisOption DB = %d, want 0", opt.DB)
	}
	if opt.PoolSize != 0 {
		t.Errorf("DefaultRedisOption PoolSize = %d, want 0", opt.PoolSize)
	}
}

func TestApplyOptions(t *testing.T) {
	opt := ApplyOptions(
		WithAddr("localhost:6380"),
		WithPassword("secret"),
		WithDB(1),
		WithPoolSize(20),
		WithMinIdleConns(5),
		WithUsername("admin"),
	)
	if opt.Addr != "localhost:6380" {
		t.Errorf("ApplyOptions Addr = %s, want localhost:6380", opt.Addr)
	}
	if opt.Password != "secret" {
		t.Errorf("ApplyOptions Password = %s, want secret", opt.Password)
	}
	if opt.DB != 1 {
		t.Errorf("ApplyOptions DB = %d, want 1", opt.DB)
	}
	if opt.PoolSize != 20 {
		t.Errorf("ApplyOptions PoolSize = %d, want 20", opt.PoolSize)
	}
	if opt.MinIdleConns != 5 {
		t.Errorf("ApplyOptions MinIdleConns = %d, want 5", opt.MinIdleConns)
	}
	if opt.Username != "admin" {
		t.Errorf("ApplyOptions Username = %s, want admin", opt.Username)
	}
}

func TestApplyOptions_nilOptIgnored(t *testing.T) {
	opt := ApplyOptions(
		WithAddr("a:6379"),
		nil,
		WithDB(2),
	)
	if opt.Addr != "a:6379" || opt.DB != 2 {
		t.Errorf("ApplyOptions with nil: Addr=%s DB=%d", opt.Addr, opt.DB)
	}
}

func TestCheckOption(t *testing.T) {
	t.Run("empty_addr", func(t *testing.T) {
		opt := DefaultRedisOption()
		opt.Addr = ""
		err := CheckOption(&opt)
		if err == nil {
			t.Error("CheckOption(empty Addr) want error, got nil")
		}
	})
	t.Run("valid", func(t *testing.T) {
		opt := RedisOption{Addr: "127.0.0.1:6379"}
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(valid) err = %v", err)
		}
	})
	t.Run("negative_pool_size_fixed", func(t *testing.T) {
		opt := RedisOption{Addr: "x:6379", PoolSize: -1}
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(negative PoolSize) err = %v", err)
		}
		if opt.PoolSize != 0 {
			t.Errorf("CheckOption should fix PoolSize to 0, got %d", opt.PoolSize)
		}
	})
	t.Run("negative_db_fixed", func(t *testing.T) {
		opt := RedisOption{Addr: "x:6379", DB: -2}
		err := CheckOption(&opt)
		if err != nil {
			t.Errorf("CheckOption(negative DB) err = %v", err)
		}
		if opt.DB != 0 {
			t.Errorf("CheckOption should fix DB to 0, got %d", opt.DB)
		}
	})
}
