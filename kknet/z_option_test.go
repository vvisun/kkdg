package kknet

import (
	"testing"
	"time"
)

func TestReconnectBackoff(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	// 0 次失败：base
	d := ReconnectBackoff(base, max, 0)
	if d < base || d > base+base/4 {
		t.Errorf("ReconnectBackoff(0 fails) = %v, want in [base, base+25%%]", d)
	}

	// 1 次失败：base
	d = ReconnectBackoff(base, max, 1)
	if d < base || d > base+base/4 {
		t.Errorf("ReconnectBackoff(1 fail) = %v, want in [base, base+25%%]", d)
	}

	// 2 次失败：2*base，有 jitter
	d = ReconnectBackoff(base, max, 2)
	if d < 2*base || d > 2*base+2*base/4 {
		t.Errorf("ReconnectBackoff(2 fails) = %v, want in [2*base, 2*base+25%%]", d)
	}

	// 多次失败：delay 被 cap 到 max，再加 [0, 25%] jitter，故可能略大于 max
	d = ReconnectBackoff(base, max, 10)
	if d > max+max/4 {
		t.Errorf("ReconnectBackoff(10 fails) = %v, want <= %v (max+jitter)", d, max+max/4)
	}
}

func TestDefaultOptions(t *testing.T) {
	opt := DefaultOptions()
	if opt.Logger == nil {
		t.Error("DefaultOptions Logger should be non-nil")
	}
	if opt.ReadBufferSize != 4*1024 {
		t.Errorf("DefaultOptions ReadBufferSize = %d, want 4096", opt.ReadBufferSize)
	}
	if opt.WriteBufferSize != 4*1024 {
		t.Errorf("DefaultOptions WriteBufferSize = %d, want 4096", opt.WriteBufferSize)
	}
	if opt.ShutdownTimeout != 10*time.Second {
		t.Errorf("DefaultOptions ShutdownTimeout = %v, want 10s", opt.ShutdownTimeout)
	}
	if !opt.IsNeedReconnect {
		t.Error("DefaultOptions IsNeedReconnect want true")
	}
	if opt.ReconnectInterval != 1*time.Second {
		t.Errorf("DefaultOptions ReconnectInterval = %v, want 1s", opt.ReconnectInterval)
	}
	if opt.ReconnectMaxInterval != 30*time.Second {
		t.Errorf("DefaultOptions ReconnectMaxInterval = %v, want 30s", opt.ReconnectMaxInterval)
	}
	if opt.ReconnectMaxRetries != 5 {
		t.Errorf("DefaultOptions ReconnectMaxRetries = %d, want 5", opt.ReconnectMaxRetries)
	}
	if opt.WsOriginChecker == nil {
		t.Error("DefaultOptions WsOriginChecker should be non-nil")
	}
}

func TestApplyOptions(t *testing.T) {
	opt := ApplyOptions(
		WithBufferSizes(8*1024, 16*1024),
		WithShutdownTimeout(20*time.Second),
		WithIsNeedReconnect(false),
		WithReconnectInterval(2*time.Second, 10),
		WithReadTimeout(30*time.Second),
		WithWriteTimeout(10*time.Second),
		WithPingInterval(10*time.Second),
		WithRecvQueueSize(512),
		WithSendQueueSize(256),
	)
	if opt.ReadBufferSize != 8*1024 {
		t.Errorf("ApplyOptions ReadBufferSize = %d, want 8192", opt.ReadBufferSize)
	}
	if opt.WriteBufferSize != 16*1024 {
		t.Errorf("ApplyOptions WriteBufferSize = %d, want 16384", opt.WriteBufferSize)
	}
	if opt.ShutdownTimeout != 20*time.Second {
		t.Errorf("ApplyOptions ShutdownTimeout = %v, want 20s", opt.ShutdownTimeout)
	}
	if opt.IsNeedReconnect {
		t.Error("ApplyOptions IsNeedReconnect want false")
	}
	if opt.ReconnectInterval != 2*time.Second {
		t.Errorf("ApplyOptions ReconnectInterval = %v, want 2s", opt.ReconnectInterval)
	}
	if opt.ReconnectMaxRetries != 10 {
		t.Errorf("ApplyOptions ReconnectMaxRetries = %d, want 10", opt.ReconnectMaxRetries)
	}
	if opt.RpOptions.RecvQueueSize != 512 {
		t.Errorf("ApplyOptions RecvQueueSize = %d, want 512", opt.RpOptions.RecvQueueSize)
	}
	if opt.WpOptions.SendQueueSize != 256 {
		t.Errorf("ApplyOptions SendQueueSize = %d, want 256", opt.WpOptions.SendQueueSize)
	}
}

func TestApplyOptions_nilIgnored(t *testing.T) {
	opt := ApplyOptions(
		WithBufferSizes(2*1024, 2*1024),
		nil,
		WithShutdownTimeout(5*time.Second),
	)
	if opt.ReadBufferSize != 2*1024 || opt.ShutdownTimeout != 5*time.Second {
		t.Errorf("ApplyOptions with nil: ReadBufferSize=%d ShutdownTimeout=%v", opt.ReadBufferSize, opt.ShutdownTimeout)
	}
}

func TestCheckOptions(t *testing.T) {
	t.Run("buffer_size_too_small", func(t *testing.T) {
		opt := DefaultOptions()
		opt.ReadBufferSize = 512
		opt.WriteBufferSize = 512
		CheckOptions(&opt)
		if opt.ReadBufferSize != 1024 {
			t.Errorf("CheckOptions ReadBufferSize = %d, want 1024", opt.ReadBufferSize)
		}
		if opt.WriteBufferSize != 1024 {
			t.Errorf("CheckOptions WriteBufferSize = %d, want 1024", opt.WriteBufferSize)
		}
	})
	t.Run("buffer_size_too_large", func(t *testing.T) {
		opt := DefaultOptions()
		opt.ReadBufferSize = 64 * 1024
		opt.WriteBufferSize = 64 * 1024
		CheckOptions(&opt)
		if opt.ReadBufferSize != 32*1024 {
			t.Errorf("CheckOptions ReadBufferSize = %d, want 32*1024", opt.ReadBufferSize)
		}
		if opt.WriteBufferSize != 32*1024 {
			t.Errorf("CheckOptions WriteBufferSize = %d, want 32*1024", opt.WriteBufferSize)
		}
	})
	t.Run("read_timeout_min", func(t *testing.T) {
		opt := DefaultOptions()
		opt.ReadTimeout = 100 * time.Millisecond
		CheckOptions(&opt)
		if opt.ReadTimeout != 1*time.Second {
			t.Errorf("CheckOptions ReadTimeout = %v, want 1s", opt.ReadTimeout)
		}
	})
	t.Run("reconnect_max_interval_cap", func(t *testing.T) {
		opt := DefaultOptions()
		opt.ReconnectMaxInterval = 0
		CheckOptions(&opt)
		if opt.ReconnectMaxInterval != 30*time.Second {
			t.Errorf("CheckOptions ReconnectMaxInterval = %v, want 30s", opt.ReconnectMaxInterval)
		}
	})
	t.Run("nil_safe", func(t *testing.T) {
		CheckOptions(nil)
	})
}

func TestDefaultReadOptions(t *testing.T) {
	opt := DefaultReadOptions()
	if opt.RecvQueueSize != 256 {
		t.Errorf("DefaultReadOptions RecvQueueSize = %d, want 256", opt.RecvQueueSize)
	}
	if opt.RecvBufShrinkCap != 2*1024 {
		t.Errorf("DefaultReadOptions RecvBufShrinkCap = %d, want 2048", opt.RecvBufShrinkCap)
	}
	if opt.WorkerQueueMaxConcurrency != 1 {
		t.Errorf("DefaultReadOptions WorkerQueueMaxConcurrency = %d, want 1", opt.WorkerQueueMaxConcurrency)
	}
}

func TestCheckReadOptions(t *testing.T) {
	t.Run("recv_queue_size_zero", func(t *testing.T) {
		opt := DefaultReadOptions()
		opt.RecvQueueSize = 0
		CheckReadOptions(&opt)
		if opt.RecvQueueSize != 256 {
			t.Errorf("CheckReadOptions RecvQueueSize = %d, want 256", opt.RecvQueueSize)
		}
	})
	t.Run("worker_concurrency_zero", func(t *testing.T) {
		opt := DefaultReadOptions()
		opt.WorkerQueueMaxConcurrency = 0
		CheckReadOptions(&opt)
		if opt.WorkerQueueMaxConcurrency != 1 {
			t.Errorf("CheckReadOptions WorkerQueueMaxConcurrency = %d, want 1", opt.WorkerQueueMaxConcurrency)
		}
	})
	t.Run("worker_concurrency_over_64", func(t *testing.T) {
		opt := DefaultReadOptions()
		opt.WorkerQueueMaxConcurrency = 100
		CheckReadOptions(&opt)
		if opt.WorkerQueueMaxConcurrency != 64 {
			t.Errorf("CheckReadOptions WorkerQueueMaxConcurrency = %d, want 64", opt.WorkerQueueMaxConcurrency)
		}
	})
	t.Run("nil_safe", func(t *testing.T) {
		CheckReadOptions(nil)
	})
}

func TestDefaultWriteOptions(t *testing.T) {
	opt := DefaultWriteOptions()
	if opt.SendQueueSize != 128 {
		t.Errorf("DefaultWriteOptions SendQueueSize = %d, want 128", opt.SendQueueSize)
	}
	if opt.BatchWriteLimitBytes != 1024 {
		t.Errorf("DefaultWriteOptions BatchWriteLimitBytes = %d, want 1024", opt.BatchWriteLimitBytes)
	}
	if opt.SendQueueFullAction != EWpQueueFullActionDrop {
		t.Errorf("DefaultWriteOptions SendQueueFullAction = %d, want Drop", opt.SendQueueFullAction)
	}
}

func TestCheckWriteOptions(t *testing.T) {
	t.Run("send_queue_size_zero", func(t *testing.T) {
		opt := DefaultWriteOptions()
		opt.SendQueueSize = 0
		CheckWriteOptions(&opt)
		if opt.SendQueueSize != 128 {
			t.Errorf("CheckWriteOptions SendQueueSize = %d, want 128", opt.SendQueueSize)
		}
	})

	t.Run("batch_write_limit_bytes_low", func(t *testing.T) {
		opt := DefaultWriteOptions()
		opt.BatchWriteLimitBytes = 100
		CheckWriteOptions(&opt)
		if opt.BatchWriteLimitBytes != 512 {
			t.Errorf("CheckWriteOptions BatchWriteLimitBytes = %d, want 512", opt.BatchWriteLimitBytes)
		}
	})
	t.Run("batch_write_limit_bytes_high", func(t *testing.T) {
		opt := DefaultWriteOptions()
		opt.BatchWriteLimitBytes = 8192
		CheckWriteOptions(&opt)
		if opt.BatchWriteLimitBytes != 2048 {
			t.Errorf("CheckWriteOptions BatchWriteLimitBytes = %d, want 2048", opt.BatchWriteLimitBytes)
		}
	})
	t.Run("nil_safe", func(t *testing.T) {
		CheckWriteOptions(nil)
	})
}
