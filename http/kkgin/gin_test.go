package kkgin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestRegisterRoutesAndMiddlewareAndCORS(t *testing.T) {
	opt := ApplyOptions(
		WithRegisterRoutes(func(engine *gin.Engine) {
			engine.GET("/ping", func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"msg": "pong"})
			})
			// gin-contrib/cors 的预检通常需要路由存在；这里显式注册 OPTIONS 便于测试头注入。
			engine.OPTIONS("/ping", func(c *gin.Context) {
				c.Status(http.StatusNoContent)
			})
		}),
		WithMiddlewares(func(c *gin.Context) {
			c.Header("X-MW", "1")
			c.Next()
		}),
	)

	srv := NewServer(opt)

	// Route + middleware + default CORS behavior.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	// Ensure host differs from Origin so CORS middleware treats it as cross-origin.
	req.Host = "localhost"
	req.Header.Set("Origin", "http://example.com")

	srv.GetEngine().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if got := rr.Header().Get("X-MW"); got != "1" {
		t.Fatalf("X-MW = %q, want %q", got, "1")
	}

	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("json unmarshal err: %v, body=%s", err, rr.Body.String())
	}
	if out["msg"] != "pong" {
		t.Fatalf("msg=%v, want pong", out["msg"])
	}

	// CORS: validate headers via preflight (OPTIONS) request.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodOptions, "/ping", nil)
	req2.Host = "localhost"
	req2.Header.Set("Origin", "http://example.com")
	req2.Header.Set("Access-Control-Request-Method", http.MethodGet)
	req2.Header.Set("Access-Control-Request-Headers", "Content-Type")
	srv.GetEngine().ServeHTTP(rr2, req2)
	if got := rr2.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatalf("missing Access-Control-Allow-Methods header on preflight, status=%d headers=%v", rr2.Code, rr2.Header())
	}
	if got := rr2.Header().Get("Access-Control-Allow-Headers"); got == "" {
		t.Fatalf("missing Access-Control-Allow-Headers header on preflight, status=%d headers=%v", rr2.Code, rr2.Header())
	}
}

func TestStartStop(t *testing.T) {
	opt := ApplyOptions(
		WithHttpAddr("127.0.0.1:0"),
		WithRegisterRoutes(func(engine *gin.Engine) {
			engine.GET("/ping", func(c *gin.Context) {
				c.JSON(http.StatusOK, gin.H{"msg": "pong"})
			})
		}),
		WithShutdownTimeout(2*time.Second),
	)

	srv := NewServer(opt)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start err: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	addr := srv.ListenAddr()
	if addr == "" {
		t.Fatalf("ListenAddr empty")
	}
	url := "http://" + addr + "/ping"

	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				lastErr = nil
				break
			}
			lastErr = err
		} else {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("wait server err: %v", lastErr)
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop err: %v", err)
	}
}

