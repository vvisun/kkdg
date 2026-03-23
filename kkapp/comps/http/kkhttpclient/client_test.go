package kkhttpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_SyncAndAsyncRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.Header().Set("X-Test", "1")
			_, _ = w.Write([]byte(`{"msg":"pong"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL), WithStrictStatus(true))

	ctx := context.Background()

	// sync
	resp, err := c.SyncRequest(ctx, http.MethodGet, "/ping", nil, nil)
	if err != nil {
		t.Fatalf("SyncRequest err: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("SyncRequest status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Test"); got != "1" {
		t.Fatalf("SyncRequest header X-Test = %q, want %q", got, "1")
	}

	// async
	ch := c.AsyncRequest(ctx, http.MethodGet, "/ping", nil, nil)
	res := <-ch
	if res.Err != nil {
		t.Fatalf("AsyncRequest err: %v", res.Err)
	}
	if res.Resp.StatusCode != 200 {
		t.Fatalf("AsyncRequest status = %d, want 200", res.Resp.StatusCode)
	}
}

func TestClient_SyncAndAsyncJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"msg":"pong"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	c := NewClient(WithBaseURL(ts.URL), WithStrictStatus(true))
	ctx := context.Background()

	// sync json
	var out1 struct {
		Msg string `json:"msg"`
	}
	if err := c.SyncJSON(ctx, http.MethodGet, "/ping", nil, nil, &out1); err != nil {
		t.Fatalf("SyncJSON err: %v", err)
	}
	if out1.Msg != "pong" {
		t.Fatalf("SyncJSON msg = %q, want %q", out1.Msg, "pong")
	}

	// async json
	var out2 struct {
		Msg string `json:"msg"`
	}
	ch := c.AsyncJSON(ctx, http.MethodGet, "/ping", nil, nil, &out2)
	res := <-ch
	if res.Err != nil {
		t.Fatalf("AsyncJSON err: %v", res.Err)
	}
	if res.Resp.StatusCode != 200 {
		t.Fatalf("AsyncJSON status = %d, want 200", res.Resp.StatusCode)
	}
	if out2.Msg != "pong" {
		t.Fatalf("AsyncJSON msg = %q, want %q", out2.Msg, "pong")
	}
}

