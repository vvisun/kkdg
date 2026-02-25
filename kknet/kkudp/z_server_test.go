package kkudp

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

func freeUDPPort(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	return addr
}

func TestServer_NewServer(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, kknet.DefaultOptions())
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.Addr() != "127.0.0.1:0" {
		t.Errorf("Addr() = %q, want 127.0.0.1:0", s.Addr())
	}
	if s.GetConnManager() == nil {
		t.Error("GetConnManager() returned nil")
	}
}

func TestServer_Start_Stop(t *testing.T) {
	addr := freeUDPPort(t)
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stats := s.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("ActiveConns = %d, want 0", stats.ActiveConns)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := s.Stop(); err != kkerrors.ErrServerNotStarted {
		t.Errorf("second Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Stop_WithoutStart(t *testing.T) {
	s := NewServer("127.0.0.1:0", nil, kknet.DefaultOptions())
	err := s.Stop()
	if err != kkerrors.ErrServerNotStarted {
		t.Errorf("Stop() = %v, want ErrServerNotStarted", err)
	}
}

func TestServer_Addr_GetConnManager(t *testing.T) {
	addr := freeUDPPort(t)
	s := NewServer(addr, nil, kknet.DefaultOptions())
	if s.Addr() != addr {
		t.Errorf("Addr() = %q, want %q", s.Addr(), addr)
	}
	mgr := s.GetConnManager()
	if mgr == nil {
		t.Fatal("GetConnManager() returned nil")
	}
	if mgr.GetCount() != 0 {
		t.Errorf("GetCount() = %d, want 0", mgr.GetCount())
	}
	_ = s.Start()
	defer s.Stop()
	if s.Addr() != addr {
		t.Errorf("Addr() after Start = %q", s.Addr())
	}
}
