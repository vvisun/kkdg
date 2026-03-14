package kkpacket

import (
	"reflect"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
)

type routerTestMsg struct {
	ID int
}

func TestMsgRouter_RegisterAndGetters_Success(t *testing.T) {
	router := NewMsgRouter()
	const id MSGID = 10
	const route = "/router/test"

	// register pointer type
	if err := router.Register(id, &routerTestMsg{}, route); err != nil {
		t.Fatalf("Register() error = %v, want nil", err)
	}

	// GetMsgID should return the registered id for the same pointer type
	gotID := router.GetMsgID(&routerTestMsg{})
	if gotID != id {
		t.Fatalf("GetMsgID() = %d, want %d", gotID, id)
	}

	// GetMsgType should return the original type
	gotType := router.GetMsgType(id)
	wantType := reflect.TypeOf(&routerTestMsg{})
	if gotType != wantType {
		t.Fatalf("GetMsgType() = %v, want %v", gotType, wantType)
	}

	// GetMsgRoute should return the registered route
	gotRoute, err := router.GetMsgRoute(id)
	if err != nil {
		t.Fatalf("GetMsgRoute() error = %v, want nil", err)
	}
	if gotRoute != route {
		t.Fatalf("GetMsgRoute() = %q, want %q", gotRoute, route)
	}
}

func TestMsgRouter_Register_InvalidID(t *testing.T) {
	router := NewMsgRouter()

	if err := router.Register(0, &routerTestMsg{}, "/invalid"); err != kkerrors.ErrPktInvalidMsgID {
		t.Fatalf("Register(0, ...) error = %v, want ErrInvalidMsgID", err)
	}
}

func TestMsgRouter_Register_NonPointer(t *testing.T) {
	router := NewMsgRouter()

	// non-pointer should be rejected
	if err := router.Register(1, routerTestMsg{}, "/nonptr"); err != kkerrors.ErrPktInvalidMessage {
		t.Fatalf("Register(non-pointer) error = %v, want ErrInvalidMessage", err)
	}
}

func TestMsgRouter_Register_DuplicateID(t *testing.T) {
	router := NewMsgRouter()
	const id MSGID = 20

	if err := router.Register(id, &routerTestMsg{}, "/dup1"); err != nil {
		t.Fatalf("first Register() error = %v, want nil", err)
	}

	type routerTestMsg2 struct {
		ID int
	}
	if err := router.Register(id, &routerTestMsg2{}, "/dup2"); err != kkerrors.ErrPktMsgIDAlreadyRegistered {
		t.Fatalf("second Register() error = %v, want ErrPktMsgIDAlreadyRegistered", err)
	}
}

func TestMsgRouter_Getters_Unregistered(t *testing.T) {
	router := NewMsgRouter()

	// unregistered type should return 0 id
	if id := router.GetMsgID(&routerTestMsg{}); id != 0 {
		t.Fatalf("GetMsgID(unregistered) = %d, want 0", id)
	}

	// unregistered id should return nil type
	if tp := router.GetMsgType(999); tp != nil {
		t.Fatalf("GetMsgType(unregistered) = %v, want nil", tp)
	}

	// unregistered id should return ErrMsgIDNotRegistered
	if route, err := router.GetMsgRoute(999); err != kkerrors.ErrPktMsgIDNotRegistered || route != "" {
		t.Fatalf("GetMsgRoute(unregistered) = (%q, %v), want (\"\", ErrMsgIDNotRegistered)", route, err)
	}
}
