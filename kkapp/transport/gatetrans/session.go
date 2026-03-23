package gatetrans

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

// manager for client connections.
type ISessionManager interface {
	// GetConn gets a client connection by sessionID
	GetConn(sessionID string) (kknet.IConn, error)
	// AddConn adds a client connection by sessionID
	AddConn(sessionID string, conn kknet.IConn)
	// RemoveConn removes a client connection by sessionID
	RemoveConn(sessionID string)
	// CloseConn closes a client by sessionID
	CloseConn(sessionID string)
}

type SessionManager struct {
	connMap sync.Map // sessionID(string) -> kknet.IConn (client connection)
}

var _ ISessionManager = (*SessionManager)(nil)

func NewSessionMgr() ISessionManager {
	return &SessionManager{
		connMap: sync.Map{},
	}
}

func (slf *SessionManager) GetConn(sessionID string) (kknet.IConn, error) {
	if sessionID == "" {
		return nil, kkerrors.ErrAppEmptySessionID
	}
	v, ok := slf.connMap.Load(sessionID)
	if !ok {
		return nil, kkerrors.ErrAppSessionNotFound
	}
	return v.(kknet.IConn), nil
}

func (slf *SessionManager) AddConn(sessionID string, conn kknet.IConn) {
	if sessionID == "" {
		return
	}
	if conn == nil {
		return
	}
	slf.connMap.Store(sessionID, conn)
}

func (slf *SessionManager) RemoveConn(sessionID string) {
	if sessionID == "" {
		return
	}
	slf.connMap.Delete(sessionID)
}

func (slf *SessionManager) CloseConn(sessionID string) {
	if sessionID == "" {
		return
	}
	conn, err := slf.GetConn(sessionID)
	if err != nil {
		return
	}
	conn.Close()
}
