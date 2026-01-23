package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkactor"
)

// Router is a router for the gate.
type Router struct {
	mu       sync.RWMutex
	connMgrs []kknet.IConnManager           // connection managers (for multiple servers)
	pidCache map[kknet.CONN_ID]*kkactor.PID // pid cache
}

// NewRouter creates a new router.
func NewRouter(connMgrs ...kknet.IConnManager) *Router {
	return &Router{
		connMgrs: connMgrs,
		pidCache: make(map[kknet.CONN_ID]*kkactor.PID),
	}
}

// AddConnManager adds a connection manager.
func (r *Router) AddConnManager(connMgr kknet.IConnManager) {
	r.mu.Lock()
	r.connMgrs = append(r.connMgrs, connMgr)
	r.mu.Unlock()
}

// AddPID adds a PID for a connection.
func (r *Router) AddPID(connID kknet.CONN_ID, pid *kkactor.PID) {
	r.mu.Lock()
	r.pidCache[connID] = pid
	r.mu.Unlock()
}

// RemovePID removes a PID for a connection.
func (r *Router) RemovePID(connID kknet.CONN_ID) {
	r.mu.Lock()
	delete(r.pidCache, connID)
	r.mu.Unlock()
}

// GetPID returns the PID for a connection.
func (r *Router) GetPID(connID kknet.CONN_ID) (*kkactor.PID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pid, ok := r.pidCache[connID]
	return pid, ok
}

// GetConnManager returns a combined connection manager view.
// It searches all connection managers for the connection.
func (r *Router) GetConnManager() kknet.IConnManager {
	return &combinedConnManager{router: r}
}

// combinedConnManager combines multiple connection managers
type combinedConnManager struct {
	router *Router
}

func (c *combinedConnManager) GetAllConns() map[int64]kknet.IConn {
	c.router.mu.RLock()
	defer c.router.mu.RUnlock()

	result := make(map[int64]kknet.IConn)
	for _, mgr := range c.router.connMgrs {
		if mgr != nil {
			conns := mgr.GetAllConns()
			for id, conn := range conns {
				result[id] = conn
			}
		}
	}
	return result
}

func (c *combinedConnManager) GetConn(id int64) kknet.IConn {
	c.router.mu.RLock()
	defer c.router.mu.RUnlock()

	for _, mgr := range c.router.connMgrs {
		if mgr != nil {
			if conn := mgr.GetConn(id); conn != nil {
				return conn
			}
		}
	}
	return nil
}

func (c *combinedConnManager) KickConn(id int64) {
	c.router.mu.RLock()
	defer c.router.mu.RUnlock()

	for _, mgr := range c.router.connMgrs {
		if mgr != nil {
			if mgr.GetConn(id) != nil {
				mgr.KickConn(id)
				return
			}
		}
	}
}
