package kknet

import (
	"sync"
	"sync/atomic"
)

var (
	// counter for connection ID. unique id for the connection.
	connIDCounter atomic.Uint64
	idPool        []CONN_ID = make([]CONN_ID, 0, 64)
	idPoolMutex   sync.Mutex
)

// NextConnID returns a unique connection ID.
func NextConnID() CONN_ID {
	// idPoolMutex.Lock()
	// defer idPoolMutex.Unlock()
	// lastIdx := len(idPool) - 1
	// if lastIdx >= 0 {
	// 	id := idPool[lastIdx]
	// 	idPool[lastIdx] = 0
	// 	idPool = idPool[:lastIdx]
	// 	return id
	// }
	return connIDCounter.Add(1)
}

func FreeConnID(id CONN_ID) {
	idPoolMutex.Lock()
	idPool = append(idPool, id)
	idPoolMutex.Unlock()
}
