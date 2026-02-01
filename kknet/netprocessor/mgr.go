package netprocessor

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type Processor struct {
	readProcessor  *ReadProcessor
	writeProcessor *WriteProcessor
}

type ProcessorManager struct {
	connIdCache sync.Map // connID -> Processor
	uidCache    sync.Map // uid -> Processor
}

func NewProcessorManager() *ProcessorManager {
	return &ProcessorManager{}
}

func (m *ProcessorManager) AddConn(connID kknet.CONN_ID, processor *Processor) {
	if processor == nil {
		return
	}
	if processor.readProcessor != nil {
		processor.readProcessor.connID = connID
	}
	if processor.writeProcessor != nil {
		processor.writeProcessor.connID = connID
	}
	m.connIdCache.Store(connID, processor)
}

func (m *ProcessorManager) BindUserID(connID kknet.CONN_ID, uid kknet.USER_ID) bool {
	v, ok := m.connIdCache.Load(connID)
	if !ok {
		return false
	}
	processor := v.(*Processor)
	if processor == nil {
		return false
	}
	if processor.readProcessor != nil {
		processor.readProcessor.userID = uid
	}
	if processor.writeProcessor != nil {
		processor.writeProcessor.userID = uid
	}
	m.uidCache.Store(uid, processor)
	return true
}

func (m *ProcessorManager) UnbindUserID(connID kknet.CONN_ID) bool {
	v, ok := m.connIdCache.Load(connID)
	if !ok {
		return false
	}
	processor := v.(*Processor)
	if processor == nil {
		return false
	}

	var userID kknet.USER_ID = 0
	if processor.readProcessor != nil {
		userID = processor.readProcessor.userID
		processor.readProcessor.userID = 0
	}
	if processor.writeProcessor != nil {
		userID = processor.writeProcessor.userID
		processor.writeProcessor.userID = 0
	}
	m.uidCache.Delete(userID)
	return true
}

func (m *ProcessorManager) GetByConnID(connID kknet.CONN_ID) *Processor {
	v, ok := m.connIdCache.Load(connID)
	if !ok {
		return nil
	}
	return v.(*Processor)
}

func (m *ProcessorManager) GetByUserID(uid kknet.USER_ID) *Processor {
	v, ok := m.uidCache.Load(uid)
	if !ok {
		return nil
	}
	return v.(*Processor)
}
