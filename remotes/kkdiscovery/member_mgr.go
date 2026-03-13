package kkdiscovery

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xrand"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MemberMgr struct {
	members   map[string]IMember   // key: nodeID, value: member
	typeMap   map[string][]IMember // key: nodeType, value: members
	membersMu sync.RWMutex

	addListeners    []MemberListener
	removeListeners []MemberListener
	listenersMu     sync.RWMutex

	logger kklog.ILogger
}

var _ IMemberMgr = (*MemberMgr)(nil)

func NewMemberMgr() *MemberMgr {
	return &MemberMgr{
		members:         make(map[string]IMember),
		typeMap:         make(map[string][]IMember),
		addListeners:    make([]MemberListener, 0),
		removeListeners: make([]MemberListener, 0),
		logger:          kklog.Nop(),
	}
}

func (m *MemberMgr) SetLogger(logger kklog.ILogger) {
	m.logger = logger
}

// 添加或更新成员
func (m *MemberMgr) AddMember(info *MemberInfo) IMember {
	m.membersMu.Lock()
	member, existed := m.members[info.NodeID]
	if !existed {
		member = &Member{
			nodeID:   info.NodeID,
			nodeType: info.NodeType,
			address:  info.Address,
			weight:   info.Weight,
			status:   info.Status,
			settings: info.Settings,
		}
		m.members[info.NodeID] = member
		m.typeMap[info.NodeType] = append(m.typeMap[info.NodeType], member)
	} else {
		oldType := member.GetNodeType()
		if oldType != info.NodeType {
			// 如果节点类型发生变化，则需要将成员从旧的类型列表中删除，再添加到新的类型列表中
			oldTypeList := m.typeMap[oldType]
			for i, oldMember := range oldTypeList {
				if oldMember.GetNodeID() == info.NodeID {
					oldTypeList = append(oldTypeList[:i], oldTypeList[i+1:]...)
					break
				}
			}
			m.typeMap[oldType] = oldTypeList
			m.typeMap[info.NodeType] = append(m.typeMap[info.NodeType], member)
		}
		mb := member.(*Member)
		mb.nodeType = info.NodeType
		mb.address = info.Address
		mb.weight = info.Weight
		mb.status = info.Status
		mb.settings = info.Settings
	}
	m.membersMu.Unlock()

	if !existed {
		m.notifyAddListeners(member)
		if m.logger != nil {
			m.logger.Debugf("discovery add member... %v", info)
		}
	} else {
		if m.logger != nil {
			m.logger.Debugf("discovery upd member... %v", info)
		}
	}
	return member
}

// 删除成员
func (m *MemberMgr) RemoveMember(nodeID string) {
	m.membersMu.Lock()
	member, existed := m.members[nodeID]
	if existed {
		delete(m.members, nodeID)
		oldType := member.GetNodeType()
		oldTypeList := m.typeMap[oldType]
		for i, oldMember := range oldTypeList {
			if oldMember.GetNodeID() == nodeID {
				oldTypeList = append(oldTypeList[:i], oldTypeList[i+1:]...)
				break
			}
		}
		m.typeMap[oldType] = oldTypeList
	}
	m.membersMu.Unlock()

	if existed {
		m.notifyRemoveListeners(member)
		if m.logger != nil {
			m.logger.Debugf("discovery del member... %v", member)
		}
	}
}

// 获取成员
func (m *MemberMgr) GetMember(nodeID string) (IMember, bool) {
	m.membersMu.RLock()
	member, ok := m.members[nodeID]
	m.membersMu.RUnlock()
	if !ok {
		return nil, false
	}
	return member, true
}

// 获取成员数量
func (m *MemberMgr) MemberCount() int {
	m.membersMu.RLock()
	count := len(m.members)
	m.membersMu.RUnlock()
	return count
}

// 遍历成员, fn返回false时停止遍历
func (m *MemberMgr) Range(fn func(nodeID string, member IMember) bool) {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	for nodeID, member := range m.members {
		if !fn(nodeID, member) {
			break
		}
	}
}

// 根据节点类型获取成员列表
func (m *MemberMgr) ListByType(nodeType string) []IMember {
	m.membersMu.RLock()
	listOfType := m.typeMap[nodeType]
	m.membersMu.RUnlock()
	return listOfType
}

// 根据节点类型随机一个成员
func (m *MemberMgr) random(nodeType string) (IMember, bool) {
	m.membersMu.RLock()
	listOfType := m.typeMap[nodeType]
	m.membersMu.RUnlock()
	if len(listOfType) == 0 {
		return nil, false
	}
	idx := xrand.Int(0, len(listOfType)-1)
	return listOfType[idx], true
}

// 根据节点id获取成员类型
func (m *MemberMgr) GetType(nodeID string) (string, error) {
	m.membersMu.RLock()
	member, ok := m.members[nodeID]
	m.membersMu.RUnlock()
	if !ok {
		return "", kkerrors.ErrClusterMemberNotFound
	}
	return member.GetNodeType(), nil
}

// 监听添加成员
func (m *MemberMgr) ObserveAddMember(listener MemberListener) {
	if listener == nil {
		return
	}
	m.listenersMu.Lock()
	for _, l := range m.addListeners {
		if xreflect.IsSameFunc(l, listener) {
			m.listenersMu.Unlock()
			return // already exists
		}
	}
	m.addListeners = append(m.addListeners, listener)
	m.listenersMu.Unlock()
}

// 监听移除成员
func (m *MemberMgr) ObserveRemoveMember(listener MemberListener) {
	if listener == nil {
		return
	}
	m.listenersMu.Lock()
	for _, l := range m.removeListeners {
		if xreflect.IsSameFunc(l, listener) {
			m.listenersMu.Unlock()
			return // already exists
		}
	}
	m.removeListeners = append(m.removeListeners, listener)
	m.listenersMu.Unlock()
}

// notifyAddListeners 通知添加
func (m *MemberMgr) notifyAddListeners(member IMember) {
	m.listenersMu.RLock()
	listeners := make([]MemberListener, len(m.addListeners))
	copy(listeners, m.addListeners)
	m.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("MemberMgr add listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}

// notifyRemoveListeners 通知移除
func (m *MemberMgr) notifyRemoveListeners(member IMember) {
	m.listenersMu.RLock()
	listeners := make([]MemberListener, len(m.removeListeners))
	copy(listeners, m.removeListeners)
	m.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("MemberMgr remove listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}
