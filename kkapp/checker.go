package kkapp

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	// 节点ID最大长度
	maxNodeIDLength int = 16
	// 节点类型最大长度
	maxNodeTypeLength int = 16
)

func isASCIIAlphaNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlphaNumericUnderscoreHyphen(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlpha(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			continue
		}
		return false
	}
	return true
}

// checkNodeID 检查节点ID是否有效
// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
func checkNodeID(nodeId string) error {
	if len(nodeId) < 1 {
		kklog.Errorf("invalid node id: %s", nodeId)
		return kkerrors.ErrAppInvalidNodeID
	}
	if len(nodeId) > maxNodeIDLength {
		kklog.Errorf("node id is too long: %s", nodeId)
		return kkerrors.ErrAppInvalidNodeID
	}
	if !isASCIIAlphaNumericUnderscoreHyphen(nodeId) {
		kklog.Errorf("node id must contain only letters, digits, underscore, or hyphen: %s", nodeId)
		return kkerrors.ErrAppInvalidNodeID
	}
	return nil
}

// checkNodeType 检查节点类型是否有效
// 只允许英文字母，例如 "logic"、"chat"、"gate"。
func checkNodeType(nodeType string) error {
	if len(nodeType) < 1 {
		kklog.Errorf("invalid node type: %s", nodeType)
		return kkerrors.ErrAppInvalidNodeType
	}
	if len(nodeType) > maxNodeTypeLength {
		kklog.Errorf("node type is too long: %s", nodeType)
		return kkerrors.ErrAppInvalidNodeType
	}
	if !isASCIIAlpha(nodeType) {
		kklog.Errorf("node type must contain only letters: %s", nodeType)
		return kkerrors.ErrAppInvalidNodeType
	}
	return nil
}

// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
func IsValidActorNodeId(nodeId string) bool {
	if nodeId == "" {
		return true //允许空字符串，表示本地Actor
	}
	return isASCIIAlphaNumericUnderscoreHyphen(nodeId)
}

// 只允许【英文字母，数字，下划线("_")，中划线("-")】组合，例如 "game_player"、"gate_router-1001"
// 一般用_分隔层级。
func IsValidActorKey(actorKey string) bool {
	return isASCIIAlphaNumericUnderscoreHyphen(actorKey)
}
