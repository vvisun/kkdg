package kkpacket

import (
	"encoding/binary"
	"testing"

	"errors"

	"github.com/vvisun/kkdg/kkerrors"
)

func TestPacketHead_Marshal_Unmarshal(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{}, &PartUint64{})
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 1, 2, 3); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	valueList, err := head.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(valueList) != 3 || valueList[0] != 1 || valueList[1] != 2 || valueList[2] != 3 {
		t.Fatalf("Unmarshal values = %v, want [1 2 3]", valueList)
	}
}

func TestPacketHead_NewPacketHead_DefaultMsgIDName(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})
	if head.GetPartCount() != 2 {
		t.Fatalf("GetPartCount = %d, want 2", head.GetPartCount())
	}

	// msgID 默认映射到第一个 part
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 0x1234, 0x5678); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	v, err := head.ReadValueByName(data, PartNameMsgID)
	if err != nil {
		t.Fatalf("ReadValueByName(msgID): %v", err)
	}
	if v != 0x1234 {
		t.Fatalf("ReadValueByName(msgID) = %d, want %d", v, 0x1234)
	}
}

func TestPacketHead_NewPacketHead_TooManyParts_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("NewPacketHead should panic when parts > maxHeadPathCount")
		}
	}()

	heads := make([]IHeadPart, maxHeadPathCount+1)
	for i := 0; i < len(heads); i++ {
		heads[i] = &PartUint16{}
	}
	_ = NewPacketHead(heads...)
}

func TestPacketHead_NewPacketHeadWithNames_Ok(t *testing.T) {
	parts := []IHeadPart{&PartUint16{}, &PartUint32{}}
	names := []string{"msgID", "route"}
	head := NewPacketHeadWithNames(parts, names)

	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 10, 20); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	v0, err := head.ReadValueByName(data, "msgID")
	if err != nil || v0 != 10 {
		t.Fatalf("ReadValueByName(msgID) = %d, err=%v, want 10, nil", v0, err)
	}
	v1, err := head.ReadValueByName(data, "route")
	if err != nil || v1 != 20 {
		t.Fatalf("ReadValueByName(route) = %d, err=%v, want 20, nil", v1, err)
	}
}

func TestPacketHead_NewPacketHeadWithNames_LengthMismatch_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("NewPacketHeadWithNames should panic on length mismatch")
		}
	}()
	_ = NewPacketHeadWithNames([]IHeadPart{&PartUint16{}}, []string{"a", "b"})
}

func TestPacketHead_SetNames_Errors(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})

	// 长度不一致
	if err := head.SetNames([]string{"a"}); !errors.Is(err, kkerrors.ErrPktNamesAndPartsLengthNotMatch) {
		t.Fatalf("SetNames len mismatch err = %v, want %v", err, kkerrors.ErrPktNamesAndPartsLengthNotMatch)
	}

	// 名称重复
	if err := head.SetNames([]string{"a", "a"}); !errors.Is(err, kkerrors.ErrPktHeadPartNameRepeated) {
		t.Fatalf("SetNames duplicate err = %v, want %v", err, kkerrors.ErrPktHeadPartNameRepeated)
	}
}

func TestPacketHead_Marshal_Errors(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})

	// headBytes 太短
	short := make([]byte, head.GetSize()-1)
	if err := head.Marshal(short, 1, 2); !errors.Is(err, kkerrors.ErrPktDataTooShortToMarshal) {
		t.Fatalf("Marshal short err = %v, want %v", err, kkerrors.ErrPktDataTooShortToMarshal)
	}

	// valueList 太短
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 1); !errors.Is(err, kkerrors.ErrPktValueListTooShortToMarshal) {
		t.Fatalf("Marshal valueList short err = %v, want %v", err, kkerrors.ErrPktValueListTooShortToMarshal)
	}
}

func TestPacketHead_Unmarshal_Errors(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})

	short := make([]byte, head.GetSize()-1)
	if _, err := head.Unmarshal(short); !errors.Is(err, kkerrors.ErrPktDataTooShortToUnmarshal) {
		t.Fatalf("Unmarshal short err = %v, want %v", err, kkerrors.ErrPktDataTooShortToUnmarshal)
	}
}

func TestPacketHead_UnmarshalTo(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 7, 8); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// headBytes 太短，返回空切片
	short := data[:head.GetSize()-1]
	values, err := head.UnmarshalTo(short, nil)
	if !errors.Is(err, kkerrors.ErrPktDataTooShortToUnmarshal) || len(values) != 0 {
		t.Fatalf("UnmarshalTo short = values:%v err:%v, want len 0, err %v", values, err, kkerrors.ErrPktDataTooShortToUnmarshal)
	}

	// valueList 初始长度不足时，会自动扩容
	values, err = head.UnmarshalTo(data, make([]int, 1))
	if err != nil {
		t.Fatalf("UnmarshalTo: %v", err)
	}
	if len(values) != 2 || values[0] != 7 || values[1] != 8 {
		t.Fatalf("UnmarshalTo values = %v, want [7 8]", values)
	}
}

type badPart struct{}

func (b *badPart) Marshal(data []byte, endian binary.ByteOrder, value int) error {
	return nil
}

func (b *badPart) Unmarshal(data []byte, endian binary.ByteOrder) (int, error) {
	return 0, kkerrors.ErrPktDataTooShortToUnmarshal
}

func (b *badPart) GetSize() int {
	return 1
}

func TestPacketHead_UnmarshalTo_PartErrorPartialResult(t *testing.T) {
	head := NewPacketHead(&PartUint8{}, &badPart{})
	data := make([]byte, head.GetSize())
	// 只需要保证第一个 part 可解
	if err := head.Marshal(data, 5, 0); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	values, err := head.UnmarshalTo(data, nil)
	if !errors.Is(err, kkerrors.ErrPktDataTooShortToUnmarshal) {
		t.Fatalf("UnmarshalTo err = %v, want %v", err, kkerrors.ErrPktDataTooShortToUnmarshal)
	}
	if len(values) != 1 || values[0] != 5 {
		t.Fatalf("UnmarshalTo partial values = %v, want [5]", values)
	}
}

func TestPacketHead_ReadValueByName_NameNotFound(t *testing.T) {
	head := NewPacketHead(&PartUint16{})
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, 1); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if _, err := head.ReadValueByName(data, "not_exists"); !errors.Is(err, kkerrors.ErrPktHeadPartNameNotFound) {
		t.Fatalf("ReadValueByName name not found err = %v, want %v", err, kkerrors.ErrPktHeadPartNameNotFound)
	}
}
