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
	if err := head.Marshal(data, binary.BigEndian, 1, 2, 3); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	valueList, err := head.Unmarshal(data, binary.BigEndian)
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
	if err := head.Marshal(data, binary.BigEndian, 0x1234, 0x5678); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	v, err := head.ReadValueByName(data, binary.BigEndian, PartNameMsgID)
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

	// maxHeadPathCount 在 defaults.go 中定义为 4
	_ = NewPacketHead(&PartUint16{}, &PartUint16{}, &PartUint16{}, &PartUint16{}, &PartUint16{})
}

func TestPacketHead_NewPacketHeadWithNames_Ok(t *testing.T) {
	parts := []IHeadPart{&PartUint16{}, &PartUint32{}}
	names := []string{"msgID", "route"}
	head := NewPacketHeadWithNames(parts, names)

	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, binary.BigEndian, 10, 20); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	v0, err := head.ReadValueByName(data, binary.BigEndian, "msgID")
	if err != nil || v0 != 10 {
		t.Fatalf("ReadValueByName(msgID) = %d, err=%v, want 10, nil", v0, err)
	}
	v1, err := head.ReadValueByName(data, binary.BigEndian, "route")
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
	if err := head.SetNames([]string{"a"}); !errors.Is(err, kkerrors.ErrPacketNamesAndPartsLengthNotMatch) {
		t.Fatalf("SetNames len mismatch err = %v, want %v", err, kkerrors.ErrPacketNamesAndPartsLengthNotMatch)
	}

	// 名称重复
	if err := head.SetNames([]string{"a", "a"}); !errors.Is(err, kkerrors.ErrPacketHeadPartNameRepeated) {
		t.Fatalf("SetNames duplicate err = %v, want %v", err, kkerrors.ErrPacketHeadPartNameRepeated)
	}
}

func TestPacketHead_Marshal_Errors(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})

	// headBytes 太短
	short := make([]byte, head.GetSize()-1)
	if err := head.Marshal(short, binary.BigEndian, 1, 2); !errors.Is(err, kkerrors.ErrDataTooShortToMarshal) {
		t.Fatalf("Marshal short err = %v, want %v", err, kkerrors.ErrDataTooShortToMarshal)
	}

	// valueList 太短
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, binary.BigEndian, 1); !errors.Is(err, kkerrors.ErrValueListTooShortToMarshal) {
		t.Fatalf("Marshal valueList short err = %v, want %v", err, kkerrors.ErrValueListTooShortToMarshal)
	}
}

func TestPacketHead_Unmarshal_Errors(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})

	short := make([]byte, head.GetSize()-1)
	if _, err := head.Unmarshal(short, binary.BigEndian); !errors.Is(err, kkerrors.ErrDataTooShortToUnmarshal) {
		t.Fatalf("Unmarshal short err = %v, want %v", err, kkerrors.ErrDataTooShortToUnmarshal)
	}
}

func TestPacketHead_UnmarshalTo(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{})
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, binary.BigEndian, 7, 8); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// headBytes 太短，返回空切片
	short := data[:head.GetSize()-1]
	values, err := head.UnmarshalTo(short, binary.BigEndian, nil)
	if !errors.Is(err, kkerrors.ErrDataTooShortToUnmarshal) || len(values) != 0 {
		t.Fatalf("UnmarshalTo short = values:%v err:%v, want len 0, err %v", values, err, kkerrors.ErrDataTooShortToUnmarshal)
	}

	// valueList 初始长度不足时，会自动扩容
	values, err = head.UnmarshalTo(data, binary.BigEndian, make([]int, 1))
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
	return 0, kkerrors.ErrDataTooShortToUnmarshal
}

func (b *badPart) GetSize() int {
	return 1
}

func TestPacketHead_UnmarshalTo_PartErrorPartialResult(t *testing.T) {
	head := NewPacketHead(&PartUint8{}, &badPart{})
	data := make([]byte, head.GetSize())
	// 只需要保证第一个 part 可解
	if err := head.Marshal(data, binary.BigEndian, 5, 0); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	values, err := head.UnmarshalTo(data, binary.BigEndian, nil)
	if !errors.Is(err, kkerrors.ErrDataTooShortToUnmarshal) {
		t.Fatalf("UnmarshalTo err = %v, want %v", err, kkerrors.ErrDataTooShortToUnmarshal)
	}
	if len(values) != 1 || values[0] != 5 {
		t.Fatalf("UnmarshalTo partial values = %v, want [5]", values)
	}
}

func TestPacketHead_ReadValueByName_NameNotFound(t *testing.T) {
	head := NewPacketHead(&PartUint16{})
	data := make([]byte, head.GetSize())
	if err := head.Marshal(data, binary.BigEndian, 1); err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if _, err := head.ReadValueByName(data, binary.BigEndian, "not_exists"); !errors.Is(err, kkerrors.ErrPacketHeadPartNameNotFound) {
		t.Fatalf("ReadValueByName name not found err = %v, want %v", err, kkerrors.ErrPacketHeadPartNameNotFound)
	}
}
