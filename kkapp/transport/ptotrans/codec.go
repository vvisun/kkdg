package ptotrans

import (
	"encoding/binary"
	"fmt"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

var byteorder = binary.BigEndian

func errTruncated(off, needN, dataLen int) error {
	return fmt.Errorf("ptotrans: truncated data (need %d bytes at offset %d, len=%d)", needN, off, dataLen)
}

type fieldType uint8

const (
	dataTypeUint16 fieldType = iota
	dataTypeUint32
	dataTypeUint64
	dataTypeString
	dataTypeBytes
	dataTypeStringList
	dataTypeBytesList
	dataTypeBool  // 1 byte: 1 = true, 0 = false
	dataTypeUint8 // 1 byte: 0-255
)

type fieldInfo struct {
	dataType fieldType
	index    int
	name     string
}

// 自定义的高性能编解码器，简单数据的编解码，不支持嵌套结构体，只支持一维数组。
// 仅用于内网传输，不用于外网传输。
type structInfo struct {
	fields []fieldInfo
}

// 添加字段
func (si *structInfo) AddField(dataType fieldType, name string) {
	index := len(si.fields)
	si.fields = append(si.fields, fieldInfo{dataType: dataType, index: index, name: name})
}

// 编码
// @param valueList 值列表
// @param offset 偏移量（offset前的数据一般用于存放消息头）
// @return *kkbuffer.ByteBuffer 编码后的数据
// @return error 错误
func (si *structInfo) Marshal(valueList []any, offset int) (*kkbuffer.ByteBuffer, error) {
	//panic时返回error，避免panic导致程序退出。
	var panicErr error
	defer func() {
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("panic: %v", r)
		}
	}()
	bb, err := si.marshal(valueList, offset)
	if panicErr != nil {
		kkbuffer.Put(bb)
		return nil, panicErr
	}
	if err != nil {
		kkbuffer.Put(bb)
		return nil, err
	}
	return bb, err
}

// 解码
//
// @param data 输入缓冲区。[]byte / [][]byte 字段为 data 的子切片视图；在仍使用返回的 []any 期间勿修改 data 对应区间。
// string 字段为拷贝，不依赖 data 生命周期。
//
// @return []any 解码后的数据
// @return error 错误
func (si *structInfo) Unmarshal(data []byte) ([]any, error) {
	//panic时返回error，避免panic导致程序退出。
	var panicErr error
	defer func() {
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("panic: %v", r)
		}
	}()
	valueList, err := si.unmarshal(data)
	if panicErr != nil {
		return nil, panicErr
	}
	if err != nil {
		return nil, err
	}
	return valueList, err
}

// 编码
// @param valueList 值列表
// @param offset 偏移量（offset前的数据一般用于存放消息头）
// @return *kkbuffer.ByteBuffer 编码后的数据
// @return error 错误
func (si *structInfo) marshal(valueList []any, offset int) (*kkbuffer.ByteBuffer, error) {
	if offset < 0 {
		return nil, fmt.Errorf("offset is less than 0")
	}
	if len(valueList) != len(si.fields) {
		return nil, fmt.Errorf("valueList length mismatch: %d != %d", len(valueList), len(si.fields))
	}

	bb := kkbuffer.GetWithLenCap(0, 1024)

	// 先预留 offset 字节（通常给消息头）；后续字段直接 append 到 bb.B。
	if offset > 0 {
		bb.B = append(bb.B, make([]byte, offset)...)
	}

	for _, field := range si.fields {
		value := valueList[field.index]
		switch field.dataType {
		case dataTypeUint16:
			v, ok := value.(uint16)
			if !ok {
				return nil, fmt.Errorf("field %q: want uint16, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint16(bb.B, v)
		case dataTypeUint32:
			v, ok := value.(uint32)
			if !ok {
				return nil, fmt.Errorf("field %q: want uint32, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint32(bb.B, v)
		case dataTypeUint64:
			v, ok := value.(uint64)
			if !ok {
				return nil, fmt.Errorf("field %q: want uint64, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint64(bb.B, v)
		case dataTypeString:
			str, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("field %q: want string, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint16(bb.B, uint16(len(str)))
			bb.B = append(bb.B, str...)
		case dataTypeBytes:
			bytes, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("field %q: want []byte, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint16(bb.B, uint16(len(bytes)))
			bb.B = append(bb.B, bytes...)
		case dataTypeStringList:
			strList, ok := value.([]string)
			if !ok {
				return nil, fmt.Errorf("field %q: want []string, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint16(bb.B, uint16(len(strList)))
			for _, v := range strList {
				bb.B = byteorder.AppendUint16(bb.B, uint16(len(v)))
				bb.B = append(bb.B, v...)
			}
		case dataTypeBytesList:
			bytesList, ok := value.([][]byte)
			if !ok {
				return nil, fmt.Errorf("field %q: want [][]byte, got %T", field.name, value)
			}
			bb.B = byteorder.AppendUint16(bb.B, uint16(len(bytesList)))
			for _, v := range bytesList {
				bb.B = byteorder.AppendUint16(bb.B, uint16(len(v)))
				bb.B = append(bb.B, v...)
			}
		case dataTypeBool:
			v, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("field %q: want bool, got %T", field.name, value)
			}
			if v {
				bb.B = append(bb.B, 1)
			} else {
				bb.B = append(bb.B, 0)
			}
		case dataTypeUint8:
			v, ok := value.(uint8)
			if !ok {
				return nil, fmt.Errorf("field %q: want uint8, got %T", field.name, value)
			}
			bb.B = append(bb.B, v)
		default:
			return nil, fmt.Errorf("invalid data type: %d", field.dataType)
		}
	}
	return bb, nil
}

// 解码
func (si *structInfo) unmarshal(data []byte) ([]any, error) {
	valueList := make([]any, len(si.fields))
	offset := 0
	n := len(data)
	for _, field := range si.fields {
		switch field.dataType {
		case dataTypeUint16:
			if offset+2 > n {
				return nil, errTruncated(offset, 2, n)
			}
			valueList[field.index] = int16(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
		case dataTypeUint32:
			if offset+4 > n {
				return nil, errTruncated(offset, 4, n)
			}
			valueList[field.index] = int32(byteorder.Uint32(data[offset : offset+4]))
			offset += 4
		case dataTypeUint64:
			if offset+8 > n {
				return nil, errTruncated(offset, 8, n)
			}
			valueList[field.index] = int64(byteorder.Uint64(data[offset : offset+8]))
			offset += 8
		case dataTypeString:
			if offset+2 > n {
				return nil, errTruncated(offset, 2, n)
			}
			slen := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+slen > n {
				return nil, errTruncated(offset, slen, n)
			}
			valueList[field.index] = string(data[offset : offset+slen])
			offset += slen
		case dataTypeBytes:
			if offset+2 > n {
				return nil, errTruncated(offset, 2, n)
			}
			blen := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			if offset+blen > n {
				return nil, errTruncated(offset, blen, n)
			}
			valueList[field.index] = data[offset : offset+blen]
			offset += blen
		case dataTypeStringList:
			if offset+2 > n {
				return nil, errTruncated(offset, 2, n)
			}
			cnt := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			strList := make([]string, cnt)
			for i := 0; i < cnt; i++ {
				if offset+2 > n {
					return nil, errTruncated(offset, 2, n)
				}
				elen := int(byteorder.Uint16(data[offset : offset+2]))
				offset += 2
				if offset+elen > n {
					return nil, errTruncated(offset, elen, n)
				}
				strList[i] = string(data[offset : offset+elen])
				offset += elen
			}
			valueList[field.index] = strList
		case dataTypeBytesList:
			if offset+2 > n {
				return nil, errTruncated(offset, 2, n)
			}
			cnt := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			bytesList := make([][]byte, cnt)
			for i := 0; i < cnt; i++ {
				if offset+2 > n {
					return nil, errTruncated(offset, 2, n)
				}
				elen := int(byteorder.Uint16(data[offset : offset+2]))
				offset += 2
				if offset+elen > n {
					return nil, errTruncated(offset, elen, n)
				}
				bytesList[i] = data[offset : offset+elen]
				offset += elen
			}
			valueList[field.index] = bytesList
		case dataTypeBool:
			if offset+1 > n {
				return nil, errTruncated(offset, 1, n)
			}
			valueList[field.index] = data[offset] != 0
			offset += 1
		case dataTypeUint8:
			if offset+1 > n {
				return nil, errTruncated(offset, 1, n)
			}
			valueList[field.index] = uint8(data[offset])
			offset += 1
		default:
			return nil, fmt.Errorf("invalid data type: %d", field.dataType)
		}
	}
	return valueList, nil
}
