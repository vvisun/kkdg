package ptotrans

import (
	"encoding/binary"
	"fmt"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

var byteorder = binary.BigEndian

type fieldType uint8

const (
	dataTypeUint16 fieldType = iota
	dataTypeUint32
	dataTypeUint64
	dataTypeString
	dataTypeBytes
	dataTypeStringList
	dataTypeBytesList
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
func (si *structInfo) addField(dataType fieldType, name string) {
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
// @param data 数据
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

	// 先预留offset偏移量的空间，valueList的值将写入offset偏移量后的位置。
	if offset > 0 {
		bb.B = append(bb.B, make([]byte, offset)...)
	}

	tmpBuf := [64]byte{}  // 临时缓冲区，用于编码。
	tmpSlice := tmpBuf[:] // 临时缓冲区，用于编码。

	for _, field := range si.fields {
		value := valueList[field.index]
		switch field.dataType {
		case dataTypeUint16:
			v := value.(uint16)
			byteorder.PutUint16(tmpSlice, v)
			bb.B = append(bb.B, tmpSlice...)
			offset += 2
		case dataTypeUint32:
			v := value.(uint32)
			byteorder.PutUint32(tmpSlice, v)
			bb.B = append(bb.B, tmpSlice...)
			offset += 4
		case dataTypeUint64:
			v := value.(uint64)
			byteorder.PutUint64(tmpSlice, v)
			bb.B = append(bb.B, tmpSlice...)
			offset += 8
		case dataTypeString:
			str := value.(string)
			// 先写入长度
			byteorder.PutUint16(tmpSlice, uint16(len(str)))
			bb.B = append(bb.B, tmpSlice...)
			offset += 2
			// 然后写入string
			bb.B = append(bb.B, str...)
			offset += len(str)
		case dataTypeBytes:
			bytes := value.([]byte)
			// 先写入长度
			byteorder.PutUint16(tmpSlice, uint16(len(bytes)))
			bb.B = append(bb.B, tmpSlice...)
			offset += 2
			// 然后写入[]byte
			bb.B = append(bb.B, bytes...)
			offset += len(bytes)
		case dataTypeStringList:
			strList := value.([]string)
			// 先计算[]string的总长度
			byteorder.PutUint16(tmpSlice, uint16(len(strList)))
			bb.B = append(bb.B, tmpSlice...)
			offset += 2
			// 然后依次写入每个string
			for _, v := range strList {
				byteorder.PutUint16(tmpSlice, uint16(len(v)))
				bb.B = append(bb.B, tmpSlice...)
				offset += 2
				bb.B = append(bb.B, v...)
				offset += len(v)
			}
		case dataTypeBytesList:
			bytesList := value.([][]byte)
			// 先计算[][]byte的总长度
			byteorder.PutUint16(tmpSlice, uint16(len(bytesList)))
			bb.B = append(bb.B, tmpSlice...)
			offset += 2
			// 然后依次写入每个[]byte
			for _, v := range bytesList {
				byteorder.PutUint16(tmpSlice, uint16(len(v)))
				bb.B = append(bb.B, tmpSlice...)
				offset += 2
				bb.B = append(bb.B, v...)
				offset += len(v)
			}
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
	for _, field := range si.fields {
		switch field.dataType {
		case dataTypeUint16:
			valueList[field.index] = int16(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
		case dataTypeUint32:
			valueList[field.index] = int32(byteorder.Uint32(data[offset : offset+4]))
			offset += 4
		case dataTypeUint64:
			valueList[field.index] = int64(byteorder.Uint64(data[offset : offset+8]))
			offset += 8
		case dataTypeString:
			length := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			valueList[field.index] = string(data[offset : offset+length])
			offset += length
		case dataTypeBytes:
			length := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			valueList[field.index] = data[offset : offset+length]
			offset += length
		case dataTypeStringList:
			length := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			strList := make([]string, length)
			for i := 0; i < length; i++ {
				length := int(byteorder.Uint16(data[offset : offset+2]))
				offset += 2
				strList[i] = string(data[offset : offset+length])
				offset += length
			}
			valueList[field.index] = strList
		case dataTypeBytesList:
			length := int(byteorder.Uint16(data[offset : offset+2]))
			offset += 2
			bytesList := make([][]byte, length)
			for i := 0; i < length; i++ {
				length := int(byteorder.Uint16(data[offset : offset+2]))
				offset += 2
				bytesList[i] = data[offset : offset+length]
				offset += length
			}
			valueList[field.index] = bytesList
		default:
			return nil, fmt.Errorf("invalid data type: %d", field.dataType)
		}
	}
	return valueList, nil
}
