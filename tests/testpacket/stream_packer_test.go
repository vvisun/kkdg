package testpacket

import (
	"encoding/binary"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

func TestNewLengthFieldPacker(t *testing.T) {
	maxSize := 1024
	packer := kkpacket.NewLengthFieldPacker(maxSize, nil)

	if packer == nil {
		t.Fatal("NewLengthFieldPacker returned nil")
	}

	if packer.GetMaxSize() != maxSize {
		t.Errorf("Expected MaxSize %d, got %d", maxSize, packer.GetMaxSize())
	}
}

func TestLengthFieldPacker_Pack_EmptyData(t *testing.T) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)
	data := []byte{}

	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}
	if buf == nil {
		t.Fatal("Pack returned nil buffer")
	}

	// Should have 4 bytes for length prefix
	if len(buf.B) != 4 {
		t.Errorf("Expected buffer length 4, got %d", len(buf.B))
	}

	// Length prefix should be 0
	length := kknet.GetByteOrder().Uint32(buf.B[:4])
	if length != 0 {
		t.Errorf("Expected length prefix 0, got %d", length)
	}
}

func TestLengthFieldPacker_Pack_NormalData(t *testing.T) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)
	data := []byte("hello world")

	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}
	if buf == nil {
		t.Fatal("Pack returned nil buffer")
	}

	expectedLen := 4 + len(data)
	if len(buf.B) != expectedLen {
		t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf.B))
	}

	// Verify length prefix
	length := kknet.GetByteOrder().Uint32(buf.B[:4])
	if length != uint32(len(data)) {
		t.Errorf("Expected length prefix %d, got %d", len(data), length)
	}

	// Verify data is correctly copied
	if string(buf.B[4:]) != string(data) {
		t.Errorf("Expected data %q, got %q", string(data), string(buf.B[4:]))
	}
}

func TestLengthFieldPacker_Pack_MaxSizeBoundary(t *testing.T) {
	maxSize := 100
	packer := kkpacket.NewLengthFieldPacker(maxSize, nil)

	// Test at MaxSize (should succeed)
	data := make([]byte, maxSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack at MaxSize returned error: %v", err)
	}
	if buf == nil {
		t.Fatal("Pack returned nil buffer")
	}

	length := kknet.GetByteOrder().Uint32(buf.B[:4])
	if length != uint32(maxSize) {
		t.Errorf("Expected length prefix %d, got %d", maxSize, length)
	}

	if len(buf.B[4:]) != maxSize {
		t.Errorf("Expected data length %d, got %d", maxSize, len(buf.B[4:]))
	}
}

func TestLengthFieldPacker_Pack_ExceedsMaxSize(t *testing.T) {
	maxSize := 100
	packer := kkpacket.NewLengthFieldPacker(maxSize, nil)

	// Test exceeding MaxSize (should fail)
	data := make([]byte, maxSize+1)

	buf, err := packer.Pack(data)
	if err == nil {
		t.Fatal("Pack should return error when data exceeds MaxSize")
	}
	if buf != nil {
		t.Error("Pack should return nil buffer when error occurs")
	}

	if err != kkerrors.ErrMaxMessageSize {
		t.Errorf("Expected ErrMaxMessageSize, got %v", err)
	}
}

func TestLengthFieldPacker_Pack_LargeData(t *testing.T) {
	packer := kkpacket.NewLengthFieldPacker(10240, nil)
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}
	if buf == nil {
		t.Fatal("Pack returned nil buffer")
	}

	length := kknet.GetByteOrder().Uint32(buf.B[:4])
	if length != uint32(len(data)) {
		t.Errorf("Expected length prefix %d, got %d", len(data), length)
	}

	// Verify data integrity
	for i := 0; i < len(data); i++ {
		if buf.B[4+i] != data[i] {
			t.Errorf("Data mismatch at index %d: expected %d, got %d", i, data[i], buf.B[4+i])
			break
		}
	}
}

func TestLengthFieldPacker_Pack_ByteOrder(t *testing.T) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)
	data := []byte("test")

	// Save original byte order
	originalOrder := kknet.GetByteOrder()
	defer kknet.SetByteOrder(originalOrder)

	// Test with BigEndian (default)
	kknet.SetByteOrder(binary.BigEndian)
	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}

	length := binary.BigEndian.Uint32(buf.B[:4])
	if length != uint32(len(data)) {
		t.Errorf("BigEndian: Expected length %d, got %d", len(data), length)
	}

	// Test with LittleEndian
	kknet.SetByteOrder(binary.LittleEndian)
	buf2, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}

	length2 := binary.LittleEndian.Uint32(buf2.B[:4])
	if length2 != uint32(len(data)) {
		t.Errorf("LittleEndian: Expected length %d, got %d", len(data), length2)
	}

	// The length prefixes should be different (byte order reversed)
	if buf.B[0] == buf2.B[0] && len(data) > 0 {
		// Only true if data length is small enough that bytes are the same
		// For len(data) = 4, BigEndian: [0,0,0,4], LittleEndian: [4,0,0,0]
		if len(data) == 4 {
			if buf.B[3] != buf2.B[0] {
				t.Error("Byte order should be reversed for length prefix")
			}
		}
	}
}

func TestLengthFieldPacker_Pack_MultiplePacks(t *testing.T) {
	packer := kkpacket.NewLengthFieldPacker(1024, nil)

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("a")},
		{"medium", []byte("hello world")},
		{"large", make([]byte, 500)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := packer.Pack(tc.data)
			if err != nil {
				t.Fatalf("Pack returned error: %v", err)
			}
			if buf == nil {
				t.Fatal("Pack returned nil buffer")
			}

			expectedLen := 4 + len(tc.data)
			if len(buf.B) != expectedLen {
				t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf.B))
			}

			length := kknet.GetByteOrder().Uint32(buf.B[:4])
			if length != uint32(len(tc.data)) {
				t.Errorf("Expected length prefix %d, got %d", len(tc.data), length)
			}

			if len(tc.data) > 0 {
				if string(buf.B[4:]) != string(tc.data) {
					t.Errorf("Expected data %q, got %q", string(tc.data), string(buf.B[4:]))
				}
			}
		})
	}
}
