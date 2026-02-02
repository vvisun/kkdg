package testpacket

import (
	"encoding/binary"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

func TestLengthFieldPacker_Split(t *testing.T) {
	packer := kkpacket.NewLengthFieldStreamPacket(nil)

	data := []byte("1234567890")
	pack, _ := packer.Pack(data)
	fullLen := len(pack.B)

	data = pack.B
	recvs := make([][]byte, 0, 8)
	packets, leftData, err := packer.Split(data, recvs)
	if err != nil {
		t.Fatalf("Split returned error: %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("Expected 1 packet, got %d", len(packets))
	}
	if string(packets[0]) != string(data) {
		t.Fatalf("Expected packet %q, got %q", string(data), string(packets[0]))
	}
	if len(leftData) != 0 {
		t.Fatalf("Expected no left data, got %d", len(leftData))
	}

	data = pack.B[:fullLen-1]
	packets, leftData, err = packer.Split(data, recvs)
	if err != nil {
		t.Fatalf("Split returned error: %v", err)
	}
	if len(packets) != 0 {
		t.Fatalf("Expected 0 packet, got %d", len(packets))
	}
	if len(leftData) != fullLen-1 {
		t.Fatalf("Expected left data length %d, got %d", fullLen-1, len(leftData))
	}
	if string(leftData) != string(pack.B[:fullLen-1]) {
		t.Fatalf("Expected left data %q, got %q", string(pack.B[:fullLen-1]), string(leftData))
	}

	data = pack.B[:]
	tmp := []byte("1234567890")
	for i := 0; i < 2; i++ {
		pk, _ := packer.Pack(tmp)
		data = append(data, pk.B...)
	}
	packets, leftData, err = packer.Split(data, recvs)
	if err != nil {
		t.Fatalf("Split returned error: %v", err)
	}
	if len(packets) != 3 {
		t.Fatalf("Expected 3 packets, got %d", len(packets))
	}
	if len(leftData) != 0 {
		t.Fatalf("Expected no left data, got %d", len(leftData))
	}
	if string(packets[0][packer.LengthFieldByteCount():]) != string(tmp) {
		t.Fatalf("Expected packet %q, got %q", string(tmp), string(packets[0]))
	}
	if string(packets[1][packer.LengthFieldByteCount():]) != string(tmp) {
		t.Fatalf("Expected packet %q, got %q", string(tmp), string(packets[1]))
	}
	if string(packets[2][packer.LengthFieldByteCount():]) != string(tmp) {
		t.Fatalf("Expected packet %q, got %q", string(tmp), string(packets[2]))
	}

	fullLen = len(data)
	data = data[:fullLen-3]
	wishLeftData := "1234567"
	packets, leftData, err = packer.Split(data, recvs)
	if err != nil {
		t.Fatalf("Split returned error: %v", err)
	}
	if len(packets) != 2 {
		t.Fatalf("Expected 2 packets, got %d", len(packets))
	}
	if len(leftData)-packer.LengthFieldByteCount() != len(wishLeftData) {
		t.Fatalf("Expected left data length %d, got %d", len(wishLeftData), len(leftData)-packer.LengthFieldByteCount())
	}
	if string(leftData[packer.LengthFieldByteCount():]) != wishLeftData {
		t.Fatalf("Expected left data %q, got %q", wishLeftData, string(leftData[packer.LengthFieldByteCount():]))
	}
	if string(packets[0][packer.LengthFieldByteCount():]) != string(tmp) {
		t.Fatalf("Expected packet %q, got %q", string(tmp), string(packets[0]))
	}
	if string(packets[1][packer.LengthFieldByteCount():]) != string(tmp) {
		t.Fatalf("Expected packet %q, got %q", string(tmp), string(packets[1]))
	}
}

func TestNewLengthFieldPacker(t *testing.T) {
	packer := kkpacket.NewLengthFieldStreamPacket(nil)

	if packer == nil {
		t.Fatal("NewLengthFieldPacker returned nil")
	}
}

func TestLengthFieldPacker_Pack_EmptyData(t *testing.T) {
	packer := kkpacket.NewLengthFieldStreamPacket(nil)
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
	length := kkpacket.GetByteOrder().Uint32(buf.B[:4])
	if length != 0 {
		t.Errorf("Expected length prefix 0, got %d", length)
	}
}

func TestLengthFieldPacker_Pack_NormalData(t *testing.T) {
	packer := kkpacket.NewLengthFieldStreamPacket(nil)
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
	length := kkpacket.GetByteOrder().Uint32(buf.B[:4])
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
	packer := kkpacket.NewLengthFieldStreamPacket(nil)

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

	length := kkpacket.GetByteOrder().Uint32(buf.B[:4])
	if length != uint32(maxSize) {
		t.Errorf("Expected length prefix %d, got %d", maxSize, length)
	}

	if len(buf.B[4:]) != maxSize {
		t.Errorf("Expected data length %d, got %d", maxSize, len(buf.B[4:]))
	}
}

func TestLengthFieldPacker_Pack_ExceedsMaxSize(t *testing.T) {
	maxSize := kkpacket.DefaultMaxMessageSize()
	packer := kkpacket.NewLengthFieldStreamPacket(nil)

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
	packer := kkpacket.NewLengthFieldStreamPacket(nil)
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

	length := kkpacket.GetByteOrder().Uint32(buf.B[:4])
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
	packer := kkpacket.NewLengthFieldStreamPacket(nil)
	data := []byte("test")

	// Save original byte order
	originalOrder := kkpacket.GetByteOrder()
	defer kkpacket.SetByteOrder(originalOrder)

	// Test with BigEndian (default)
	kkpacket.SetByteOrder(binary.BigEndian)
	buf, err := packer.Pack(data)
	if err != nil {
		t.Fatalf("Pack returned error: %v", err)
	}

	length := binary.BigEndian.Uint32(buf.B[:4])
	if length != uint32(len(data)) {
		t.Errorf("BigEndian: Expected length %d, got %d", len(data), length)
	}

	// Test with LittleEndian
	kkpacket.SetByteOrder(binary.LittleEndian)
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
	packer := kkpacket.NewLengthFieldStreamPacket(nil)

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

			length := kkpacket.GetByteOrder().Uint32(buf.B[:4])
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
