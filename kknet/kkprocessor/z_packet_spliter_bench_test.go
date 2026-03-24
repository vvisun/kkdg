package kkprocessor

import (
	"math/rand"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func benchPack(b *testing.B, stream kkpacket.IPacket, msg []byte) []byte {
	b.Helper()
	bb, err := stream.Pack(msg)
	if err != nil {
		b.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)
	out := make([]byte, len(bb.B))
	copy(out, bb.B)
	return out
}

func BenchmarkPacketSpliter_Split_SinglePacket(b *testing.B) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)
	packet := benchPack(b, stream, []byte("hello"))

	b.ReportAllocs()
	b.SetBytes(int64(len(packet)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packets, err := ps.Split(packet)
		if err != nil {
			b.Fatalf("Split: %v", err)
		}
		if len(packets) != 1 {
			b.Fatalf("packets=%d, want 1", len(packets))
		}
	}
}

func BenchmarkPacketSpliter_Split_MultiPacketBatch(b *testing.B) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	var batch []byte
	for i := 0; i < 32; i++ {
		batch = append(batch, benchPack(b, stream, []byte("abcdefghijklmnopqrstuvwxyz"))...)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(batch)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packets, err := ps.Split(batch)
		if err != nil {
			b.Fatalf("Split: %v", err)
		}
		if len(packets) != 32 {
			b.Fatalf("packets=%d, want 32", len(packets))
		}
	}
}

func BenchmarkPacketSpliter_Split_PartialStream(b *testing.B) {
	stream := kkpacket.DefaultStreamPacket()
	packet := benchPack(b, stream, []byte("partial-stream-payload"))

	half := len(packet) / 2
	if half <= 0 {
		b.Fatalf("invalid packet size=%d", len(packet))
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(packet)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps := NewPacketSpliter(stream, 2048)
		packets, err := ps.Split(packet[:half])
		if err != nil {
			b.Fatalf("Split(part1): %v", err)
		}
		if len(packets) != 0 {
			b.Fatalf("packets(part1)=%d, want 0", len(packets))
		}
		packets, err = ps.Split(packet[half:])
		if err != nil {
			b.Fatalf("Split(part2): %v", err)
		}
		if len(packets) != 1 {
			b.Fatalf("packets(part2)=%d, want 1", len(packets))
		}
	}
}

func makeMixedBatch(b *testing.B, stream kkpacket.IPacket, count int, seed int64) ([]byte, int) {
	b.Helper()
	if count <= 0 {
		return nil, 0
	}
	r := rand.New(rand.NewSource(seed))
	// Approximate online traffic: many tiny packets, fewer medium packets, occasional large packets.
	weights := []struct {
		size   int
		weight int
	}{
		{size: 16, weight: 45},
		{size: 64, weight: 30},
		{size: 128, weight: 15},
		{size: 256, weight: 7},
		{size: 512, weight: 3},
	}
	totalWeight := 0
	for _, w := range weights {
		totalWeight += w.weight
	}

	var out []byte
	for i := 0; i < count; i++ {
		x := r.Intn(totalWeight)
		size := weights[0].size
		sum := 0
		for _, w := range weights {
			sum += w.weight
			if x < sum {
				size = w.size
				break
			}
		}
		msg := make([]byte, size)
		for j := range msg {
			msg[j] = byte((i + j) & 0x7f)
		}
		out = append(out, benchPack(b, stream, msg)...)
	}
	return out, count
}

func makeFragmentSizes(total int, minChunk int, maxChunk int, seed int64) []int {
	if total <= 0 {
		return nil
	}
	if minChunk <= 0 {
		minChunk = 1
	}
	if maxChunk < minChunk {
		maxChunk = minChunk
	}
	r := rand.New(rand.NewSource(seed))
	remain := total
	var chunks []int
	for remain > 0 {
		n := minChunk + r.Intn(maxChunk-minChunk+1)
		if n > remain {
			n = remain
		}
		chunks = append(chunks, n)
		remain -= n
	}
	return chunks
}

func BenchmarkPacketSpliter_Split_MixedPacketSizeBatch(b *testing.B) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	batch, packetCount := makeMixedBatch(b, stream, 128, 20260324)
	b.ReportAllocs()
	b.SetBytes(int64(len(batch)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packets, err := ps.Split(batch)
		if err != nil {
			b.Fatalf("Split: %v", err)
		}
		if len(packets) != packetCount {
			b.Fatalf("packets=%d, want %d", len(packets), packetCount)
		}
	}
}

func BenchmarkPacketSpliter_Split_MixedPacketFragmentedStream(b *testing.B) {
	stream := kkpacket.DefaultStreamPacket()
	batch, packetCount := makeMixedBatch(b, stream, 128, 20260324)
	frags := makeFragmentSizes(len(batch), 1, 64, 20260325)

	b.ReportAllocs()
	b.SetBytes(int64(len(batch)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps := NewPacketSpliter(stream, 2048)
		offset := 0
		totalPackets := 0
		for _, n := range frags {
			packets, err := ps.Split(batch[offset : offset+n])
			if err != nil {
				b.Fatalf("Split(fragment): %v", err)
			}
			totalPackets += len(packets)
			offset += n
		}
		if totalPackets != packetCount {
			b.Fatalf("totalPackets=%d, want %d", totalPackets, packetCount)
		}
	}
}

