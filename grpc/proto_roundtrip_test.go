package grpcapi_test

import (
	"testing"

	grpcapi "m31labs.dev/corkscrewdb/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestProtoRoundTripCodeCarrying verifies the code-carrying wire messages
// (QuantizedVector / SparseBlock / ChildVector plus ReplicaEntry /
// SnapshotVersion / Version) round-trip through proto.Marshal/Unmarshal. This
// test lives in package grpcapi_test and imports ONLY the generated grpcapi
// package + protobuf, so it stays green while the root corkscrewdb package is
// intentionally red during the Tasks 1-5 cluster.
func TestProtoRoundTripCodeCarrying(t *testing.T) {
	rawHash := make([]byte, 32)
	for i := range rawHash {
		rawHash[i] = byte(i)
	}
	ts := timestamppb.New(timestamppb.Now().AsTime())

	quant := &grpcapi.QuantizedVector{
		Mse:     []byte{1, 2, 3, 4},
		Signs:   []byte{0xAA, 0x55},
		ResNorm: 1.5,
	}
	sparse := &grpcapi.SparseBlock{
		Indices: []uint32{1, 3, 7},
		Values:  []float32{0.1, 0.2, 0.3},
	}
	children := []*grpcapi.ChildVector{
		{
			Id:        "child-0",
			Quantized: &grpcapi.QuantizedVector{Mse: []byte{9}, Signs: []byte{1}, ResNorm: 0.25},
			Dim:       8,
			Text:      "c0",
			Metadata:  map[string]string{"k": "v0"},
		},
		{
			Id:        "child-1",
			Quantized: &grpcapi.QuantizedVector{Mse: []byte{8, 8}, Signs: []byte{2}, ResNorm: 0.75},
			Dim:       8,
			Text:      "c1",
			Metadata:  map[string]string{"k": "v1"},
		},
	}

	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "ReplicaEntry",
			msg: &grpcapi.ReplicaEntry{
				Kind:         1,
				CollectionId: "docs",
				VectorId:     "vec-1",
				Quantized:    proto.Clone(quant).(*grpcapi.QuantizedVector),
				Dim:          8,
				RawHash:      append([]byte(nil), rawHash...),
				Sparse:       proto.Clone(sparse).(*grpcapi.SparseBlock),
				Children:     []*grpcapi.ChildVector{proto.Clone(children[0]).(*grpcapi.ChildVector), proto.Clone(children[1]).(*grpcapi.ChildVector)},
				Text:         "hello",
				Metadata:     map[string]string{"a": "b"},
				LamportClock: 42,
				ActorId:      "actor-1",
				WallClock:    ts,
			},
		},
		{
			name: "SnapshotVersion",
			msg: &grpcapi.SnapshotVersion{
				Quantized:    proto.Clone(quant).(*grpcapi.QuantizedVector),
				RawHash:      append([]byte(nil), rawHash...),
				Sparse:       proto.Clone(sparse).(*grpcapi.SparseBlock),
				Children:     []*grpcapi.ChildVector{proto.Clone(children[0]).(*grpcapi.ChildVector), proto.Clone(children[1]).(*grpcapi.ChildVector)},
				Text:         "snap",
				Metadata:     map[string]string{"x": "y"},
				LamportClock: 99,
				ActorId:      "actor-2",
				WallClock:    ts,
				Tombstone:    false,
			},
		},
		{
			name: "Version",
			msg: &grpcapi.Version{
				Quantized:    proto.Clone(quant).(*grpcapi.QuantizedVector),
				RawHash:      append([]byte(nil), rawHash...),
				Sparse:       proto.Clone(sparse).(*grpcapi.SparseBlock),
				Children:     []*grpcapi.ChildVector{proto.Clone(children[0]).(*grpcapi.ChildVector), proto.Clone(children[1]).(*grpcapi.ChildVector)},
				Text:         "ver",
				Metadata:     map[string]string{"m": "n"},
				LamportClock: 7,
				ActorId:      "actor-3",
				WallClock:    ts,
				Tombstone:    true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.msg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := tc.msg.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(data, got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !proto.Equal(tc.msg, got) {
				t.Fatalf("round-trip mismatch:\n want %v\n got  %v", tc.msg, got)
			}
		})
	}
}
