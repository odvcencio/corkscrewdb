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
		Norm:    proto.Float32(3.25),
	}
	sparse := &grpcapi.SparseBlock{
		Indices: []uint32{1, 3, 7},
		Values:  []float32{0.1, 0.2, 0.3},
	}
	children := []*grpcapi.ChildVector{
		{
			Id:        "child-0",
			Quantized: &grpcapi.QuantizedVector{Mse: []byte{9}, Signs: []byte{1}, ResNorm: 0.25, Norm: proto.Float32(2.0)},
			Dim:       8,
			Text:      "c0",
			Metadata:  map[string]string{"k": "v0"},
		},
		{
			Id:        "child-1",
			Quantized: &grpcapi.QuantizedVector{Mse: []byte{8, 8}, Signs: []byte{2}, ResNorm: 0.75, Norm: proto.Float32(0.5)},
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

// TestProtoRoundTripRebalanceAndRaw covers the GetRaw RPC messages and the
// rebalance epoch / Freeze / Abort / Resolve RPC messages added in Task 2. Like
// the test above it imports only the generated package + protobuf and stays
// green while the root package is red.
func TestProtoRoundTripRebalanceAndRaw(t *testing.T) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(255 - i)
	}
	shards := []*grpcapi.ShardAssignment{
		{Id: "s0", Owner: "node-a", Start: 0, End: 100},
		{Id: "s1", Owner: "node-b", Start: 100, End: 200},
	}

	cases := []struct {
		name string
		msg  proto.Message
	}{
		{
			name: "GetRawRequest",
			msg:  &grpcapi.GetRawRequest{Token: "tok", Collection: "docs", Hash: append([]byte(nil), hash...)},
		},
		{
			name: "GetRawResponse",
			msg:  &grpcapi.GetRawResponse{Raw: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		},
		{
			name: "RebalanceRequestEpoch",
			msg: &grpcapi.RebalanceRequest{
				Token:  "tok",
				Shards: []*grpcapi.ShardAssignment{shards[0], shards[1]},
				Epoch:  7,
			},
		},
		{
			name: "FreezeRebalanceRequest",
			msg: &grpcapi.FreezeRebalanceRequest{
				Token:       "tok",
				Shards:      []*grpcapi.ShardAssignment{shards[0], shards[1]},
				Epoch:       7,
				Coordinator: "127.0.0.1:9001",
			},
		},
		{
			name: "AbortRebalanceRequest",
			msg:  &grpcapi.AbortRebalanceRequest{Token: "tok", Epoch: 7},
		},
		{
			name: "ResolveRebalanceRequest",
			msg:  &grpcapi.ResolveRebalanceRequest{Token: "tok", Epoch: 7},
		},
		{
			name: "ResolveRebalanceResponseCommit",
			msg:  &grpcapi.ResolveRebalanceResponse{Decision: grpcapi.Decision_DECISION_COMMIT, CommittedEpoch: 7},
		},
		{
			name: "ResolveRebalanceResponseAbort",
			msg:  &grpcapi.ResolveRebalanceResponse{Decision: grpcapi.Decision_DECISION_ABORT, CommittedEpoch: 6},
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

// TestProtoQuantizedVectorNormPresence verifies the wire-level presence
// semantics of QuantizedVector.norm (B1): it is `optional float`, backed by
// a synthetic oneof, so proto.Marshal/Unmarshal must distinguish "field
// never set" (old peer, pre-B1) from "field set to the zero value" (a
// genuine zero vector) — a plain (non-optional) proto3 float cannot make
// this distinction, since unmarshal always yields the Go zero value for an
// absent field. Presence is what lets corkscrewdb's transport layer
// (fromProtoQuantized) default an ABSENT norm to 1 while trusting a PRESENT
// 0 as a real zero-vector norm.
func TestProtoQuantizedVectorNormPresence(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		msg := &grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5}
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got := &grpcapi.QuantizedVector{}
		if err := proto.Unmarshal(data, got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.Norm != nil {
			t.Fatalf("Norm = %v, want nil (absent, not sent on the wire)", *got.Norm)
		}
	})

	t.Run("present zero", func(t *testing.T) {
		msg := &grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: proto.Float32(0)}
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got := &grpcapi.QuantizedVector{}
		if err := proto.Unmarshal(data, got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.Norm == nil {
			t.Fatal("Norm = nil, want present (a genuine zero vector must transmit presence=true, value 0)")
		}
		if *got.Norm != 0 {
			t.Fatalf("Norm = %v, want 0", *got.Norm)
		}
	})
}
