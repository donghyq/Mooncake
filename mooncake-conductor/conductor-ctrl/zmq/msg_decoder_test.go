package zmq_test

import (
	"testing"
	"time"

	"conductor/zmq"

	msgpack "github.com/shamaton/msgpack/v2"
)

func TestDecodeMooncakeEventBatch(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockStoreEvent",
		"mooncake-key-123",
		[][]interface{}{
			[]interface{}{"replica1", "replica2"},
			[]interface{}{"replica3"},
		},
		nil,                                     // index 3 is not used
		int64(1024),                             // BlockSize at index 4
		[]interface{}{uint64(100), uint64(200)}, // BlockHashes at index 5
		uint64(50),                              // ParentBlockHash at index 6
		[]interface{}{int32(1), int32(2), int32(3)}, // TokenIDs at index 7
	}
	events := []interface{}{event}
	batch := []interface{}{timestamp, events}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	result, err := zmq.DecodeMooncakeEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeMooncakeEventBatch failed: %v", err)
	}

	if result.Source != zmq.SourceMooncake {
		t.Errorf("Expected source %v, got %v", zmq.SourceMooncake, result.Source)
	}

	if len(result.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result.Events))
	}

	blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
	}

	if blockEvent.Type != zmq.EventTypeBlockStored {
		t.Errorf("Expected type %v, got %v", zmq.EventTypeBlockStored, blockEvent.Type)
	}

	if blockEvent.MooncakeKey != "mooncake-key-123" {
		t.Errorf("Expected MooncakeKey 'mooncake-key-123', got '%s'", blockEvent.MooncakeKey)
	}

	if blockEvent.BlockSize != 1024 {
		t.Errorf("Expected BlockSize 1024, got %d", blockEvent.BlockSize)
	}

	if len(blockEvent.BlockHashes) != 2 {
		t.Fatalf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
	}

	if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
		t.Errorf("Expected block hashes [100, 200], got %v", blockEvent.BlockHashes)
	}

	if blockEvent.ParentBlockHash != 50 {
		t.Errorf("Expected ParentBlockHash 50, got %d", blockEvent.ParentBlockHash)
	}

	if len(blockEvent.TokenIDs) != 3 {
		t.Fatalf("Expected 3 token IDs, got %d", len(blockEvent.TokenIDs))
	}

	if len(blockEvent.ReplicaList) != 2 {
		t.Fatalf("Expected 2 replica lists, got %d", len(blockEvent.ReplicaList))
	}
}

func TestDecodeVllmEventBatch(t *testing.T) {
	timestamp := int64(1700000000)
	event := []interface{}{
		"BlockStored",
		[]interface{}{uint64(100), uint64(200)},            // BlockHashes
		uint64(5000000000),                                 // ParentBlockHash
		[]interface{}{int32(10000000), int32(2), int32(3)}, // TokenIDs
		int64(1024),                                        // BlockSize
	}
	events := []interface{}{event}
	status := "ok"
	batch := []interface{}{timestamp, events, status}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	result, err := zmq.DecodeVllmEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeVllmEventBatch failed: %v", err)
	}

	if result.Source != zmq.SourceVLLM {
		t.Errorf("Expected source %v, got %v", zmq.SourceVLLM, result.Source)
	}

	if len(result.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result.Events))
	}

	blockEvent, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", result.Events[0])
	}

	if blockEvent.Type != zmq.EventTypeBlockStored {
		t.Errorf("Expected type %v, got %v", zmq.EventTypeBlockStored, blockEvent.Type)
	}

	expectedTime := time.Unix(1700000000, 0).UTC()
	if !blockEvent.Timestamp.Equal(expectedTime) {
		t.Errorf("Expected timestamp %v, got %v", expectedTime, blockEvent.Timestamp)
	}

	if len(blockEvent.BlockHashes) != 2 {
		t.Fatalf("Expected 2 block hashes, got %d", len(blockEvent.BlockHashes))
	}

	if blockEvent.BlockHashes[0] != 100 || blockEvent.BlockHashes[1] != 200 {
		t.Errorf("Expected block hashes [100, 200], got %v", blockEvent.BlockHashes)
	}

	if blockEvent.ParentBlockHash != 5000000000 {
		t.Errorf("Expected ParentBlockHash 50, got %d", blockEvent.ParentBlockHash)
	}

	if blockEvent.BlockSize != 1024 {
		t.Errorf("Expected BlockSize 1024, got %d", blockEvent.BlockSize)
	}

	if len(blockEvent.TokenIDs) != 3 {
		t.Fatalf("Expected 3 token IDs, got %d", len(blockEvent.TokenIDs))
	}
}

func TestDecodeMooncakeEventBatch_InvalidData(t *testing.T) {
	// Test with invalid array length
	invalidBatch := []interface{}{int64(1700000000)} // Missing events
	data, err := msgpack.Marshal(invalidBatch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	_, err = zmq.DecodeMooncakeEventBatch(data)
	if err == nil {
		t.Error("Expected error for invalid array length, got nil")
	}
}

func TestDecodeVllmEventBatch_InvalidData(t *testing.T) {
	// Test with invalid array length
	invalidBatch := []interface{}{int64(1700000000)} // Missing events and status
	data, err := msgpack.Marshal(invalidBatch)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	_, err = zmq.DecodeVllmEventBatch(data)
	if err == nil {
		t.Error("Expected error for invalid array length, got nil")
	}
}

func TestDecodeSGLangEventBatch(t *testing.T) {
	timestamp := 1700000000.25
	batch := []interface{}{
		timestamp,
		[]interface{}{
			// msgspec preserves nil placeholders for these required fields.
			[]interface{}{
				"BlockStored",
				[]interface{}{uint64(42)},
				nil,
				[]interface{}{int32(1), int32(2), int32(3)},
				int64(3),
				nil,
				"GPU",
			},
			// A non-null parent and lora id are both represented in order.
			[]interface{}{
				"BlockStored",
				[]interface{}{uint64(43)},
				uint64(42),
				[]interface{}{int32(4), int32(5), int32(6)},
				int64(3),
				int64(7),
				"CPU_PINNED",
			},
			[]interface{}{"BlockRemoved", []interface{}{uint64(42), uint64(43)}, "GPU"},
			[]interface{}{"AllBlocksCleared"},
		},
		int64(2),
	}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	if result.Source != zmq.SourceSGLang {
		t.Errorf("expected source %q, got %q", zmq.SourceSGLang, result.Source)
	}
	if result.DataParallelRank != 2 {
		t.Errorf("expected dp rank 2, got %d", result.DataParallelRank)
	}
	if len(result.Events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(result.Events))
	}

	first, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("expected first event BlockStoredEvent, got %T", result.Events[0])
	}
	if first.ParentBlockHash != 0 || first.LoraID != 0 || first.Medium != "GPU" {
		t.Errorf("unexpected omitted-field event: %+v", first)
	}
	if len(first.TokenIDs) != 3 || first.TokenIDs[2] != 3 {
		t.Errorf("unexpected token IDs: %v", first.TokenIDs)
	}

	second, ok := result.Events[1].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("expected second event BlockStoredEvent, got %T", result.Events[1])
	}
	if second.ParentBlockHash != 42 || second.LoraID != 7 || second.Medium != "CPU_PINNED" {
		t.Errorf("unexpected populated-field event: %+v", second)
	}

	removed, ok := result.Events[2].(*zmq.BlockRemovedEvent)
	if !ok {
		t.Fatalf("expected third event BlockRemovedEvent, got %T", result.Events[2])
	}
	if len(removed.BlockHashes) != 2 || removed.Medium != "GPU" {
		t.Errorf("unexpected removed event: %+v", removed)
	}

	if _, ok := result.Events[3].(*zmq.AllBlocksClearedEvent); !ok {
		t.Errorf("expected fourth event AllBlocksClearedEvent, got %T", result.Events[3])
	}
}

func TestDecodeSGLangEventBatch_BlockRemovedWithNilMedium(t *testing.T) {
	batch := []interface{}{
		int64(1700000000),
		[]interface{}{[]interface{}{"BlockRemoved", []interface{}{uint64(42)}, nil}},
		int64(0),
	}
	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	removed := result.Events[0].(*zmq.BlockRemovedEvent)
	if removed.Medium != "GPU" {
		t.Errorf("expected nil medium to use SGLang GPU default, got %q", removed.Medium)
	}
}

func TestDecodeSGLangEventBatch_BlockStoredWithoutLoraPlaceholder(t *testing.T) {
	// SGLang 0.5.4 uses msgspec omit_defaults and emits this six-field form
	// when lora_id is unset while medium is populated.
	batch := []interface{}{
		int64(1700000000),
		[]interface{}{[]interface{}{
			"BlockStored", []interface{}{uint64(42)}, nil,
			[]interface{}{int32(1), int32(2)}, int64(2), "GPU",
		}},
		int64(0),
	}
	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	stored, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("expected BlockStoredEvent, got %T", result.Events[0])
	}
	if stored.LoraID != 0 || stored.Medium != "GPU" {
		t.Errorf("unexpected six-field event: %+v", stored)
	}
}

func TestDecodeSGLangEventBatch_BlockStoredWithoutMedium(t *testing.T) {
	// msgspec omit_defaults can also omit medium while retaining a non-default
	// lora_id. The six-field form must not interpret that LoRA id as medium.
	batch := []interface{}{
		int64(1700000000),
		[]interface{}{[]interface{}{
			"BlockStored", []interface{}{uint64(42)}, nil,
			[]interface{}{int32(1), int32(2)}, int64(2), int64(7),
		}},
		int64(0),
	}
	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	stored, ok := result.Events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("expected BlockStoredEvent, got %T", result.Events[0])
	}
	if stored.LoraID != 7 || stored.Medium != "GPU" {
		t.Errorf("unexpected six-field event without medium: %+v", stored)
	}
}

func TestDecodeSGLangEventBatch_BlockStoredWithoutOptionalFields(t *testing.T) {
	batch := []interface{}{
		int64(1700000000),
		[]interface{}{[]interface{}{
			"BlockStored", []interface{}{uint64(42)}, nil,
			[]interface{}{int32(1), int32(2)}, int64(2),
		}},
		int64(0),
	}
	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	stored := result.Events[0].(*zmq.BlockStoredEvent)
	if stored.LoraID != 0 || stored.Medium != "GPU" {
		t.Errorf("unexpected event without optional fields: %+v", stored)
	}
}

func TestDecodeSGLangEventBatch_BlockStoredWithNilOptionalPlaceholders(t *testing.T) {
	// The current msgspec encoder retains nil placeholders in its array-like
	// representation even with omit_defaults enabled. SGLang's event producer
	// treats this omitted medium as the default GPU tier.
	batch := []interface{}{
		int64(1700000000),
		[]interface{}{[]interface{}{
			"BlockStored", []interface{}{uint64(42)}, nil,
			[]interface{}{int32(1), int32(2)}, int64(2), nil, nil,
		}},
		int64(0),
	}
	data, err := msgpack.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal SGLang batch: %v", err)
	}

	result, err := zmq.DecodeSGLangEventBatch(data)
	if err != nil {
		t.Fatalf("DecodeSGLangEventBatch failed: %v", err)
	}
	stored := result.Events[0].(*zmq.BlockStoredEvent)
	if stored.LoraID != 0 || stored.Medium != "GPU" {
		t.Errorf("unexpected event with nil optional placeholders: %+v", stored)
	}
}
