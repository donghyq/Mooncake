package kvevent

import (
	"context"
	"testing"
	"time"

	"conductor/common"
	"conductor/prefixindex"
	"conductor/zmq"
)

func TestHandleBlockStored_BlockSizeMismatch_ReturnsNil(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)

	handler := &KVEventHandler{
		manager:   m,
		blockSize: 64,
		modelName: "test-model",
	}

	event := &zmq.BlockStoredEvent{
		BlockSize:   128, // mismatch: 128 != 64
		BlockHashes: []uint64{100},
		TokenIDs:    []int32{1, 2, 3, 4},
	}

	// Should return nil (not error) when BlockSize mismatches
	err := handler.handleBlockStored(context.Background(), event, 0)
	if err != nil {
		t.Errorf("handleBlockStored with mismatched BlockSize should return nil, got %v", err)
	}
}

func TestHandleBlockStored_BlockSizeMatch_ProcessesEvent(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)

	handler := &KVEventHandler{
		manager:   m,
		blockSize: 4,
		modelName: "test-model",
	}

	event := &zmq.BlockStoredEvent{
		BlockSize:       4, // matches handler.blockSize
		BlockHashes:     []uint64{100, 200},
		TokenIDs:        []int32{1, 2, 3, 4, 5, 6, 7, 8}, // 2 blocks * 4 tokens each = 8
		ParentBlockHash: 0,
	}

	// Should process the event without error
	err := handler.handleBlockStored(context.Background(), event, 0)
	if err != nil {
		t.Errorf("handleBlockStored with matching BlockSize should not error, got %v", err)
	}
}

func TestHandleBlockStored_EmptyBlockHashesWithMismatch(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)

	handler := &KVEventHandler{
		manager:   m,
		blockSize: 64,
		modelName: "test-model",
	}

	event := &zmq.BlockStoredEvent{
		BlockSize:   128, // mismatch
		BlockHashes: []uint64{},
		TokenIDs:    []int32{},
	}

	err := handler.handleBlockStored(context.Background(), event, 0)
	if err != nil {
		t.Errorf("handleBlockStored with mismatched BlockSize and empty hashes should return nil, got %v", err)
	}
}

func TestHandleEvent_BlockStored_SizeMismatch_ReturnsNil(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)

	handler := &KVEventHandler{
		manager:   m,
		blockSize: 64,
		modelName: "test-model",
	}

	event := &zmq.BlockStoredEvent{
		BlockSize:   128, // mismatch
		BlockHashes: []uint64{100},
		TokenIDs:    []int32{1, 2, 3, 4},
		Timestamp:   time.Now(),
	}

	// HandleEvent should handle BlockStoredEvent dispatch and return nil on BlockSize mismatch
	err := handler.HandleEvent(event, 0)
	if err != nil {
		t.Errorf("HandleEvent with mismatched BlockSize should return nil, got %v", err)
	}
}

func TestHandleAllBlocksCleared_InvalidatesInstanceIndex(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)
	handler := &KVEventHandler{
		manager:    m,
		blockSize:  4,
		modelName:  "test-model",
		loraName:   "",
		instanceID: "test-instance",
	}

	stored := &zmq.BlockStoredEvent{
		BlockSize:   4,
		BlockHashes: []uint64{100},
		TokenIDs:    []int32{1, 2, 3, 4},
		Medium:      "GPU",
	}
	if err := handler.HandleEvent(stored, 0); err != nil {
		t.Fatalf("store event: %v", err)
	}
	if got := len(m.getIndexer().GetGlobalView().ModelContexts); got != 1 {
		t.Fatalf("expected populated index, got %d contexts", got)
	}

	if err := handler.HandleEvent(&zmq.AllBlocksClearedEvent{}, 0); err != nil {
		t.Fatalf("all-cleared event: %v", err)
	}
	if got := len(m.getIndexer().GetGlobalView().ModelContexts); got != 0 {
		t.Errorf("expected cleared index, got %d contexts", got)
	}
}

func TestHandleAllBlocksCleared_UsesServiceTenantAndSalt(t *testing.T) {
	m := NewEventManager([]common.ServiceConfig{}, 0)
	handler := &KVEventHandler{
		manager:        m,
		blockSize:      4,
		modelName:      "test-model",
		loraName:       "",
		tenant_id:      "tenant-a",
		additionalSalt: "isolated-cache",
		instanceID:     "test-instance",
	}

	stored := &zmq.BlockStoredEvent{
		BlockSize:   4,
		BlockHashes: []uint64{100},
		TokenIDs:    []int32{1, 2, 3, 4},
		Medium:      "GPU",
	}
	if err := handler.HandleEvent(stored, 0); err != nil {
		t.Fatalf("store event: %v", err)
	}

	context := &prefixindex.ModelContext{
		TenantID:       "tenant-a",
		ModelName:      "test-model",
		LoraName:       "",
		BlockSize:      4,
		AdditionalSalt: "isolated-cache",
		InstanceID:     "test-instance",
	}
	if result := m.getIndexer().CacheHitCompute(context, []int32{1, 2, 3, 4}); result.LongestMatchTokens != 4 {
		t.Fatalf("cache hit with service tenant and salt = %d, want 4", result.LongestMatchTokens)
	}

	if err := handler.HandleEvent(&zmq.AllBlocksClearedEvent{}, 0); err != nil {
		t.Fatalf("all-cleared event: %v", err)
	}
	if result := m.getIndexer().CacheHitCompute(context, []int32{1, 2, 3, 4}); result.LongestMatchTokens != 0 {
		t.Errorf("cache hit after clear = %d, want 0", result.LongestMatchTokens)
	}
}
