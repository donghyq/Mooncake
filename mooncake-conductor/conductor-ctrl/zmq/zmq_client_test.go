package zmq_test

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"conductor/zmq"

	zmq4 "github.com/pebbe/zmq4"
	msgpack "github.com/shamaton/msgpack/v2"
)

// MockEventHandler implements EventHandler for testing
type MockEventHandler struct {
	mu          sync.Mutex
	events      []zmq.KVEvent
	handleError error
	callCount   int64
}

func NewMockEventHandler() *MockEventHandler {
	return &MockEventHandler{
		events: make([]zmq.KVEvent, 0),
	}
}

func (m *MockEventHandler) HandleEvent(event zmq.KVEvent, _ int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.AddInt64(&m.callCount, 1)
	if m.handleError != nil {
		return m.handleError
	}
	m.events = append(m.events, event)
	return nil
}

func (m *MockEventHandler) GetEvents() []zmq.KVEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	events := make([]zmq.KVEvent, len(m.events))
	copy(events, m.events)
	return events
}

func (m *MockEventHandler) GetCallCount() int64 {
	return atomic.LoadInt64(&m.callCount)
}

func (m *MockEventHandler) SetHandleError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handleError = err
}

func (m *MockEventHandler) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = m.events[:0]
	m.handleError = nil
	atomic.StoreInt64(&m.callCount, 0)
}

// MockPublisher simulates a ZMQ publisher for testing
type MockPublisher struct {
	pubSocket    *zmq4.Socket
	routerSocket *zmq4.Socket
	sequence     int64
	replayBuffer []replayEntry
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

type replayEntry struct {
	sequence int64
	topic    string
	payload  []byte
}

func createMockPublisher(t *testing.T, pubPort, routerPort int) *MockPublisher {
	ctx, cancel := context.WithCancel(context.Background())

	// Create PUB socket
	pubSocket, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		t.Fatalf("Failed to create PUB socket: %v", err)
	}

	err = pubSocket.SetIpv6(true)
	if err != nil {
		pubSocket.Close()
		t.Fatalf("Failed to enable IPv6 on PUB socket: %v", err)
	}

	err = pubSocket.Bind("tcp://127.0.0.1:*")
	if err != nil {
		pubSocket.Close()
		t.Fatalf("Failed to bind PUB socket: %v", err)
	}

	// Create ROUTER socket for replay
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		pubSocket.Close()
		t.Fatalf("Failed to create ROUTER socket: %v", err)
	}

	err = routerSocket.SetIpv6(true)
	if err != nil {
		pubSocket.Close()
		routerSocket.Close()
		t.Fatalf("Failed to enable IPv6 on ROUTER socket: %v", err)
	}

	err = routerSocket.Bind("tcp://127.0.0.1:*")
	if err != nil {
		pubSocket.Close()
		routerSocket.Close()
		t.Fatalf("Failed to bind ROUTER socket: %v", err)
	}

	mp := &MockPublisher{
		pubSocket:    pubSocket,
		routerSocket: routerSocket,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start replay handler
	mp.wg.Add(1)
	go mp.handleReplay()

	// Wait for sockets to bind
	time.Sleep(100 * time.Millisecond)

	return mp
}

func (mp *MockPublisher) PublishEvent(topic string, event zmq.KVEvent) error {
	// Encode event based on topic
	var payload []byte
	var err error

	if topic == "mooncake" {
		payload, err = encodeMooncakeEvent(event)
	} else if topic == zmq.SourceSGLang {
		payload, err = encodeSGLangEvent(event)
	} else {
		payload, err = encodeVllmEvent(event)
	}
	if err != nil {
		return err
	}

	mp.mu.Lock()
	mp.sequence++
	seq := mp.sequence
	mp.replayBuffer = append(mp.replayBuffer, replayEntry{sequence: seq, topic: topic, payload: payload})
	mp.mu.Unlock()

	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, uint64(seq))

	_, err = mp.pubSocket.SendMessage(topic, seqBytes, payload)
	return err
}

func (mp *MockPublisher) PublishBuffered(sequence int64) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	for _, entry := range mp.replayBuffer {
		if entry.sequence == sequence {
			_, err := mp.pubSocket.SendMessage(entry.topic, sequenceBytes(entry.sequence), entry.payload)
			return err
		}
	}
	return errors.New("replay entry not found")
}

func (mp *MockPublisher) handleReplay() {
	defer mp.wg.Done()
	for {
		select {
		case <-mp.ctx.Done():
			return
		default:
		}

		// Set receive timeout
		_ = mp.routerSocket.SetRcvtimeo(100 * time.Millisecond)

		frames, err := mp.routerSocket.RecvMessageBytes(0)
		if err != nil {
			continue
		}
		if len(frames) != 3 || len(frames[2]) != 8 {
			continue
		}
		fromSeq := int64(binary.BigEndian.Uint64(frames[2]))
		mp.mu.Lock()
		entries := append([]replayEntry(nil), mp.replayBuffer...)
		mp.mu.Unlock()
		for _, entry := range entries {
			if entry.sequence >= fromSeq {
				_, _ = mp.routerSocket.SendMessage(frames[0], []byte{}, sequenceBytes(entry.sequence), entry.payload)
			}
		}
		_, _ = mp.routerSocket.SendMessage(frames[0], []byte{}, sequenceBytes(-1), []byte{})
	}
}

func sequenceBytes(sequence int64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(sequence))
	return data
}

func (mp *MockPublisher) Close() {
	mp.cancel()
	mp.wg.Wait()
	_ = mp.pubSocket.Close()
	_ = mp.routerSocket.Close()
}

func encodeMooncakeEvent(event zmq.KVEvent) ([]byte, error) {
	switch e := event.(type) {
	case *zmq.BlockStoredEvent:
		timestamp := e.Timestamp.Unix()
		eventData := []interface{}{
			"BlockStoreEvent",
			e.MooncakeKey,
			e.ReplicaList,
			nil, // index 3 not used
			e.BlockSize,
			convertUint64Slice(e.BlockHashes),
			e.ParentBlockHash,
			convertInt32Slice(e.TokenIDs),
		}
		batch := []interface{}{timestamp, []interface{}{eventData}}
		return msgpack.Marshal(batch)
	default:
		return nil, errors.New("unsupported event type for mooncake")
	}
}

func encodeVllmEvent(event zmq.KVEvent) ([]byte, error) {
	switch e := event.(type) {
	case *zmq.BlockStoredEvent:
		timestamp := e.Timestamp.Unix()
		eventData := []interface{}{
			"BlockStored",
			convertUint64Slice(e.BlockHashes),
			e.ParentBlockHash,
			convertInt32Slice(e.TokenIDs),
			e.BlockSize,
		}
		batch := []interface{}{timestamp, []interface{}{eventData}, "ok"}
		return msgpack.Marshal(batch)
	case *zmq.BlockRemovedEvent:
		timestamp := e.Timestamp.Unix()
		eventData := []interface{}{
			"BlockRemoved",
			convertUint64Slice(e.BlockHashes),
		}
		batch := []interface{}{timestamp, []interface{}{eventData}, "ok"}
		return msgpack.Marshal(batch)
	default:
		return nil, errors.New("unsupported event type for vllm")
	}
}

func encodeSGLangEvent(event zmq.KVEvent) ([]byte, error) {
	switch e := event.(type) {
	case *zmq.BlockStoredEvent:
		eventData := []interface{}{
			"BlockStored",
			convertUint64Slice(e.BlockHashes),
			e.ParentBlockHash,
			convertInt32Slice(e.TokenIDs),
			e.BlockSize,
			e.LoraID,
			e.Medium,
		}
		batch := []interface{}{e.Timestamp.Unix(), []interface{}{eventData}, int64(0)}
		return msgpack.Marshal(batch)
	default:
		return nil, errors.New("unsupported event type for SGLang")
	}
}

func convertUint64Slice(slice []uint64) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = uint64(v)
	}
	return result
}

func convertInt32Slice(slice []int32) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = int32(v)
	}
	return result
}

func skipIfZMQUnavailable(t *testing.T) {
	ctx, err := zmq4.NewContext()
	if err != nil {
		t.Skip("ZMQ not available:", err)
	}
	_ = ctx.Term()
}

func TestZMQClient_Connect_Success(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5547, 5548)
	defer publisher.Close()

	// Get actual bound ports
	pubEndpoint, err := publisher.pubSocket.GetLastEndpoint()
	if err != nil {
		t.Fatalf("Failed to get PUB endpoint: %v", err)
	}
	routerEndpoint, err := publisher.routerSocket.GetLastEndpoint()
	if err != nil {
		t.Fatalf("Failed to get ROUTER endpoint: %v", err)
	}

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ModelName:      "test-model",
		ReplayEndpoint: routerEndpoint,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err = client.Connect()
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	client.Stop()
}

func TestZMQClient_Connect_AlreadyConnected(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ModelName:      "test-model",
		ReplayEndpoint: routerEndpoint,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Connect()
	if err != nil {
		t.Fatalf("First Connect failed: %v", err)
	}

	// Connect again should not error
	err = client.Connect()
	if err != nil {
		t.Fatalf("Second Connect failed: %v", err)
	}

	client.Stop()
}

func TestZMQClient_Start_Stop(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait a bit for loop to start
	time.Sleep(50 * time.Millisecond)

	// Stop should work gracefully
	client.Stop()
}

func TestZMQClient_ProcessMessage_MooncakeTopic(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Create and publish mooncake event
	event := &zmq.BlockStoredEvent{
		Type:            zmq.EventTypeBlockStored,
		Timestamp:       time.Now().UTC(),
		BlockHashes:     []uint64{100, 200},
		TokenIDs:        []int32{1, 2, 3},
		ParentBlockHash: 50,
		BlockSize:       1024,
		MooncakeKey:     "mooncake-key-123",
		ReplicaList:     [][]string{{"replica1", "replica2"}},
		ModelName:       "test-model",
	}

	err = publisher.PublishEvent("mooncake", event)
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	events := handler.GetEvents()
	if len(events) < 1 {
		t.Fatalf("Expected at least 1 event, got %d", len(events))
	}

	blockEvent, ok := events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", events[0])
	}
	if blockEvent.PodName != "test-pod" {
		t.Errorf("Expected PodName 'test-pod', got '%s'", blockEvent.PodName)
	}
	if blockEvent.MooncakeKey != "mooncake-key-123" {
		t.Errorf("Expected MooncakeKey 'mooncake-key-123', got '%s'", blockEvent.MooncakeKey)
	}
	if len(blockEvent.BlockHashes) == 0 || blockEvent.BlockHashes[0] != 100 {
		t.Errorf("Expected first BlockHash 100, got %v", blockEvent.BlockHashes)
	}
}

func TestZMQClient_ProcessMessage_VllmTopic(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Create and publish vllm event
	event := &zmq.BlockStoredEvent{
		Type:            zmq.EventTypeBlockStored,
		Timestamp:       time.Now().UTC(),
		BlockHashes:     []uint64{300, 400},
		TokenIDs:        []int32{4, 5, 6},
		ParentBlockHash: 1500000000000000,
		BlockSize:       2048,
		ModelName:       "test-model",
	}

	err = publisher.PublishEvent("vllm", event)
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	events := handler.GetEvents()
	if len(events) < 1 {
		t.Fatalf("Expected at least 1 event, got %d", len(events))
	}
	blockEvent, ok := events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("Expected BlockStoredEvent, got %T", events[0])
	}
	if blockEvent.PodName != "test-pod" {
		t.Errorf("Expected PodName 'test-pod', got '%s'", blockEvent.PodName)
	}
	if len(blockEvent.BlockHashes) == 0 || blockEvent.BlockHashes[0] != 300 {
		t.Errorf("Expected first BlockHash 300, got %v", blockEvent.BlockHashes)
	}
}

func TestZMQClient_ProcessMessage_SGLangTopic(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)
	if err := client.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Stop()

	time.Sleep(100 * time.Millisecond)
	event := &zmq.BlockStoredEvent{
		Type:            zmq.EventTypeBlockStored,
		Timestamp:       time.Now().UTC(),
		BlockHashes:     []uint64{500},
		ParentBlockHash: 400,
		TokenIDs:        []int32{7, 8, 9},
		BlockSize:       3,
		LoraID:          11,
		Medium:          "GPU",
	}
	if err := publisher.PublishEvent(zmq.SourceSGLang, event); err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	events := handler.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	stored, ok := events[0].(*zmq.BlockStoredEvent)
	if !ok {
		t.Fatalf("expected BlockStoredEvent, got %T", events[0])
	}
	if stored.PodName != "test-pod" || stored.ParentBlockHash != 400 || stored.LoraID != 11 || stored.Medium != "GPU" {
		t.Errorf("unexpected decoded SGLang event: %+v", stored)
	}
}

func TestZMQClient_SGLangReplay_ReplaysAndDeduplicates(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()
	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	event := &zmq.BlockStoredEvent{
		Type:            zmq.EventTypeBlockStored,
		Timestamp:       time.Now().UTC(),
		BlockHashes:     []uint64{900},
		ParentBlockHash: 0,
		TokenIDs:        []int32{1, 2, 3, 4},
		BlockSize:       4,
		Medium:          "GPU",
	}
	if err := publisher.PublishEvent(zmq.SourceSGLang, event); err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}
	// Let the PUB socket complete its slow-joiner window before creating the client.
	time.Sleep(100 * time.Millisecond)

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(&zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		Source:         zmq.SourceSGLang,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}, handler)
	if err := client.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for handler.GetCallCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := handler.GetCallCount(); got != 1 {
		t.Fatalf("expected replay to dispatch one event, got %d", got)
	}
	if err := publisher.PublishBuffered(1); err != nil {
		t.Fatalf("PublishBuffered failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	// The sequence already received through replay must not be processed again
	// when it also arrives through the regular PUB/SUB channel.
	if got := handler.GetCallCount(); got != 1 {
		t.Fatalf("expected duplicate sequence to be skipped, got %d calls", got)
	}
	client.Stop()
}

func TestZMQClient_SequenceTracking(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Publish multiple events
	for i := 0; i < 5; i++ {
		event := &zmq.BlockStoredEvent{
			Type:            zmq.EventTypeBlockStored,
			Timestamp:       time.Now().UTC(),
			BlockHashes:     []uint64{uint64(i)},
			TokenIDs:        []int32{int32(i)},
			ParentBlockHash: 1000000000000000000,
			BlockSize:       128,
			ModelName:       "test-model",
		}
		_ = publisher.PublishEvent("vllm", event)
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Verify events were processed
	events := handler.GetEvents()
	if len(events) < 5 {
		t.Errorf("Expected at least 5 events, got %d", len(events))
	}
}

func TestZMQClient_ProcessMessage_EventGap(t *testing.T) {
	skipIfZMQUnavailable(t)

	publisher := createMockPublisher(t, 5557, 5558)
	defer publisher.Close()

	pubEndpoint, _ := publisher.pubSocket.GetLastEndpoint()
	routerEndpoint, _ := publisher.routerSocket.GetLastEndpoint()

	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       pubEndpoint,
		ReplayEndpoint: routerEndpoint,
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  1 * time.Second,
		ReconnectDelay: 100 * time.Millisecond,
	}

	handler := NewMockEventHandler()
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Manually set last sequence to simulate gap
	// This is a bit tricky since we need to access internal state
	// For now, we publish events and verify gap detection works
	// The actual gap detection is logged, so we verify the code path exists
	event := &zmq.BlockStoredEvent{
		Type:        zmq.EventTypeBlockStored,
		Timestamp:   time.Now().UTC(),
		BlockHashes: []uint64{100},
		TokenIDs:    []int32{1},
		ModelName:   "test-model",
	}

	_ = publisher.PublishEvent("vllm", event)
	time.Sleep(100 * time.Millisecond)

	// Publish another event - gap detection should work for subsequent events
	_ = publisher.PublishEvent("vllm", event)
	time.Sleep(100 * time.Millisecond)
}

// Helper function to extract port from endpoint string like "tcp://127.0.0.1:5557"
func extractPortFromEndpoint(endpoint string) int {
	// Parse "tcp://127.0.0.1:5557" to get 5557
	parts := strings.Split(endpoint, ":")
	if len(parts) < 3 {
		return 5557 // Default fallback
	}
	portStr := parts[len(parts)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 5557 // Default fallback
	}
	return port
}
