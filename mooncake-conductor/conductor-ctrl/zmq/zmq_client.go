package zmq

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type ZMQClient struct {
	config *ZMQClientConfig

	subSocket    *zmq.Socket
	replaySocket *zmq.Socket

	eventHandler EventHandler

	// State management
	mu             sync.RWMutex
	processMu      sync.Mutex
	connected      bool
	lastSeq        int64
	reconnectDelay time.Duration

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewZMQClient(config *ZMQClientConfig, handler EventHandler) *ZMQClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &ZMQClient{
		config:         config,
		eventHandler:   handler,
		lastSeq:        -1,
		reconnectDelay: config.ReconnectDelay,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start initiates the connection and background event consumption loop.
func (c *ZMQClient) Start() error {
	// Attempt initial connection
	if err := c.Connect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}
	c.wg.Add(1)
	go c.loop()
	// SGLang retains a bounded event buffer behind its replay ROUTER. Replay
	// runs independently so a slow or temporarily unavailable replay service
	// never prevents the HTTP control plane from starting.
	if c.config.Source == SourceSGLang && c.config.ReplayEndpoint != "" {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			if err := c.requestReplay(1); err != nil {
				slog.Warn("Initial SGLang replay failed", "service", c.config.CachePoolKey, "error", err)
			}
		}()
	}

	slog.Info("ZMQ client started", "service", c.config.CachePoolKey)
	return nil
}

func (c *ZMQClient) Stop() {
	c.cancel()
	c.wg.Wait()

	c.mu.Lock()
	c.cleanupSockets()
	c.mu.Unlock()

	slog.Info("ZMQ client stopped", "service", c.config.CachePoolKey)
}

// loop is the main background loop handling events and reconnections.
// Simplified: Fixed reconnect interval, single loop structure.
func (c *ZMQClient) loop() {
	defer c.wg.Done()

	for {
		// Check if we should stop
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 1. If disconnected, wait for ticker then try to reconnect
		if !c.isConnected() {
			c.handleReconnect()
			continue
		}

		// 2. If connected, consume events
		if err := c.consume(); err != nil {
			slog.Error("Consumption error", "service", c.config.CachePoolKey, "error", err)
			c.markDisconnected()
		}
	}
}

func (c *ZMQClient) handleReconnect() {
	slog.Info("Attempting to reconnect to the service.", "service", c.config.CachePoolKey, "reconnectDelay", c.reconnectDelay)

	ticker := time.NewTicker(c.config.ReconnectDelay)
	defer ticker.Stop()

	select {
	case <-c.ctx.Done():
		return
	case <-ticker.C:
	}

	if err := c.Connect(); err != nil {
		slog.Error("Reconnect failed", "service", c.config.CachePoolKey, "error", err)
	}

	// Reconnected! Request replay from last known sequence
	lastSeq := c.getLastSequence()
	if lastSeq >= 0 && c.config.ReplayEndpoint != "" {
		slog.Info("Reconnected", "service", c.config.CachePoolKey, "resuming_from", lastSeq+1)
		if err := c.requestReplay(lastSeq + 1); err != nil {
			slog.Warn("Failed to request replay after reconnect", "service", c.config.CachePoolKey, "error", err)
		}
	}

}

// Connect establishes the ZMQ SUB and DEALER sockets.
func (c *ZMQClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	// Ensure clean state
	c.cleanupSockets()

	sock, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("create socket failed: %w", err)
	}

	if err := sock.SetIpv6(true); err != nil {
		_ = sock.Close()
		return fmt.Errorf("failed to enable IPv6 on socket: %w", err)
	}

	if err := sock.Connect(c.config.Endpoint); err != nil {
		_ = sock.Close()
		return fmt.Errorf("failed to connect to %s: %w", c.config.Endpoint, err)
	}

	// Important: Subscribe to all topics
	if err := sock.SetSubscribe(""); err != nil {
		_ = sock.Close()
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	replaySocket, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		sock.Close()
		return fmt.Errorf("failed to create DEALER socket: %w", err)
	}

	// Enable IPv6 for dual-stack support
	if err := replaySocket.SetIpv6(true); err != nil {
		_ = sock.Close()
		_ = replaySocket.Close()
		return fmt.Errorf("failed to enable IPv6 on DEALER socket: %w", err)
	}

	if err := replaySocket.Connect(c.config.ReplayEndpoint); err != nil {
		_ = sock.Close()
		_ = replaySocket.Close()
		return fmt.Errorf("failed to connect to replay endpoint %s: %w", c.config.ReplayEndpoint, err)
	}

	c.subSocket = sock
	c.replaySocket = replaySocket
	c.connected = true

	c.reconnectDelay = c.config.ReconnectDelay

	slog.Info("Successfully connected to KV event publisher", "service", c.config.CachePoolKey, "endpoint", c.config.Endpoint)

	return nil
}

// consume reads and processes messages from the SUB socket.
func (c *ZMQClient) consume() error {
	c.mu.RLock()
	socket := c.subSocket
	c.mu.RUnlock()

	if socket == nil {
		return fmt.Errorf("socket is nil")
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	// Poll for data
	polled, err := poller.Poll(c.config.PollTimeout)
	if err != nil {
		return fmt.Errorf("poll error: %w", err)
	}
	if len(polled) == 0 {
		return nil // No data, continue loop
	}

	if err := c.processMessage(socket); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	return nil

}

func (c *ZMQClient) processMessage(socket *zmq.Socket) error {

	if socket == nil {
		return fmt.Errorf("socket is nil")
	}

	// Read Frames: [Topic, Seq, Payload]
	topic, err := socket.RecvBytes(0)
	if err != nil {
		return err
	}
	seqBytes, err := socket.RecvBytes(0)
	if err != nil {
		return err
	}
	payload, err := socket.RecvBytes(0)
	if err != nil {
		return err
	}

	if len(seqBytes) != 8 {
		return fmt.Errorf("invalid sequence length")
	}
	seq := int64(binary.BigEndian.Uint64(seqBytes))

	return c.processPayload(seq, payload, string(topic))

}

// processPayload decodes and dispatches both PUB/SUB batches and replay
// batches. Sequence de-duplication is deliberately shared: SGLang may replay
// an event that was already delivered through PUB/SUB while reconnecting.
func (c *ZMQClient) processPayload(seq int64, payload []byte, source string) error {
	c.processMu.Lock()
	defer c.processMu.Unlock()

	c.mu.RLock()
	lastSeq := c.lastSeq
	c.mu.RUnlock()
	if seq <= lastSeq {
		slog.Debug("Skipping already processed KV event", "service", c.config.CachePoolKey, "seq", seq, "last", lastSeq)
		return nil
	}
	if lastSeq != -1 && seq > lastSeq+1 {
		slog.Warn("Event gap detected", "service", c.config.CachePoolKey, "missed", seq-lastSeq-1, "last", lastSeq, "current", seq)
	}

	batch, err := decodeBatch(source, payload)
	if err != nil {
		return fmt.Errorf("decode failed: %w", err)
	}
	for _, event := range batch.Events {
		switch e := event.(type) {
		case *BlockStoredEvent:
			e.PodName = c.config.CachePoolKey
		case *BlockRemovedEvent:
			e.PodName = c.config.CachePoolKey
		}
		if err := c.eventHandler.HandleEvent(event, batch.DataParallelRank); err != nil {
			slog.Error("Handler error", "service", c.config.CachePoolKey, "error", err)
		}
	}

	c.mu.Lock()
	if seq > c.lastSeq {
		c.lastSeq = seq
	}
	c.mu.Unlock()
	slog.Debug("Processed batch", "service", c.config.CachePoolKey, "seq", seq, "source", source)
	return nil
}

func decodeBatch(source string, payload []byte) (*EventBatch, error) {
	switch source {
	case SourceMooncake:
		return DecodeMooncakeEventBatch(payload)
	case SourceSGLang:
		return DecodeSGLangEventBatch(payload)
	default:
		return DecodeVllmEventBatch(payload)
	}
}

func (c *ZMQClient) requestReplay(fromSeq int64) error {
	c.mu.RLock()
	socket := c.replaySocket
	c.mu.RUnlock()

	if socket == nil {
		return fmt.Errorf("replay socket is nil")
	}

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(fromSeq))
	if _, err := socket.SendMessage([]byte{}, req); err != nil {
		return fmt.Errorf("failed to send replay request: %w", err)
	}
	_ = socket.SetRcvtimeo(c.config.ReplayTimeout)
	if c.config.Source != SourceSGLang {
		resp, err := socket.RecvBytes(0)
		if err != nil {
			return fmt.Errorf("failed to receive replay response: %w", err)
		}
		slog.Info("Replay requested", "service", c.config.CachePoolKey, "from", fromSeq, "resp_len", len(resp))
		return nil
	}

	for {
		frames, err := socket.RecvMessageBytes(0)
		if err != nil {
			return fmt.Errorf("failed to receive SGLang replay response: %w", err)
		}
		// libzmq versions differ in whether the ROUTER's empty delimiter is
		// surfaced to a DEALER receiver. Accept both [seq, payload] and
		// [empty, seq, payload] while keeping the SGLang wire contract strict.
		if len(frames) == 3 && len(frames[0]) == 0 {
			frames = frames[1:]
		}
		if len(frames) != 2 || len(frames[0]) != 8 {
			return fmt.Errorf("invalid SGLang replay frame: %d parts", len(frames))
		}
		seq := int64(binary.BigEndian.Uint64(frames[0]))
		if seq == -1 {
			slog.Info("SGLang replay completed", "service", c.config.CachePoolKey, "from", fromSeq)
			return nil
		}
		if err := c.processPayload(seq, frames[1], SourceSGLang); err != nil {
			return fmt.Errorf("process SGLang replay sequence %d: %w", seq, err)
		}
	}
}

func (c *ZMQClient) cleanupSockets() {
	if c.subSocket != nil {
		c.subSocket.Close()
		c.subSocket = nil
	}
	if c.replaySocket != nil {
		c.replaySocket.Close()
		c.replaySocket = nil
	}
	c.connected = false
}

func (c *ZMQClient) markDisconnected() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
}

func (c *ZMQClient) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *ZMQClient) getLastSequence() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSeq
}
