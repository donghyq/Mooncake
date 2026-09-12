package zmq

import (
	"fmt"
	"time"
)

// EventHandler processes received KV events
type EventHandler interface {
	HandleEvent(event KVEvent, dpRank int64) error
}

// ZMQClientConfig contains configuration for the ZMQ client
type ZMQClientConfig struct {
	CachePoolKey   string
	Endpoint       string
	ReplayEndpoint string
	// Source selects the decoder for replay payloads, whose ROUTER protocol
	// does not include the PUB/SUB topic frame.
	Source         string
	ModelName      string
	PollTimeout    time.Duration
	ReplayTimeout  time.Duration
	ReconnectDelay time.Duration
}

const (
	// Timeouts and intervals
	DefaultPollTimeout       = 100 * time.Millisecond
	DefaultReplayTimeout     = 5 * time.Second
	DefaultReconnectInterval = 1 * time.Second
	MaxReconnectInterval     = 30 * time.Second
	ReconnectBackoffFactor   = 2.0

	EventChannelBufferSize = 1000
)

func ValidateConfig(config *ZMQClientConfig) error {
	if config.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	return nil
}
