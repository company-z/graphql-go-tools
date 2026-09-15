package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r3labs/sse/v2"

	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/graphql_datasource/subscriptionclient/common"
)

const (
	sseMaxConsecutiveFailures = 10
	sseInitialRetryDelay      = 250 * time.Millisecond
	sseMaxRetryDelay          = 5 * time.Second
	sseHealthyStreamDuration  = time.Second

	// sseMaxEventSize bounds a single SSE event read from a subgraph. Upstream caps
	// this at 64 KiB, which is smaller than the batched payloads our subgraphs
	// emit routinely (event feeds and filtered token tables run to hundreds of
	// KiB per event). The engine this replaced had no cap and subgraphs are
	// trusted internal services, so reads stay unbounded.
	sseMaxEventSize = math.MaxInt
)

var (
	headerData  = []byte("data:")
	headerEvent = []byte("event:")
)

// sseConnection owns a subscription across successive SSE response streams.
type sseConnection struct {
	mu        sync.Mutex // Protects response replacement against concurrent close.
	ctx       context.Context
	reconnect func() (*http.Response, error)
	resp      *http.Response
	handler   common.Handler
	closed    atomic.Bool
	onClose   func() // Callback function to notify parent transport that the connection was closed.
}

func newSSEConnection(resp *http.Response, handler common.Handler, onClose func()) *sseConnection {
	return &sseConnection{
		resp:    resp,
		handler: handler,
		onClose: onClose,
	}
}

// readLoop reads SSE events from the response body and delivers them to the handler.
// Every exit path delivers a terminal message to the handler unless the connection
// was closed by the consumer.
func (c *sseConnection) readLoop() {
	defer c.cleanup()

	failures := 0
	for {
		started := time.Now()
		delivered, err := c.readStream()
		if err == nil || c.closed.Load() || (c.ctx != nil && c.ctx.Err() != nil) {
			return
		}
		// A clean EOF and protocol errors remain terminal. Only interrupted reads
		// retry. An oversized event is not an interruption: replaying the
		// subscription would hit the same event again and loop forever.
		if c.reconnect == nil || errors.Is(err, io.EOF) || errors.Is(err, bufio.ErrTooLong) {
			c.sendError(err)
			return
		}
		// Healthy rotations reconnect immediately; short-lived failures share the
		// same bounded budget as failed HTTP reconnect attempts.
		if delivered && time.Since(started) >= sseHealthyStreamDuration {
			failures = 0
		} else {
			failures++
		}
		c.resp.Body.Close()
		for {
			if failures >= sseMaxConsecutiveFailures {
				c.sendError(err)
				return
			}
			if failures > 0 {
				delay := min(sseInitialRetryDelay<<(failures-1), sseMaxRetryDelay)
				timer := time.NewTimer(delay)
				select {
				case <-c.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if c.closed.Load() || c.ctx.Err() != nil {
				return
			}
			var resp *http.Response
			resp, err = c.reconnect()
			if err != nil {
				failures++
				continue
			}
			c.mu.Lock()
			if c.closed.Load() || c.ctx.Err() != nil {
				c.mu.Unlock()
				resp.Body.Close()
				return
			}
			c.resp = resp
			c.mu.Unlock()
			break
		}
	}
}

// readStream returns transport read errors to the reconnect loop. Protocol-level
// terminal messages are delivered directly and must never trigger a replay.
func (c *sseConnection) readStream() (delivered bool, err error) {
	reader := sse.NewEventStreamReader(c.resp.Body, sseMaxEventSize)
	for {
		if c.closed.Load() {
			return delivered, nil
		}
		eventBytes, err := reader.ReadEvent()
		if err != nil {
			return delivered, err
		}
		eventType, data := c.parseEventBytes(eventBytes)
		if eventType == "" && data == nil {
			continue
		}
		msg := c.parseEvent(eventType, data)
		if c.closed.Load() || (c.ctx != nil && c.ctx.Err() != nil) {
			return delivered, nil
		}
		c.handler(msg)
		if msg.Type.IsTerminal() {
			return delivered, nil
		}
		delivered = true
	}
}

// parseEventBytes extracts the event type and data from raw SSE event bytes.
// Based on r3labs/sse's processEvent but simplified for our needs.
func (c *sseConnection) parseEventBytes(msg []byte) (eventType string, data []byte) {
	if len(msg) == 0 {
		return "", nil
	}

	for line := range bytes.Lines(msg) {
		line = bytes.TrimRight(line, "\r\n")
		switch {
		case bytes.HasPrefix(line, headerEvent):
			eventType = string(trimHeader(len(headerEvent), line))

		case bytes.HasPrefix(line, headerData):
			// The spec allows for multiple data fields per event, concatenated with "\n"
			data = append(data, trimHeader(len(headerData), line)...)
			data = append(data, '\n')

		case bytes.Equal(line, []byte("data")):
			// A line that simply contains "data" should be treated as empty data
			data = append(data, '\n')

			// Comments (lines starting with ':') are already filtered by EventStreamReader
		}
	}

	// Trim the trailing "\n" per SSE spec
	data = bytes.TrimSuffix(data, []byte("\n"))

	return eventType, data
}

// trimHeader removes the header prefix and optional leading space.
func trimHeader(size int, data []byte) []byte {
	if len(data) < size {
		return data
	}

	data = data[size:]
	// Remove optional leading whitespace (single space after colon)
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	return data
}

// parseEvent converts parsed SSE event data into a shared.Message.
func (c *sseConnection) parseEvent(eventType string, data []byte) *common.Message {
	switch eventType {
	case "next":
		var resp common.ExecutionResult
		if err := json.Unmarshal(data, &resp); err != nil {
			return &common.Message{Type: common.MessageTypeConnectionError, Err: err}
		}
		return &common.Message{Type: common.MessageTypeData, Payload: &resp}

	case "error":
		return &common.Message{
			Type:    common.MessageTypeError,
			Payload: &common.ExecutionResult{Errors: data},
		}

	case "complete":
		return &common.Message{Type: common.MessageTypeComplete}

	default:
		// Unknown event type or no event type specified - treat as data
		// This handles servers that send data without an event type
		if len(data) == 0 {
			return &common.Message{Type: common.MessageTypeComplete}
		}
		var resp common.ExecutionResult
		if err := json.Unmarshal(data, &resp); err != nil {
			return &common.Message{Type: common.MessageTypeConnectionError, Err: err}
		}
		return &common.Message{Type: common.MessageTypeData, Payload: &resp}
	}
}

func (c *sseConnection) sendError(err error) {
	if c.closed.Load() || (c.ctx != nil && c.ctx.Err() != nil) {
		return
	}
	c.handler(&common.Message{Type: common.MessageTypeConnectionError, Err: err})
}

func (c *sseConnection) cleanup() {
	c.closeConn()

	if c.onClose != nil {
		c.onClose()
	}
}

// closeConn terminates the SSE connection.
func (c *sseConnection) closeConn() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.resp.Body.Close()
}
