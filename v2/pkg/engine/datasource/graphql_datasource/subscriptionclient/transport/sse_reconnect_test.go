package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/graphql_datasource/subscriptionclient/common"
)

func TestSSETransport_ReconnectAbruptStream(t *testing.T) {
	for _, method := range []common.SSEMethod{common.SSEMethodPOST, common.SSEMethodGET} {
		t.Run(string(method), func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer test", r.Header.Get("Authorization"))
				if method == common.SSEMethodPOST {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.JSONEq(t, `{"query":"subscription Test { value }","operationName":"Test","variables":{"id":1},"extensions":{"test":true}}`, string(body))
				} else {
					require.Equal(t, "subscription Test { value }", r.URL.Query().Get("query"))
					require.Equal(t, "Test", r.URL.Query().Get("operationName"))
					require.JSONEq(t, `{"id":1}`, r.URL.Query().Get("variables"))
					require.JSONEq(t, `{"test":true}`, r.URL.Query().Get("extensions"))
				}
				attempt := requests.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "event: next\ndata: {\"data\":{\"value\":%d}}\n\n", attempt)
				w.(http.Flusher).Flush()
				if attempt == 1 {
					conn, _, err := w.(http.Hijacker).Hijack()
					require.NoError(t, err)
					conn.Close()
					return
				}
				fmt.Fprint(w, "event: complete\ndata:\n\n")
			}))
			defer server.Close()
			tr := NewSSETransport(t.Context(), server.Client(), nil)
			handler, receive := collectingHandler()
			cancel, err := tr.Subscribe(context.Background(), &common.Request{
				Query: "subscription Test { value }", OperationName: "Test", Variables: []byte(`{"id":1}`), Extensions: []byte(`{"test":true}`),
			}, common.Options{Endpoint: server.URL, SSEMethod: method, Headers: http.Header{"Authorization": {"Bearer test"}}}, handler)
			require.NoError(t, err)
			defer cancel()
			for _, value := range []int{1, 2} {
				msg := receive(t, 2*time.Second)
				require.Equal(t, common.MessageTypeData, msg.Type, "subscription must survive stream rotation: %v", msg.Err)
				require.JSONEq(t, fmt.Sprintf(`{"value":%d}`, value), string(msg.Payload.Data))
			}
			require.Equal(t, common.MessageTypeComplete, receive(t, time.Second).Type)
			require.EqualValues(t, 2, requests.Load())
			require.Eventually(t, func() bool { return tr.ConnCount() == 0 }, time.Second, time.Millisecond)
		})
	}
}

// A fake HTTP client lets synctest advance real production backoff timers without
// wall-clock waits. The separate hijacked-server test covers actual HTTP framing.
type reconnectRoundTripper func(*http.Request) (*http.Response, error)

func (f reconnectRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSSETransport_ReconnectFailureLimit(t *testing.T) {
	for _, mode := range []string{"read", "status", "content-type", "network"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var attempts int
				var times []time.Time
				client := &http.Client{Transport: reconnectRoundTripper(func(r *http.Request) (*http.Response, error) {
					attempts++
					times = append(times, time.Now())
					resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}
					if attempts > 1 {
						switch mode {
						case "status":
							resp.StatusCode = 503
							resp.Body = io.NopCloser(strings.NewReader("unavailable"))
						case "content-type":
							resp.Header.Set("Content-Type", "application/json")
						case "network":
							return nil, io.ErrClosedPipe
						}
					}
					return resp, nil
				})}
				tr := NewSSETransport(t.Context(), client, nil)
				handler, receive := collectingHandler()
				cancel, err := tr.Subscribe(t.Context(), &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: "http://example.test", SSEMethod: common.SSEMethodGET}, handler)
				require.NoError(t, err)
				defer cancel()
				require.Equal(t, common.MessageTypeConnectionError, receive(t, time.Minute).Type)
				synctest.Wait()
				require.Equal(t, 10, attempts)
				require.Zero(t, tr.ConnCount())
				for i := 1; i < len(times); i++ {
					require.Equal(t, min(250*time.Millisecond<<(i-1), 5*time.Second), times[i].Sub(times[i-1]))
				}
			})
		})
	}
}

func TestSSETransport_ReconnectCancellation(t *testing.T) {
	for _, mode := range []string{"unsubscribe", "subscription", "transport", "request"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transportCtx, stopTransport := context.WithCancel(t.Context())
				defer stopTransport()
				subscriptionCtx, stopSubscription := context.WithCancel(t.Context())
				defer stopSubscription()
				attempts, messages := 0, 0
				client := &http.Client{Transport: reconnectRoundTripper(func(r *http.Request) (*http.Response, error) {
					attempts++
					if attempts > 1 {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}, nil
				})}
				tr := NewSSETransport(transportCtx, client, nil)
				cancel, err := tr.Subscribe(subscriptionCtx, &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: "http://example.test", SSEMethod: common.SSEMethodGET}, func(*common.Message) { messages++ })
				require.NoError(t, err)
				defer cancel()
				synctest.Wait() // read loop is now in backoff
				if mode == "request" {
					time.Sleep(250 * time.Millisecond)
					synctest.Wait()
				}
				switch mode {
				case "subscription":
					stopSubscription()
				case "transport":
					stopTransport()
				default:
					cancel()
				}
				synctest.Wait()
				require.Zero(t, messages)
				require.Zero(t, tr.ConnCount())
				expected := 1
				if mode == "request" {
					expected = 2
				}
				require.Equal(t, expected, attempts)
			})
		})
	}
}

type delayedReadError struct{}

func (delayedReadError) Read([]byte) (int, error) {
	time.Sleep(time.Second)
	return 0, io.ErrUnexpectedEOF
}

func TestSSETransport_ReconnectHealthyStreamResetsFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		client := &http.Client{Transport: reconnectRoundTripper(func(r *http.Request) (*http.Response, error) {
			attempts++
			var body io.Reader = &errorReader{err: io.ErrUnexpectedEOF}
			// Alternate unhealthy and healthy streams beyond the consecutive failure cap.
			if attempts%2 == 0 {
				body = io.MultiReader(strings.NewReader("event: next\ndata: {\"data\":{}}\n\n"), delayedReadError{})
			}
			if attempts == 24 {
				body = strings.NewReader("event: complete\ndata:\n\n")
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(body)}, nil
		})}
		tr := NewSSETransport(t.Context(), client, nil)
		terminal := make(chan *common.Message, 1)
		start := time.Now()
		cancel, err := tr.Subscribe(t.Context(), &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: "http://example.test", SSEMethod: common.SSEMethodGET}, func(msg *common.Message) {
			if msg.Type.IsTerminal() {
				terminal <- msg
			}
		})
		require.NoError(t, err)
		defer cancel()
		require.Equal(t, common.MessageTypeComplete, (<-terminal).Type)
		synctest.Wait()
		require.Equal(t, 24, attempts)
		require.Equal(t, 11*time.Second+12*250*time.Millisecond, time.Since(start))
		require.Zero(t, tr.ConnCount())
	})
}

func TestSSETransport_ReconnectTerminalEvents(t *testing.T) {
	for _, tc := range []struct {
		name, stream string
		want         common.MessageType
	}{
		{"complete", "event: complete\ndata:\n\n", common.MessageTypeComplete},
		{"error", "event: error\ndata: [{\"message\":\"denied\"}]\n\n", common.MessageTypeError},
		{"malformed", "event: next\ndata: invalid-json\n\n", common.MessageTypeConnectionError},
		{"clean EOF", "", common.MessageTypeConnectionError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				client := &http.Client{Transport: reconnectRoundTripper(func(r *http.Request) (*http.Response, error) {
					attempts++
					var body io.Reader = &errorReader{err: io.ErrUnexpectedEOF}
					if attempts > 1 {
						body = strings.NewReader(tc.stream)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(body)}, nil
				})}
				tr := NewSSETransport(t.Context(), client, nil)
				handler, receive := collectingHandler()
				cancel, err := tr.Subscribe(t.Context(), &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: "http://example.test", SSEMethod: common.SSEMethodPOST}, handler)
				require.NoError(t, err)
				defer cancel()
				require.Equal(t, tc.want, receive(t, time.Second).Type)
				synctest.Wait()
				require.Equal(t, 2, attempts)
				require.Zero(t, tr.ConnCount())
			})
		})
	}
}
