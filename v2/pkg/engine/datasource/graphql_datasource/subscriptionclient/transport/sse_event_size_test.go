package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/graphql_datasource/subscriptionclient/common"
)

// Subgraph subscriptions batch many records into one SSE event, so a single
// event is regularly far larger than upstream's 64 KiB reader limit.
func TestSSETransport_DeliversLargeEvent(t *testing.T) {
	t.Parallel()
	data := `{"value":"` + strings.Repeat("x", 1<<20) + `"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: next\ndata: {\"data\":%s}\n\n", data)
		fmt.Fprint(w, "event: complete\ndata:\n\n")
	}))
	defer server.Close()
	tr := NewSSETransport(t.Context(), server.Client(), nil)
	handler, receive := collectingHandler()
	cancel, err := tr.Subscribe(context.Background(), &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: server.URL, SSEMethod: common.SSEMethodPOST}, handler)
	require.NoError(t, err)
	defer cancel()
	msg := receive(t, 5*time.Second)
	require.Equal(t, common.MessageTypeData, msg.Type, "a 1 MiB event must be delivered: %v", msg.Err)
	require.Equal(t, data, string(msg.Payload.Data))
	require.Equal(t, common.MessageTypeComplete, receive(t, time.Second).Type)
	require.Eventually(t, func() bool { return tr.ConnCount() == 0 }, time.Second, time.Millisecond)
}

// If a reader limit is ever reintroduced, hitting it must end the subscription
// instead of re-subscribing: the replayed stream would produce the same
// oversized event and the loop would never deliver anything.
func TestSSETransport_OversizedEventIsTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		client := &http.Client{Transport: reconnectRoundTripper(func(r *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(&errorReader{err: bufio.ErrTooLong})}, nil
		})}
		tr := NewSSETransport(t.Context(), client, nil)
		handler, receive := collectingHandler()
		cancel, err := tr.Subscribe(t.Context(), &common.Request{Query: "subscription { value }"}, common.Options{Endpoint: "http://example.test", SSEMethod: common.SSEMethodPOST}, handler)
		require.NoError(t, err)
		defer cancel()
		msg := receive(t, time.Minute)
		require.Equal(t, common.MessageTypeConnectionError, msg.Type)
		require.ErrorIs(t, msg.Err, bufio.ErrTooLong)
		synctest.Wait()
		require.Equal(t, 1, attempts, "an oversized event must not trigger a re-subscribe")
		require.Zero(t, tr.ConnCount())
	})
}
