package remotedialer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func Test_encodeConnectionIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		size int
	}{
		{0},
		{1},
		{2},
		{10},
		{100},
		{1000},
	}
	for x := range tests {
		tt := tests[x]
		t.Run(fmt.Sprintf("%d_ids", tt.size), func(t *testing.T) {
			t.Parallel()
			ids, top := generateIDs(tt.size)

			decoded, err := decodeConnectionIDs(encodeConnectionIDs(ids))
			if err != nil {
				t.Error(err)
			}
			if got, want := decoded, ids; !reflect.DeepEqual(got, want) {
				t.Errorf("encoding and decoding differs from original data, got: %v, want: %v", got, want)
			}

			decoded, decodedTop, err := decodeConnectionIDsWithTop(encodeConnectionIDsWithTop(top, ids))
			if err != nil {
				t.Error(err)
			}
			if got, want := decodedTop, top; got != want {
				t.Errorf("encoding and decoding top differs from original data, got: %v, want: %v", got, want)
			}
			if got, want := decoded, ids; !reflect.DeepEqual(got, want) {
				t.Errorf("encoding and decoding with top differs from original data, got: %v, want: %v", got, want)
			}
		})
	}
}

func Test_decodeConnectionIDs_invalid(t *testing.T) {
	t.Parallel()
	if _, err := decodeConnectionIDs(make([]byte, 12)); err == nil {
		t.Error("expected an error decoding a payload that is not a multiple of 8 bytes")
	}
	if _, _, err := decodeConnectionIDsWithTop(nil); err == nil {
		t.Error("expected an error decoding a payload with no top")
	}
	if _, _, err := decodeConnectionIDsWithTop(make([]byte, 12)); err == nil {
		t.Error("expected an error decoding a payload that is not a multiple of 8 bytes")
	}
}

func Test_diffSortedSetsGetRemoved(t *testing.T) {
	t.Parallel()
	tests := []struct {
		server, client, expected []int64
	}{
		{
			// same ids
			server:   []int64{2, 4, 6},
			client:   []int64{2, 4, 6},
			expected: nil,
		},
		{
			// Client keeps all ids from the server, additional ids are okay
			server:   []int64{1, 2, 3},
			client:   []int64{1, 2, 3, 4, 5},
			expected: nil,
		},
		{
			// Client closed some ids kept by the server
			server:   []int64{1, 2, 3, 4, 5},
			client:   []int64{1, 2, 3},
			expected: []int64{4, 5},
		},
		{
			// Combined case
			server:   []int64{1, 2, 3, 4, 5},
			client:   []int64{3, 6},
			expected: []int64{1, 2, 4, 5},
		},
	}
	for x := range tests {
		tt := tests[x]
		t.Run(fmt.Sprintf("case_%d", x), func(t *testing.T) {
			t.Parallel()
			if got, want := diffSortedSetsGetRemoved(tt.server, tt.client), tt.expected; !reflect.DeepEqual(got, want) {
				t.Errorf("unexpected result, got: %v, want: %v", got, want)
			}
		})
	}
}

func TestSession_sendSyncConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		protocolVersion int
		expectedType    messageType
	}{
		{"legacy server gets the legacy message", protocolVersionLegacy, SyncConnections},
		{"server supporting top gets the new message", protocolVersionSyncTop, SyncConnectionsTop},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data := make(chan []byte)
			conn := testServerWS(t, data)
			session := NewClientSession(nil, conn)
			session.protocolVersion = tt.protocolVersion

			for _, n := range []int{0, 5, 20} {
				ids, _ := generateIDs(n)
				for _, id := range ids {
					session.addConnection(id, nil)
				}

				if err := session.sendSyncConnections(); err != nil {
					t.Fatal(err)
				}
				message, err := newServerMessage(bytes.NewBuffer(<-data))
				if err != nil {
					t.Fatal(err)
				}
				payload, err := io.ReadAll(message.body)
				if err != nil {
					t.Fatal(err)
				}

				if got, want := message.messageType, tt.expectedType; got != want {
					t.Fatalf("incorrect message type, got: %v, want: %v", got, want)
				}

				activeIDs, top := session.activeConnectionIDs()
				var decoded []int64
				if tt.expectedType == SyncConnectionsTop {
					var decodedTop int64
					decoded, decodedTop, err = decodeConnectionIDsWithTop(payload)
					if got, want := decodedTop, top; got != want {
						t.Errorf("incorrect top, got: %v, want: %v", got, want)
					}
					if len(activeIDs) > 0 && top != activeIDs[len(activeIDs)-1] {
						t.Errorf("top should be the highest connection ID received, got: %v, want: %v", top, activeIDs[len(activeIDs)-1])
					}
				} else {
					decoded, err = decodeConnectionIDs(payload)
				}
				if err != nil {
					t.Fatal(err)
				}
				if got, want := decoded, activeIDs; !reflect.DeepEqual(got, want) {
					t.Errorf("incorrect connections IDs, got: %v, want: %v", got, want)
				}
			}
		})
	}
}

// TestSession_syncConnections_legacyMessage tests that every connection missing from a SyncConnections message is still closed
func TestSession_syncConnections_legacyMessage(t *testing.T) {
	t.Parallel()

	s := setupDummySession(t, 0)
	for _, id := range []int64{2, 3, 4, 5} {
		s.addConnection(id, newConnection(id, s, "test", "test"))
	}

	if err := s.serveMessage(context.Background(), bytes.NewReader(newSyncConnectionsMessage([]int64{3, 4}).Bytes())); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.activeConnectionIDs(); !reflect.DeepEqual(got, []int64{3, 4}) {
		t.Errorf("unexpected connections after legacy sync, got: %v, want: [3 4]", got)
	}

	// A SyncConnectionsTop message keeps connections above top
	s.addConnection(6, newConnection(6, s, "test", "test"))
	if err := s.serveMessage(context.Background(), bytes.NewReader(newSyncConnectionsTopMessage(4, []int64{3}).Bytes())); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.activeConnectionIDs(); !reflect.DeepEqual(got, []int64{3, 6}) {
		t.Errorf("unexpected connections after sync with top, got: %v, want: [3 6]", got)
	}
}

// TestProtocolVersionNegotiation tests the protocol version used by clients for every combination of old and new clients and servers
func TestProtocolVersionNegotiation(t *testing.T) {
	origClientVersion, origServerVersion := clientProtocolVersion, serverProtocolVersion
	t.Cleanup(func() {
		clientProtocolVersion = origClientVersion
		serverProtocolVersion = origServerVersion
	})

	tests := []struct {
		name          string
		clientVersion int
		serverVersion int
		expected      int
	}{
		{"legacy client, legacy server", protocolVersionLegacy, protocolVersionLegacy, protocolVersionLegacy},
		{"legacy client, new server", protocolVersionLegacy, protocolVersionSyncTop, protocolVersionLegacy},
		{"new client, legacy server", protocolVersionSyncTop, protocolVersionLegacy, protocolVersionLegacy},
		{"new client, new server", protocolVersionSyncTop, protocolVersionSyncTop, protocolVersionSyncTop},
		{"future client, new server", latestProtocolVersion + 1, latestProtocolVersion, latestProtocolVersion},
		{"new client, future server", latestProtocolVersion, latestProtocolVersion + 1, latestProtocolVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			serverAddr, _, err := newTestServer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			serverProtocolVersion = tt.serverVersion
			clientProtocolVersion = tt.clientVersion

			sessions := make(chan *Session, 1)
			clientDone := make(chan struct{})
			defer func() {
				cancel()
				<-clientDone
			}()
			go func() {
				defer close(clientDone)
				_ = ConnectToProxy(ctx, "ws://"+serverAddr, nil, func(string, string) bool { return true }, nil,
					func(_ context.Context, session *Session) error {
						sessions <- session
						return nil
					})
			}()

			var clientSession *Session
			select {
			case clientSession = <-sessions:
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for the client to connect")
			}
			if got, want := clientSession.protocolVersion, tt.expected; got != want {
				t.Errorf("unexpected client protocol version, got: %d, want: %d", got, want)
			}
		})
	}
}

func generateIDs(n int) ([]int64, int64) {
	ids := make([]int64, n)
	for x := range ids {
		ids[x] = rand.Int63()
	}
	return ids, rand.Int63()
}

func testServerWS(t *testing.T, data chan<- []byte) *websocket.Conn {
	t.Helper()

	var upgrader websocket.Upgrader
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				return
			}
			if data != nil {
				data <- message
			}
		}
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	url := "ws" + server.URL[4:] // http:// -> ws://
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestCompareAndCloseStaleConnections tests that compareAndCloseStaleConnections
// correctly handles the 'top' field: connections with IDs greater than top should
// never be closed (the client hasn't seen them yet), while stale connections with
// IDs <= top should be closed.
func TestCompareAndCloseStaleConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		serverConns    []int64
		clientConns    []int64
		top            int64
		expectedOpen   []int64
		expectedClosed []int64
	}{
		{
			name:           "no connections to close when lists match",
			serverConns:    []int64{1, 2, 3},
			clientConns:    []int64{1, 2, 3},
			top:            3,
			expectedOpen:   []int64{1, 2, 3},
			expectedClosed: nil,
		},
		{
			name:           "close stale connections at or below top",
			serverConns:    []int64{1, 2, 3, 4, 5},
			clientConns:    []int64{1, 3, 5},
			top:            5,
			expectedOpen:   []int64{1, 3, 5},
			expectedClosed: []int64{2, 4},
		},
		{
			name:           "preserve all connections above top",
			serverConns:    []int64{1, 2, 3, 4, 5},
			clientConns:    []int64{1, 2, 3},
			top:            3,
			expectedOpen:   []int64{1, 2, 3, 4, 5},
			expectedClosed: nil,
		},
		{
			name:           "mixed: close old stale and preserve new unknown",
			serverConns:    []int64{1, 2, 3, 4, 5, 10, 20},
			clientConns:    []int64{1, 3},
			top:            5,
			expectedOpen:   []int64{1, 3, 10, 20},
			expectedClosed: []int64{2, 4, 5},
		},
		{
			name:           "empty client list with low top preserves everything",
			serverConns:    []int64{5, 10, 15},
			clientConns:    []int64{},
			top:            0,
			expectedOpen:   []int64{5, 10, 15},
			expectedClosed: nil,
		},
		{
			name:           "empty client list with high top closes all",
			serverConns:    []int64{1, 2, 3},
			clientConns:    []int64{},
			top:            10,
			expectedOpen:   nil,
			expectedClosed: []int64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := setupDummySession(t, 0)
			for _, id := range tt.serverConns {
				conn := newConnection(id, s, "test", "test")
				s.addConnection(id, conn)
			}

			s.compareAndCloseStaleConnections(tt.clientConns, tt.top)

			for _, id := range tt.expectedOpen {
				if s.getConnection(id) == nil {
					t.Errorf("connection %d should be open but was closed", id)
				}
			}

			for _, id := range tt.expectedClosed {
				if s.getConnection(id) != nil {
					t.Errorf("connection %d should be closed but is still open", id)
				}
			}
		})
	}
}

// TestSyncConnectionsRace is an integration test that reproduces the race condition
// described in PR #150. When SyncConnectionsInterval is very small and many concurrent
// requests are being processed, the sync mechanism can close connections that the client
// hasn't yet received.
//
// The same load runs for old and new clients and servers:
//   - if either end only supports SyncConnections, the bug must be reproducible
//     (sync error rate > 10%)
//   - if both ends support SyncConnectionsTop, no request may fail
func TestSyncConnectionsRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	origInterval := SyncConnectionsInterval
	origTimeout := SyncConnectionsTimeout
	origClientVersion, origServerVersion := clientProtocolVersion, serverProtocolVersion

	// Aggressive sync settings to maximize the race window
	SyncConnectionsInterval = time.Millisecond
	SyncConnectionsTimeout = time.Second

	t.Cleanup(func() {
		SyncConnectionsInterval = origInterval
		SyncConnectionsTimeout = origTimeout
		clientProtocolVersion = origClientVersion
		serverProtocolVersion = origServerVersion
	})

	const totalRequests = 500
	const minLegacyErrorPercent = 10
	threshold := int64(totalRequests * minLegacyErrorPercent / 100)

	tests := []struct {
		name          string
		clientVersion int
		serverVersion int
		expectRace    bool
	}{
		{"legacy client, new server", protocolVersionLegacy, latestProtocolVersion, true},
		{"new client, legacy server", latestProtocolVersion, protocolVersionLegacy, true},
		{"new client, new server", latestProtocolVersion, latestProtocolVersion, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientProtocolVersion = tt.clientVersion
			serverProtocolVersion = tt.serverVersion
			totalErrs, syncErrs := runSyncRaceLoad(t, totalRequests)

			if tt.expectRace {
				if syncErrs <= threshold {
					t.Errorf("expected sync error rate >%d%% with the legacy sync, but got only %d/%d sync errors",
						minLegacyErrorPercent, syncErrs, totalRequests)
				}
				return
			}
			if totalErrs != 0 {
				t.Errorf("expected no errors, got %d/%d (%d of them sync errors)", totalErrs, totalRequests, syncErrs)
			}
		})
	}
}

// runSyncRaceLoad sets up a remotedialer tunnel and fires totalRequests concurrent HTTP
// requests through it, returning the total error count and sync-specific error count.
// SyncConnectionsInterval, SyncConnectionsTimeout and clientProtocolVersion must be set by the caller.
func runSyncRaceLoad(t *testing.T, totalRequests int) (totalErrors, syncErrors int64) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	// Backend HTTP server that returns a response body.
	responseBody := strings.Repeat("x", 4096)
	backendAddr, err := newServer(ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(responseBody))
	}))
	if err != nil {
		t.Fatal(err)
	}

	// remotedialer server
	serverAddr, server, err := newTestServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Connect client
	connected := make(chan struct{})
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		_ = ConnectToProxy(ctx, "ws://"+serverAddr, nil, func(string, string) bool { return true }, nil,
			func(context.Context, *Session) error {
				close(connected)
				return nil
			})
	}()
	select {
	case <-connected:
	case <-clientDone:
		t.Fatal("client could not connect")
	}

	// HTTP client that routes through the remotedialer tunnel
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				return server.Dialer("client")(ctx, network, address)
			},
			DisableKeepAlives: true,
		},
	}

	const concurrency = 50

	sem := make(chan struct{}, concurrency)
	var (
		wg           sync.WaitGroup
		errCount     int64
		syncErrCount int64
	)

	for i := 0; i < totalRequests; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()

			resp, err := client.Get("http://" + backendAddr)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				if strings.Contains(err.Error(), errCloseSyncConnections.Error()) {
					atomic.AddInt64(&syncErrCount, 1)
				}
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}

	wg.Wait()

	// Stop all goroutines before returning
	cancel()
	<-clientDone

	t.Logf("completed %d requests: %d total errors, %d sync errors",
		totalRequests, errCount, syncErrCount)

	return errCount, syncErrCount
}
