package remotedialer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

var errCloseSyncConnections = errors.New("sync from client")

// syncTopUnknown is the top used for SyncConnections messages, which do not include one
const syncTopUnknown int64 = math.MaxInt64

// encodeConnectionIDs serializes a slice of connection IDs
func encodeConnectionIDs(ids []int64) []byte {
	return appendConnectionIDs(make([]byte, 0, 8*len(ids)), ids)
}

// encodeConnectionIDsWithTop serializes the highest connection ID received by the client, followed by a slice of connection IDs
func encodeConnectionIDsWithTop(top int64, ids []int64) []byte {
	payload := make([]byte, 0, 8*(len(ids)+1))
	payload = binary.LittleEndian.AppendUint64(payload, uint64(top))
	return appendConnectionIDs(payload, ids)
}

func appendConnectionIDs(payload []byte, ids []int64) []byte {
	for _, id := range ids {
		payload = binary.LittleEndian.AppendUint64(payload, uint64(id))
	}
	return payload
}

// decodeConnectionIDs deserializes a slice of connection IDs
func decodeConnectionIDs(payload []byte) ([]int64, error) {
	if len(payload)%8 != 0 {
		return nil, fmt.Errorf("incorrect data format")
	}
	result := make([]int64, 0, len(payload)/8)
	for x := 0; x < len(payload); x += 8 {
		id := binary.LittleEndian.Uint64(payload[x : x+8])
		result = append(result, int64(id))
	}
	return result, nil
}

// decodeConnectionIDsWithTop deserializes a slice of connection IDs preceded by the highest connection ID received by the client
func decodeConnectionIDsWithTop(payload []byte) ([]int64, int64, error) {
	if len(payload) < 8 {
		return nil, 0, fmt.Errorf("incorrect data format")
	}
	top := int64(binary.LittleEndian.Uint64(payload[:8]))
	ids, err := decodeConnectionIDs(payload[8:])
	return ids, top, err
}

func newSyncConnectionsMessage(connectionIDs []int64) *message {
	return &message{
		id:          nextid(),
		messageType: SyncConnections,
		bytes:       encodeConnectionIDs(connectionIDs),
	}
}

func newSyncConnectionsTopMessage(top int64, connectionIDs []int64) *message {
	return &message{
		id:          nextid(),
		messageType: SyncConnectionsTop,
		bytes:       encodeConnectionIDsWithTop(top, connectionIDs),
	}
}

// sendSyncConnections sends a binary message of type SyncConnections or SyncConnectionsTop, depending on the negotiated
// protocol version, whose payload is a list of the active connection IDs for this session
func (s *Session) sendSyncConnections() error {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()

	activeIDs, top := s.activeConnectionIDs()

	msg := newSyncConnectionsMessage(activeIDs)
	if s.protocolVersion >= protocolVersionSyncTop {
		msg = newSyncConnectionsTopMessage(top, activeIDs)
	}

	_, err := s.writeMessage(time.Now().Add(SyncConnectionsTimeout), msg)
	return err
}

// compareAndCloseStaleConnections compares the Session's activeConnectionIDs with the provided list from the client, then closing every connection not present in it.
// Connections with an ID higher than top are kept, as the client had not received them yet when it built the list.
func (s *Session) compareAndCloseStaleConnections(clientIDs []int64, top int64) {
	serverIDs, _ := s.activeConnectionIDs()
	toClose := diffSortedSetsGetRemoved(serverIDs, clientIDs)
	if len(toClose) == 0 {
		return
	}

	s.Lock()
	defer s.Unlock()

	for _, id := range toClose {
		if id > top {
			break // not continue as toClose is sorted
		}

		// Connection no longer active in the client, close it server-side
		conn := s.removeConnectionLocked(id)

		if conn != nil {
			// Using doTunnelClose directly instead of tunnelClose, omitting unnecessarily sending an Error message
			conn.doTunnelClose(errCloseSyncConnections)
		}
	}
}

// diffSortedSetsGetRemoved compares two sorted slices and returns those items present in a that are not present in b
// similar to coreutil's "comm -23"
func diffSortedSetsGetRemoved(a, b []int64) []int64 {
	var res []int64
	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] { // present in "a", not in "b"
			res = append(res, a[i])
			i++
		} else if a[i] > b[j] { // present in "b", not in "a"
			j++
		} else { // present in both
			i++
			j++
		}
	}
	res = append(res, a[i:]...) // any remainders in "a" are also removed from "b"
	return res
}
