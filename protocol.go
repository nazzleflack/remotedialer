package remotedialer

import (
	"net/http"
	"strconv"
)

// ProtocolVersionHeader is used by client and server to agree on a protocol version during the websocket handshake.
// A missing header means the other end only supports protocolVersionLegacy.
const ProtocolVersionHeader = "X-Remotedialer-Protocol-Version"

const (
	protocolVersionLegacy = 1
	// protocolVersionSyncTop adds the SyncConnectionsTop message type
	protocolVersionSyncTop = 2

	latestProtocolVersion = protocolVersionSyncTop
)

// Variables to allow tests to emulate older clients and servers
var (
	clientProtocolVersion = latestProtocolVersion
	serverProtocolVersion = latestProtocolVersion
)

// protocolVersionFromHeader returns the protocol version found in the headers, or protocolVersionLegacy if there is none
func protocolVersionFromHeader(headers http.Header) int {
	version, err := strconv.Atoi(headers.Get(ProtocolVersionHeader))
	if err != nil || version < protocolVersionLegacy {
		return protocolVersionLegacy
	}
	return version
}

// negotiateProtocolVersion returns the highest version supported by both ends
func negotiateProtocolVersion(local, remote int) int {
	if remote < local {
		return remote
	}
	return local
}

// withProtocolVersion returns a copy of the headers including the protocol version, unless it is protocolVersionLegacy
func withProtocolVersion(headers http.Header, version int) http.Header {
	if version <= protocolVersionLegacy {
		return headers
	}
	result := headers.Clone()
	if result == nil {
		result = http.Header{}
	}
	result.Set(ProtocolVersionHeader, strconv.Itoa(version))
	return result
}
