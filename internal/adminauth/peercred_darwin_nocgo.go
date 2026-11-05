//go:build darwin && !cgo

package adminauth

import (
	"errors"
	"net"
)

func peerIdentity(net.Conn) (PeerIdentity, error) {
	return PeerIdentity{}, errors.New("macOS peer credentials require cgo")
}
