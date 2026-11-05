//go:build !linux && !darwin

package adminauth

import (
	"errors"
	"net"
)

func peerIdentity(net.Conn) (PeerIdentity, error) {
	return PeerIdentity{}, errors.New("platform does not provide supported Unix peer credentials")
}
