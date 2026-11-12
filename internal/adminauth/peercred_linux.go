//go:build linux

package adminauth

import (
	"errors"
	"net"
	"syscall"
)

func peerIdentity(conn net.Conn) (PeerIdentity, error) {
	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return PeerIdentity{}, errors.New("unix connection does not expose OS credentials")
	}
	var cred *syscall.Ucred
	var controlErr error
	raw, err := sysConn.SyscallConn()
	if err != nil {
		return PeerIdentity{}, err
	}
	if err := raw.Control(func(fd uintptr) {
		cred, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return PeerIdentity{}, err
	}
	if controlErr != nil || cred == nil {
		if controlErr != nil {
			return PeerIdentity{}, controlErr
		}
		return PeerIdentity{}, errors.New("peer credentials unavailable")
	}
	return PeerIdentity{UID: int(cred.Uid), GID: int(cred.Gid), PID: int(cred.Pid)}, nil
}
