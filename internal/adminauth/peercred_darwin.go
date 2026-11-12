//go:build darwin && cgo

package adminauth

/*
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
static int larm_getpeereid(int fd, uid_t *uid, gid_t *gid) {
	if (getpeereid(fd, uid, gid) != 0) return errno;
	return 0;
}
*/
import "C"

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
	var uid C.uid_t
	var gid C.gid_t
	var callErr error
	raw, err := sysConn.SyscallConn()
	if err != nil {
		return PeerIdentity{}, err
	}
	if err := raw.Control(func(fd uintptr) {
		if errnoValue := C.larm_getpeereid(C.int(fd), &uid, &gid); errnoValue != 0 {
			callErr = syscall.Errno(errnoValue)
		}
	}); err != nil {
		return PeerIdentity{}, err
	}
	if callErr != nil {
		return PeerIdentity{}, callErr
	}
	return PeerIdentity{UID: int(uid), GID: int(gid), PID: -1}, nil
}
