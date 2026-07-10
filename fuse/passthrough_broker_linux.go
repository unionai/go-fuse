// Broker-delegated FUSE passthrough registration.
//
// FUSE_DEV_IOC_BACKING_OPEN requires CAP_SYS_ADMIN in the initial user
// namespace, which an unprivileged container does not have. When the daemon
// serves a channel premounted by a node-side mount broker (fd handoff), the
// broker performs the ioctl on the daemon's behalf: the daemon sends the
// backing fd over the broker's control socket and gets the kernel backing ID
// back. Enabled by environment (set by the runtime that launches the
// daemon):
//
//	UVOL_BROKER_SOCKET   path to the broker control socket
//	UVOL_BROKER_SUBPATH  channel name within the pod's broker volume
//
// Wire protocol (one connection, serial request/reply):
//
//	register:   'B' + subpath, backing fd as SCM_RIGHTS → 8 bytes (id, errno)
//	unregister: 'C' + subpath + ":" + id                → 4 bytes (errno)
package fuse

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
)

var passthroughBroker = newBrokerConn(
	os.Getenv("UVOL_BROKER_SOCKET"), os.Getenv("UVOL_BROKER_SUBPATH"))

type brokerConn struct {
	sock string
	sub  string

	mu   sync.Mutex
	conn *net.UnixConn
}

func newBrokerConn(sock, sub string) *brokerConn {
	if sock == "" {
		return nil
	}
	if sub == "" {
		sub = "mnt"
	}
	return &brokerConn{sock: sock, sub: sub}
}

func (b *brokerConn) dial() error {
	if b.conn != nil {
		return nil
	}
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: b.sock, Net: "unix"})
	if err != nil {
		return err
	}
	b.conn = c
	return nil
}

func (b *brokerConn) reset() {
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
}

// roundTrip sends one request (with optional fd) and reads want reply bytes.
// One transparent retry on a fresh connection covers broker restarts.
func (b *brokerConn) roundTrip(msg []byte, fd int, want int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			b.reset()
		}
		if err := b.dial(); err != nil {
			lastErr = err
			continue
		}
		var rights []byte
		if fd >= 0 {
			rights = syscall.UnixRights(fd)
		}
		if _, _, err := b.conn.WriteMsgUnix(msg, rights, nil); err != nil {
			lastErr = err
			continue
		}
		reply := make([]byte, want)
		n, err := b.conn.Read(reply)
		if err != nil || n < want {
			lastErr = err
			continue
		}
		return reply, nil
	}
	b.reset()
	return nil, lastErr
}

func (b *brokerConn) registerBacking(m *BackingMap) (int32, syscall.Errno) {
	reply, err := b.roundTrip(append([]byte{'B'}, b.sub...), int(m.Fd), 8)
	if err != nil {
		log.Printf("passthrough broker: register backing: %v", err)
		return -1, syscall.EIO
	}
	id := int32(binary.LittleEndian.Uint32(reply[0:]))
	errno := syscall.Errno(binary.LittleEndian.Uint32(reply[4:]))
	return id, errno
}

func (b *brokerConn) unregisterBacking(id int32) syscall.Errno {
	msg := append([]byte{'C'}, (b.sub + ":" + strconv.Itoa(int(id)))...)
	reply, err := b.roundTrip(msg, -1, 4)
	if err != nil {
		log.Printf("passthrough broker: unregister backing %d: %v", id, err)
		return syscall.EIO
	}
	return syscall.Errno(binary.LittleEndian.Uint32(reply))
}
