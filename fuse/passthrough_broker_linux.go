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
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// roundTripTimeout bounds each dial+write+read attempt. Without it a hung or
// slow broker blocks brokerConn.mu forever, wedging every passthrough
// registration/unregistration for that Server (see Server.passthroughBroker).
const roundTripTimeout = 5 * time.Second

// passthroughBroker returns this Server's own broker connection, creating it
// on first use. Scoped per-Server (via Server.passthroughBrokerOnce/Conn)
// rather than a package-wide global: go-fuse supports multiple concurrent
// *Server instances (mounts) in one process, and a bare global would route
// every mount's backing-fd registrations through one shared connection and
// mutex — a mount with a hung/slow broker would then wedge every other
// mount's passthrough registration too, not just its own.
func (ms *Server) passthroughBroker() *brokerConn {
	ms.passthroughBrokerOnce.Do(func() {
		ms.passthroughBrokerConn = newBrokerConn(
			os.Getenv("UVOL_BROKER_SOCKET"), os.Getenv("UVOL_BROKER_SUBPATH"))
	})
	b, _ := ms.passthroughBrokerConn.(*brokerConn)
	return b
}

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
//
// Uses io.ReadFull rather than a single Read: on a stream socket the
// broker's reply can legitimately arrive across more than one read, and a
// bare Read() that treats any short read as a failure has two costs — (1) a
// short read with err==nil looks identical to "nothing came back yet" to a
// naive check, so the LAST attempt's short read can leave the function
// returning (nil, nil), which callers then index into and panic on; and (2)
// treating a short read as a failure triggers a reconnect+resend of a
// non-idempotent request (register/unregister), double-registering a
// backing ID with the broker and leaking the first one. io.ReadFull keeps
// reading on the SAME connection until it has `want` bytes or hits a real
// error (EOF/closed/timeout) — only a genuine connection failure reaches the
// retry path below, and on success the returned slice is always fully
// populated (never nil with a nil error).
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
		if err := b.conn.SetDeadline(time.Now().Add(roundTripTimeout)); err != nil {
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
		if _, err := io.ReadFull(b.conn, reply); err != nil {
			lastErr = err
			continue
		}
		return reply, nil
	}
	b.reset()
	if lastErr == nil {
		lastErr = io.ErrUnexpectedEOF
	}
	return nil, lastErr
}

func (b *brokerConn) registerBacking(m *BackingMap) (int32, syscall.Errno) {
	// Dup m.Fd immediately, before roundTrip's b.mu.Lock() (which can block
	// for the entire duration of another in-flight round trip, and longer
	// still if that one is stuck against a hung broker — roundTripTimeout
	// bounds it, but not the queue of waiters behind it). m.Fd is caller-
	// owned; the caller's own close of it races this call regardless, but
	// dup'ing here — rather than sending the bare int untouched after an
	// unbounded wait — closes the window where a concurrent close-then-
	// reopen-elsewhere could hand the OS's reused fd number to a request
	// that thinks it's still sending the original file. Mirrors the same
	// dup-before-use fix already applied on the broker's own end of this
	// protocol.
	dup, err := syscall.Dup(int(m.Fd))
	if err != nil {
		log.Printf("passthrough broker: dup backing fd: %v", err)
		return -1, syscall.EIO
	}
	defer syscall.Close(dup)

	reply, err := b.roundTrip(append([]byte{'B'}, b.sub...), dup, 8)
	if err != nil {
		log.Printf("passthrough broker: register backing: %v", err)
		return -1, syscall.EIO
	}
	id := int32(binary.LittleEndian.Uint32(reply[0:]))
	errno := syscall.Errno(binary.LittleEndian.Uint32(reply[4:]))
	if errno == 0 && id <= 0 {
		// The kernel's FUSE_DEV_IOC_BACKING_OPEN contract guarantees id>0 on
		// success (see RegisterBackingFd's doc comment); a broker returning
		// errno=0 with id<=0 is violating that contract. Don't hand a bogus
		// "successful" id through — 0 is OpenOut.BackingID's own sentinel
		// for "passthrough not set" (see types.go), so silently accepting it
		// here would produce ambiguous downstream state.
		log.Printf("passthrough broker: register backing: broker returned errno=0 with invalid id=%d", id)
		return -1, syscall.EIO
	}
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
