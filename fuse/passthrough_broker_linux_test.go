package fuse

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeBroker is a minimal, scriptable stand-in for the real node-side mount
// broker: it accepts one connection at a time and runs a caller-supplied
// handler against it, so tests can control exactly how (and how many bytes
// at a time) a reply is written back.
type fakeBroker struct {
	ln   net.Listener
	sock string
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "broker.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &fakeBroker{ln: ln, sock: sock}
}

func (f *fakeBroker) serveOnce(t *testing.T, handle func(c *net.UnixConn)) {
	t.Helper()
	go func() {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		handle(c.(*net.UnixConn))
	}()
}

func (f *fakeBroker) close() { f.ln.Close() }

// TestRoundTripHandlesSplitReply is a regression test for the bug where a
// legitimate short read (a reply delivered across two separate writes on the
// stream socket) was treated as an ambiguous failure: on the final retry
// attempt it left `lastErr` nil, so roundTrip returned (nil, nil) and both
// callers (registerBacking/unregisterBacking) then panicked indexing the nil
// reply. io.ReadFull should transparently accumulate the split reply instead
// of ever surfacing this as an error.
func TestRoundTripHandlesSplitReply(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	want := []byte{0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // id=42, errno=0
	fb.serveOnce(t, func(c *net.UnixConn) {
		buf := make([]byte, 4096)
		if _, err := c.Read(buf); err != nil {
			return
		}
		// Deliver the 8-byte reply as two separate writes, exactly the
		// scenario that used to be misread as an ambiguous failure.
		c.Write(want[:3])
		time.Sleep(10 * time.Millisecond)
		c.Write(want[3:])
	})

	b := newBrokerConn(fb.sock, "mnt")
	reply, err := b.roundTrip([]byte{'B'}, -1, 8)
	if err != nil {
		t.Fatalf("roundTrip: unexpected error: %v", err)
	}
	if len(reply) != 8 {
		t.Fatalf("roundTrip: got %d bytes, want 8", len(reply))
	}
	for i := range want {
		if reply[i] != want[i] {
			t.Fatalf("roundTrip: reply[%d] = %#x, want %#x", i, reply[i], want[i])
		}
	}
}

// TestRoundTripNeverReturnsNilNil is a regression test: on a genuine,
// irrecoverable failure (broker never accepts/responds), roundTrip must
// return a non-nil error — never (nil, nil), which panics callers that only
// check `err != nil` before indexing the reply.
func TestRoundTripNeverReturnsNilNil(t *testing.T) {
	dir := t.TempDir()
	// Nothing listens on this path — every dial attempt fails.
	b := newBrokerConn(filepath.Join(dir, "nobody-home.sock"), "mnt")
	reply, err := b.roundTrip([]byte{'B'}, -1, 8)
	if err == nil {
		t.Fatalf("roundTrip: got nil error against an unreachable broker")
	}
	if reply != nil {
		t.Fatalf("roundTrip: got non-nil reply alongside an error")
	}
}

// TestRegisterBackingRejectsInvalidID is a regression test: a broker that
// returns errno=0 with a non-positive id violates FUSE_DEV_IOC_BACKING_OPEN's
// documented success contract (id>0); registerBacking must not pass that
// through as if it were a valid registration.
func TestRegisterBackingRejectsInvalidID(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	fb.serveOnce(t, func(c *net.UnixConn) {
		buf := make([]byte, 4096)
		if _, err := c.Read(buf); err != nil {
			return
		}
		// id=0, errno=0 — exactly the contract violation under test.
		c.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	})

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer devNull.Close()

	b := newBrokerConn(fb.sock, "mnt")
	id, errno := b.registerBacking(&BackingMap{Fd: int32(devNull.Fd())})
	if errno == 0 {
		t.Fatalf("registerBacking: accepted id=%d errno=0 as valid", id)
	}
}

// TestPassthroughBrokerScopedPerServer is a regression test: two *Server
// instances must not share one brokerConn (and its mutex) — a hung/slow
// broker on one mount must not wedge passthrough registration for another.
func TestPassthroughBrokerScopedPerServer(t *testing.T) {
	os.Setenv("UVOL_BROKER_SOCKET", "/nonexistent-for-this-test.sock")
	defer os.Unsetenv("UVOL_BROKER_SOCKET")

	s1 := &Server{}
	s2 := &Server{}
	b1 := s1.passthroughBroker()
	b2 := s2.passthroughBroker()
	if b1 == nil || b2 == nil {
		t.Fatalf("passthroughBroker() = nil, nil; want non-nil brokerConn objects (socket env var is set)")
	}
	if b1 == b2 {
		t.Fatalf("two *Server instances share the same *brokerConn; expected independent connections")
	}

	// Once per Server: repeated calls return the same cached instance.
	if again := s1.passthroughBroker(); again != b1 {
		t.Fatalf("passthroughBroker() returned a different instance on second call")
	}
}

// TestPassthroughBrokerNilWithoutEnv confirms the no-broker-configured path
// (bare ioctl mode) still resolves to a nil *brokerConn per Server.
func TestPassthroughBrokerNilWithoutEnv(t *testing.T) {
	os.Unsetenv("UVOL_BROKER_SOCKET")
	s := &Server{}
	if b := s.passthroughBroker(); b != nil {
		t.Fatalf("passthroughBroker() = %v, want nil with no UVOL_BROKER_SOCKET set", b)
	}
}
