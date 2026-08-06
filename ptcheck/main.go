// Standalone check for the ported passthrough primitives.
// Run: go run ./ptcheck <emptydir-on-tmpfs> <backing-file-on-tmpfs>
//
// LIMITATION — read before trusting a green run as "passthrough works":
// this only exercises INIT-time capability negotiation and a raw
// RegisterBackingFd/UnregisterBackingFd round trip (ioctl, or the broker RPC
// when UVOL_BROKER_SOCKET is set) against a backing file opened directly by
// this process. It never opens a file THROUGH the mount with
// FOPEN_PASSTHROUGH + OpenOut.BackingID set — the actual kernel-bypass
// read/write path a real filesystem daemon (e.g. juicefs) drives — because
// no in-tree code in this repo sets FOPEN_PASSTHROUGH on a real Open
// response; that wiring belongs to the consumer. A successful run here
// proves the lower-level primitives round-trip; it does NOT prove reads/
// writes on a real open file are actually served via the backing file.
// Verify that separately against the real consumer before relying on it.
package main

import (
	"fmt"
	"os"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type root struct{ fs.Inode }

func main() {
	mnt := os.Args[1]
	server, err := fs.Mount(mnt, &root{}, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:             true,
			EnablePassthrough: true,
			MaxStackDepth:     2,
		},
	})
	if err != nil {
		fmt.Println("MOUNT ERR:", err)
		os.Exit(1)
	}
	fmt.Println(">>> SupportsPassthrough():", server.SupportsPassthrough())

	f, err := os.OpenFile(os.Args[2], os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("OPEN backing ERR:", err)
		server.Unmount()
		os.Exit(1)
	}
	id, errno := server.RegisterBackingFd(&fuse.BackingMap{Fd: int32(f.Fd())})
	fmt.Printf(">>> RegisterBackingFd: id=%d errno=%v\n", id, errno)
	if errno == 0 {
		fmt.Printf(">>> UnregisterBackingFd errno=%v\n", server.UnregisterBackingFd(id))
	}
	f.Close()
	server.Unmount()
	fmt.Println(">>> NOTE: this checks INIT negotiation + the raw backing-fd " +
		"round trip only — it does NOT open a file through the mount with " +
		"FOPEN_PASSTHROUGH set, so it does not prove end-to-end passthrough " +
		"reads/writes work. See the package doc comment.")
}
