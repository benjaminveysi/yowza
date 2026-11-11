//go:build unix

package platform

import "golang.org/x/sys/unix"

func munmapCodeSegment(code []byte) error {
	return unix.Munmap(code)
}

// MprotectCodeSegment is like unix.Mprotect with RX permission.
func MprotectCodeSegment(b []byte) (err error) {
	return unix.Mprotect(b, unix.PROT_READ|unix.PROT_EXEC)
}

func freeMemoryRegion(memory []byte) error {
	return unix.Munmap(memory[:cap(memory)])
}

func commitMemoryRegion(memory []byte) error {
	// On Unix, PROT_READ|PROT_WRITE with MAP_ANON automatically lazy-commits on write, so we do nothing.
	return nil
}
