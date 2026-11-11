// Separated from linux which has support for huge pages.

//go:build unix && !linux

package platform

import "golang.org/x/sys/unix"

func mmapCodeSegment(size int) ([]byte, error) {
	return unix.Mmap(
		-1,
		0,
		size,
		unix.PROT_READ|unix.PROT_WRITE,
		// Anonymous as this is not an actual file, but a memory,
		// Private as this is in-process memory region.
		unix.MAP_ANON|unix.MAP_PRIVATE,
	)
}

func allocateMemoryRegion(size uint64) ([]byte, error) {
	if size > uint64(^uint(0)>>1) {
		return nil, unix.ENOMEM
	}
	return unix.Mmap(
		-1,
		0,
		int(size),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE,
	)
}

func commitMemoryRegion(memory []byte) error {
	// On Unix, PROT_READ|PROT_WRITE with MAP_ANON automatically lazy-commits on write, so we do nothing.
	return nil
}
