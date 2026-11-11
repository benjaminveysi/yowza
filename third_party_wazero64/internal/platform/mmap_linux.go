package platform

import (
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

var hugePagesConfigs []hugePagesConfig

type hugePagesConfig struct {
	size int
	flag int
}

func (hpc *hugePagesConfig) match(size int) bool {
	return (size & (hpc.size - 1)) == 0
}

func init() {
	dirents, err := os.ReadDir("/sys/kernel/mm/hugepages/")
	if err != nil {
		return
	}

	for _, dirent := range dirents {
		name := dirent.Name()
		if !strings.HasPrefix(name, "hugepages-") {
			continue
		}
		if !strings.HasSuffix(name, "kB") {
			continue
		}
		n, err := strconv.ParseUint(name[10:len(name)-2], 10, 64)
		if err != nil {
			continue
		}
		if bits.OnesCount64(n) != 1 {
			continue
		}
		n *= 1024
		hugePagesConfigs = append(hugePagesConfigs, hugePagesConfig{
			size: int(n),
			flag: int(bits.TrailingZeros64(n)<<unix.MAP_HUGE_SHIFT) | unix.MAP_HUGETLB,
		})
	}

	sort.Slice(hugePagesConfigs, func(i, j int) bool {
		return hugePagesConfigs[i].size > hugePagesConfigs[j].size
	})
}

// hugePagesAvailable reports whether the kernel's hugepage pool for this
// page size can actually back an allocation of the given size.
//
// This check is load-bearing, not an optimisation: mmap with MAP_HUGETLB
// (especially combined with MAP_NORESERVE, as in allocateMemoryRegion)
// succeeds even when the pool is empty, and the first touch of the mapping
// then faults with SIGBUS, which is fatal and unrecoverable in Go. Checking
// free_hugepages before choosing MAP_HUGETLB is the only safe way to avoid
// that fault. If the file cannot be read, be conservative and skip hugetlb.
func hugePagesAvailable(cfg hugePagesConfig, size int) bool {
	// cfg.size is in bytes; the sysfs directory name is in kB.
	path := "/sys/kernel/mm/hugepages/hugepages-" +
		strconv.Itoa(cfg.size/1024) + "kB/free_hugepages"
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	free, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return false
	}
	need := uint64(size / cfg.size)
	return free >= need
}

func mmapCodeSegment(size int) ([]byte, error) {
	flag := unix.MAP_ANON | unix.MAP_PRIVATE
	prot := unix.PROT_READ | unix.PROT_WRITE

	for _, hugePagesConfig := range hugePagesConfigs {
		if hugePagesConfig.match(size) && hugePagesAvailable(hugePagesConfig, size) {
			b, err := unix.Mmap(-1, 0, size, prot, flag|hugePagesConfig.flag)
			if err != nil {
				continue
			}
			return b, nil
		}
	}

	return unix.Mmap(-1, 0, size, prot, flag)
}

func allocateMemoryRegion(size uint64) ([]byte, error) {
	flag := unix.MAP_ANON | unix.MAP_PRIVATE | unix.MAP_NORESERVE
	prot := unix.PROT_READ | unix.PROT_WRITE

	for _, hugePagesConfig := range hugePagesConfigs {
		if hugePagesConfig.match(int(size)) && hugePagesAvailable(hugePagesConfig, int(size)) {
			b, err := unix.Mmap(-1, 0, int(size), prot, flag|hugePagesConfig.flag)
			if err != nil {
				continue
			}
			return b, nil
		}
	}

	return unix.Mmap(-1, 0, int(size), prot, flag)
}
