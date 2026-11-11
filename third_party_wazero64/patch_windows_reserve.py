import re

with open("internal/platform/mmap_windows.go", "r") as f:
    content = f.read()

# Make AllocateMemoryRegion just MEM_RESERVE!
old_alloc = """func allocateMemoryRegion(size uint64) ([]byte, error) {
	if size > uint64(^uintptr(0)) {
		return nil, windows.ERROR_NOT_ENOUGH_MEMORY
	}
	address := uintptr(0)
	p, err := windows.VirtualAlloc(address, uintptr(size),
		windows.MEM_RESERVE|windows.MEM_COMMIT, windows.PAGE_READWRITE)
	if err != nil {
		return nil, err
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(p)), size), nil
}"""

new_alloc = """func allocateMemoryRegion(size uint64) ([]byte, error) {
	if size > uint64(^uintptr(0)) {
		return nil, windows.ERROR_NOT_ENOUGH_MEMORY
	}
	address := uintptr(0)
	p, err := windows.VirtualAlloc(address, uintptr(size),
		windows.MEM_RESERVE, windows.PAGE_READWRITE)
	if err != nil {
		return nil, err
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(p)), size), nil
}"""
if "windows.MEM_RESERVE|windows.MEM_COMMIT" in content:
    content = content.replace("windows.MEM_RESERVE|windows.MEM_COMMIT", "windows.MEM_RESERVE")

with open("internal/platform/mmap_windows.go", "w") as f:
    f.write(content)
