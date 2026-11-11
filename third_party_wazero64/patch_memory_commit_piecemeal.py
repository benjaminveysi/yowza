import re

with open("internal/wasm/memory.go", "r") as f:
    content = f.read()

# Fix Grow64 logic to just commit the piecemeal parts and reslice!
# In `NewMemoryInstance`, it already reserves `allocBytes` which is `maxBytes` (unless `capBytes` is somehow bigger, which doesn't make sense).
# Then it does `buffer = b[:minBytes]` and `platform.CommitMemoryRegion(buffer)`.
# So `m.Buffer` has capacity `maxBytes` but length `minBytes`.
# Wait, `platform.CommitMemoryRegion` on `b[:minBytes]` only commits the first `minBytes`!
# This is perfect!
# But what about `Grow64`?
old_grow64 = """		if m.mmap {
			newLen := MemoryPagesToBytesNum(newPages)
			newBuffer, err := platform.AllocateMemoryRegion(newLen)
			if err != nil {
				return 0, false
			}
			copy(newBuffer, m.Buffer)
			if err := platform.CommitMemoryRegion(newBuffer); err != nil {
				platform.FreeMemoryRegion(newBuffer)
				return 0, false
			}
			platform.FreeMemoryRegion(m.Buffer)
			m.Buffer = newBuffer
			m.Cap = newPages
		} else {"""
new_grow64 = """		if m.mmap {
			// For mmapped memory, we already reserved the virtual address space up to `Max`.
			// So we can simply reslice up to the new length, and then commit the newly required pages to physical memory!
			newLen := MemoryPagesToBytesNum(newPages)

			// If for some reason the capacity of the reserved region is smaller than we need (which shouldn't happen unless Max was smaller than we're trying to grow), we fail.
			if newLen > uint64(cap(m.Buffer)) {
				return 0, false
			}

			// Commit only the newly grown region
			deltaBytes := MemoryPagesToBytesNum(delta)
			oldLen := uint64(len(m.Buffer))

			// We temporarily expand the slice to include the new region
			expandedBuf := m.Buffer[:newLen]
			grownRegion := expandedBuf[oldLen : oldLen+deltaBytes]

			if err := platform.CommitMemoryRegion(grownRegion); err != nil {
				return 0, false
			}

			m.Buffer = expandedBuf
			m.Cap = newPages
		} else {"""

content = content.replace(old_grow64, new_grow64)

# Wait, `Grow(delta uint32)` should ALSO do this for 32-bit if it's mmapped!
# But `IsMemory64` is false, so it doesn't use mmap right now.
# We'll leave `Grow` as it is (it uses append) unless `m.mmap` is true, wait `m.mmap` is only true for 64-bit right now!
old_grow = """		if m.mmap {
			newLen := MemoryPagesToBytesNum(uint64(newPages))
			newBuffer, err := platform.AllocateMemoryRegion(newLen)
			if err != nil {
				return 0, false
			}
			copy(newBuffer, m.Buffer)
			if err := platform.CommitMemoryRegion(newBuffer); err != nil {
				platform.FreeMemoryRegion(newBuffer)
				return 0, false
			}
			platform.FreeMemoryRegion(m.Buffer)
			m.Buffer = newBuffer
			m.Cap = newPages
		} else {"""
new_grow = """		if m.mmap {
			// Reslice and commit
			newLen := MemoryPagesToBytesNum(uint64(newPages))
			if newLen > uint64(cap(m.Buffer)) {
				return 0, false
			}
			deltaBytes := MemoryPagesToBytesNum(uint64(delta))
			oldLen := uint64(len(m.Buffer))
			expandedBuf := m.Buffer[:newLen]
			grownRegion := expandedBuf[oldLen : oldLen+deltaBytes]

			if err := platform.CommitMemoryRegion(grownRegion); err != nil {
				return 0, false
			}
			m.Buffer = expandedBuf
			m.Cap = newPages
		} else {"""

content = content.replace(old_grow, new_grow)

with open("internal/wasm/memory.go", "w") as f:
    f.write(content)
