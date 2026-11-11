import re

with open("internal/platform/mmap_windows.go", "r") as f:
    content = f.read()

# Fix MEM_RESERVE / MEM_COMMIT logic on Windows
# AllocateMemoryRegion must just RESERVE
# CommitMemoryRegion must just COMMIT
# This was actually exactly what I did in my previous patch!
# Let's check `allocateMemoryRegion` in `mmap_windows.go`.
