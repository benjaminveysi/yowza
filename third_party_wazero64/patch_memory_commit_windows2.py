import re

with open("internal/wasm/memory.go", "r") as f:
    content = f.read()

# Since we commit `buffer` directly during allocation, we only commit the required initial size.
# In `Grow64`, we also call CommitMemoryRegion! Let's check Grow64:
