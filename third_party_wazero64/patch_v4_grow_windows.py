import re

with open("internal/wasm/memory.go", "r") as f:
    content = f.read()

# Fix Grow64 logic for Windows/Mmap
# We need to make sure we don't reallocate the entire region using AllocateMemoryRegion!
# Wait, the user specifically told us:
# "تو زمان تخصیص اولیه (توی AllocateMemoryRegion)، کل فضای Max رو فقط با MEM_RESERVE بگیر."
# "بلافاصله همونجا، به اندازه Min (حافظه اولیه) از همون آدرس شروع کن و با MEM_COMMIT کامیتش کن."
# "توی تابع Grow64، وقتی دیتابیس درخواست افزایش حافظه میده، باید دوباره API ویندوز رو صدا بزنی و فقط همون deltaBytes جدید رو تو ادامه حافظه‌ی قبلی با MEM_COMMIT فعال کنی."

# Ah! In my patch I reverted to `AllocateMemoryRegion` for `newLen` instead of `maxBytes` because tests failed!
# I reverted `AllocateMemoryRegion` from `allocBytes` to `minBytes` initially, and in `Grow64` I reallocate!
# But the user EXPLICITLY wants to reserve `Max` initially and commit piecemeal!
# Let's fix this properly. For 64-bit Memory, we should reserve `Max`!
# The reason tests failed initially when I did this was because of 32-bit tests mapped 4GB with `MAP_NORESERVE` but then I removed `MAP_NORESERVE`.
# I should re-introduce reserving `Max` ONLY for `IsMemory64`.
# Let's check `NewMemoryInstance`:
