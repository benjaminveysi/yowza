#include "arena.h"

// Helper function to align a memory pointer forward.
// Returns (size_t)-1 on overflow instead of wrapping around: a wrapped
// result used to pass the bounds check in arena_alloc and move the arena
// offset backwards, overwriting live data.
size_t arena_align_forward(size_t size, size_t alignment) {
    if (alignment == 0) return size;
    size_t modulo = size & (alignment - 1);
    if (modulo != 0) {
        size_t add = alignment - modulo;
        if (size > (size_t)-1 - add) return (size_t)-1;
        size += add;
    }
    return size;
}

void arena_init(Arena* arena, void* buffer, size_t size) {
    if (arena == NULL) return;

    arena->buffer = (uint8_t*)buffer;
    arena->size   = size;
    arena->offset = 0;
}

void* arena_alloc(Arena* arena, size_t size) {
    if (arena == NULL || arena->buffer == NULL) return NULL;

    // We align all allocations to 8 bytes to avoid memory alignment issues
    // common in both C and WebAssembly.
    size_t aligned_offset = arena_align_forward(arena->offset, 8);
    size_t aligned_size = arena_align_forward(size, 8);

    if (aligned_offset == (size_t)-1 || aligned_size == (size_t)-1) {
        // Overflow while aligning: never allocate.
        return NULL;
    }
    // Overflow-free bounds check: never compute aligned_offset + aligned_size
    // directly, it could wrap past arena->size for a huge request.
    if (aligned_offset > arena->size || aligned_size > arena->size - aligned_offset) {
        // Out of memory for this arena
        return NULL;
    }

    void* ptr = &arena->buffer[aligned_offset];
    arena->offset = aligned_offset + aligned_size;

    // In a zero-dependency Wasm build, we don't have memset.
    // If the user wants it zeroed out, they will zero it themselves,
    // or we can implement a custom memory zero here if needed.
    // For raw performance, we don't zero it out by default.

    return ptr;
}

void arena_reset(Arena* arena) {
    if (arena != NULL) {
        arena->offset = 0;
    }
}
