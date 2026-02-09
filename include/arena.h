#ifndef YOWZA_ARENA_H
#define YOWZA_ARENA_H

#ifdef __wasm__
// Using specific bit widths to avoid stdint.h dependency in Wasm build
typedef unsigned long long uint64_t;
typedef unsigned int       uint32_t;
typedef unsigned short     uint16_t;
typedef unsigned char      uint8_t;
// Standard size types
typedef unsigned long      size_t;
#define NULL ((void*)0)
#else
#include <stdint.h>
#include <stddef.h>
#endif

typedef struct {
    uint8_t* buffer;
    size_t   size;
    size_t   offset;
} Arena;

// Initializes an arena with a given memory buffer and size
void arena_init(Arena* arena, void* buffer, size_t size);

// Allocates memory from the arena. Returns NULL if out of memory
void* arena_alloc(Arena* arena, size_t size);

// Resets the arena offset to 0, freeing all allocated memory instantly
void arena_reset(Arena* arena);

// Aligns size to standard machine word (8 bytes for wasm/64-bit architectures usually)
size_t arena_align_forward(size_t size, size_t alignment);

#endif // YOWZA_ARENA_H
