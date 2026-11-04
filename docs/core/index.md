# Adaptive Radix Tree (ART)

## Overview
Yowza! utilizes a state-of-the-art Adaptive Radix Tree (ART) for its primary key-value index. Traditional B-Trees and Hash Maps fall short in environments where memory overhead must be minimized, and prefix searches are common. ART provides the perfect balance: O(k) search complexity (where k is the key length), minimal cache misses, and cache-aligned node sizes.

## Evolution: From LCRS to True ART
Initially, Yowza! implemented the index using a Left-Child Right-Sibling (LCRS) tree structure to save memory. However, as the system evolved to handle high-throughput edge workloads, the O(N) sibling traversal became a bottleneck for nodes with many children.

We transitioned to a True ART implementation, characterized by dynamically sized nodes:
- **Node4:** Holds up to 4 children using parallel arrays for keys and pointers.
- **Node16:** Holds up to 16 children. Utilizes SIMD (if available) for ultra-fast key matching.
- **Node48:** Holds up to 48 children. Uses a 256-byte indirection array to map key bytes to an array of 48 pointers, saving significant space compared to a full array.
- **Node256:** The ultimate node type, directly mapping a byte to an array of 256 pointers for instant O(1) traversal at that level.

## Node Expansion and Compaction
The transition between node types is handled automatically by the `art.c` module. When a `Node4` reaches capacity, it is dynamically allocated as a `Node16`, the contents are copied over, and the old node is freed via our Arena allocator's free-list.

During a `yowza_db_compact` operation, deleted nodes are fully reclaimed, and sparsely populated nodes can be shrunk back to smaller capacities, keeping the memory footprint incredibly tight.
