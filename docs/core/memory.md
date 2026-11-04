# Memory Management: The Arena Allocator

## The Serverless Constraint
In WebAssembly and serverless environments, memory management is critical. Frequent `malloc` and `free` calls lead to fragmentation, system call overhead, and unpredictable latency—factors that are unacceptable for an edge database.

## The Arena Strategy
Yowza! bypasses standard `libc` allocators by implementing a custom Arena Allocator (`arena.c`). An Arena is a large contiguous block of memory allocated once at startup.

### Allocation
When a new node or string needs allocation, the Arena simply advances an internal pointer (`head`) by the requested size. This makes allocation an O(1) operation consisting of a simple pointer addition.

### Recycling and The Free-List
Since databases modify data constantly, we cannot simply let the Arena fill up. We implemented a free-list mechanism. When memory is "freed" (e.g., when a value is updated or an ART node is expanded), its offset and size are added to an internal linked list of free blocks.

Future allocations first check the free-list for a block of suitable size (using a first-fit or best-fit heuristic) before advancing the main Arena pointer.

### Snapshot Compaction
The true power of the Arena shines during Snapshot creation. When `yowza_db_compact` is called, the database performs a deep copy of the live ART tree into a *new* Arena. This instantly reclaims all fragmented space, leaving a perfectly packed, contiguous memory layout. This compacted Arena can then be directly written to disk or sent over the network as a zero-copy snapshot!
