# RFC 0002: Transition from LCRS to True ART

## Motivation
The initial Left-Child Right-Sibling (LCRS) tree implementation saved memory but exhibited O(N) lookup times per node level, causing severe performance degradation for datasets with many siblings sharing the same prefix.

## Proposal
Implement a full Adaptive Radix Tree (ART) compliant with the original paper by Leis et al.

1. **Dynamic Node Types:** Introduce `Node4`, `Node16`, `Node48`, and `Node256`.
2. **Expansion:** Implement logic to seamlessly allocate and upgrade nodes when they reach capacity.
3. **Memory Arena Integration:** Ensure old, smaller nodes are correctly returned to the Arena's free-list to prevent memory leaks.

## Benefits
- O(k) lookups, strictly bounded by the key length.
- Minimal cache misses due to dense array packing in Node4 and Node16.
- Resolves the performance cliff seen in LCRS.
