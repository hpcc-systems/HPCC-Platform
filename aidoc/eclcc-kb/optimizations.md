# Optimizations Backlog

> Last verified: 2026-05

This file tracks identified optimization opportunities for eclcc. Each entry includes a description, expected impact, and actionable next steps.

---

## Pending Investigation

### 1. Normalized Selector Redesign
**Description**: Currently `queryNormalizedSelector()` returns the table expression node. When a transform modifies a table, all selectors derived from it change, forcing deep recursion even when field references are unaffected. Using a unique-ID-based selector (similar to `selSeq`) could dramatically reduce transform work.

**Expected Impact**: Significant reduction in transformation time for large graphs with many table operations.

**Status**: [UNVERIFIED] — mentioned in `devdoc/CodeGenerator.md` as a planned improvement. Needs investigation of scope implications and whether optimization passes rely on selector identity.

**Next Steps**:
1. Audit all callers of `queryNormalizedSelector()` to understand dependencies.
2. Prototype unique-ID selectors and measure transform-pass runtimes.
3. Verify regression suite passes.

---

### 2. Compilation Time Profiling
**Description**: For very large queries, compilation time itself can be a bottleneck. Profiling which passes dominate compilation time would identify the highest-value optimization targets.

**Expected Impact**: Faster turnaround for large production queries; enables prioritization of pass-level improvements.

**Status**: Not yet investigated.

**Next Steps**:
1. Instrument pass boundaries with timing markers.
2. Profile representative large queries from the private regression suite.
3. Rank passes by wall-clock and memory-allocation cost.

---

### 3. Memory Consumption Reduction
**Description**: With 10M–100M nodes, per-node overhead dominates memory. Opportunities include:
- Reducing flags/cached-info storage through bit-packing
- Lazy computation of less-frequently-accessed derived properties
- Arena allocation for expression nodes (batch deallocation)

**Expected Impact**: Reduce peak memory for large queries, enabling compilation of even larger jobs without OOM.

**Status**: Needs profiling to identify largest contributors.

**Next Steps**:
1. Profile memory allocation patterns for large queries.
2. Measure sizeof() for each expression class variant.
3. Identify fields that could be lazily computed or bit-packed.

---

### 4. Parallel Transformation Passes
**Description**: Currently all transformation passes run sequentially on a single thread. For independent subgraphs, parallelization may be possible.

**Expected Impact**: Reduced compilation wall-clock time on multi-core machines.

**Status**: [UNVERIFIED] — Needs careful analysis of which passes have interdependencies.

**Next Steps**:
1. Identify passes that are purely local (no cross-graph dependencies).
2. Evaluate whether node commoning (global hash table) would be a contention bottleneck.
3. Prototype parallel fold or optimize for independent workflow items.

---

### 5. Incremental Compilation
**Description**: When a small change is made to a large ECL module, recompiling the entire query from scratch is wasteful. Caching parsed/normalized subgraphs across compilations could dramatically improve interactive development.

**Expected Impact**: Faster edit-compile-test cycles for large projects.

**Status**: The `hqlcache.cpp` file exists but scope of current caching is unclear.

**Next Steps**:
1. Understand current caching mechanism in `hqlcache.cpp`.
2. Identify which pipeline stages produce deterministic output from unchanged input.
3. Design cache invalidation strategy based on module dependency graph.

---

## Completed
(None yet)
