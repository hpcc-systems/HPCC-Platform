# Thor Shared Buffering and Spilling Classes

This document covers the common buffering/spilling infrastructure in [../../thorlcr/thorutil/thmem.cpp](../../thorlcr/thorutil/thmem.cpp) that many Thor activities depend on. The goal here is to capture the shared contracts before looking at specific activities or graph users.

## 1. Layering
The shared infrastructure is built in layers:

- [../../thorlcr/thorutil/thmem.cpp#L156](../../thorlcr/thorutil/thmem.cpp#L156) defines `CSpillable`, the common base that integrates with the row manager as a buffered-row callback.
- `CThorSpillableRowArray` is the spill-aware row-pointer container that separates committed rows from the writer-owned tail and provides save/flush/shrink behavior.
- [../../thorlcr/thorutil/thmem.cpp#L238](../../thorlcr/thorutil/thmem.cpp#L238) defines `CSpillableStreamBase`, a spill owner that can serialize committed rows to a temp file and then discard in-memory state.
- [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1735) shows `CThorRowCollectorBase::getStream()`, the main handoff point from collected rows into final output form.
- [../../thorlcr/thorutil/thmem.cpp#L1999](../../thorlcr/thorutil/thmem.cpp#L1999) shows `CThorRowLoader`, a thin loader facade over the shared collector path.

## 2. Spill Callback Lifecycle
`CSpillable` deliberately separates two states:

- **registered**: the object has been added to the row manager's buffered-row list
- **activated**: the callback is currently eligible to free buffered rows under memory pressure

That distinction matters because ownership can be transferred while the old owner still exists. The collector path uses that separation to remove asynchronous spillability before building final streams.

The practical rule is: callback ordering is part of the concurrency contract, not just housekeeping.

## 3. The Committed-Row Contract
`CThorSpillableRowArray` is the key shared container. The important semantic split is between:

- **committed rows**: safe for readers and spillers
- **uncommitted rows**: still owned by the writer path and not yet safe to spill/read concurrently

In [../../thorlcr/thorutil/thmem.cpp#L1394](../../thorlcr/thorutil/thmem.cpp#L1394), `CThorSpillableRowArray::save()` serializes only committed rows. It also copies and sorts write-position callbacks, flushing the writer at callback boundaries so readers can learn precise spill-file offsets.

In [../../thorlcr/thorutil/thmem.cpp#L1496](../../thorlcr/thorutil/thmem.cpp#L1496), flush/compaction logic moves unread rows down and advances the committed boundary. This is why resize, flush, and spill behavior must be reasoned about together.

## 4. Two Handoff Patterns
The shared code supports two distinct output patterns.

### Single-reader handoff
[../../thorlcr/thorutil/thmem.cpp#L425](../../thorlcr/thorutil/thmem.cpp#L425) defines `CSpillableStream`, which starts with an in-memory staging buffer and switches to a spill-backed stream when necessary. The constructor activates a spill callback and allocates a fixed 500-pointer `readRows` buffer.

This is a generic tradeoff point:

- larger staging buffers reduce lock/check frequency
- smaller staging buffers reduce retained memory and may switch to spill sooner

### Shared-reader handoff
[../../thorlcr/thorutil/thmem.cpp#L380](../../thorlcr/thorutil/thmem.cpp#L380) shows the shared-reader path, where a reader is told the spill-file position corresponding to its current logical row before the in-memory rows are discarded.

This allows multiple readers to keep a logical position in the shared row set and resume from the spill file without restarting from the beginning.

## 5. Collector Ownership Transfer
In [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1735), `CThorRowCollectorBase::getStream()` is the main ownership-transfer point.

Important behavior in this path:

- it flushes pending rows before final handoff
- it may choose all-disk, all-memory, or mixed behavior based on policy and prior overflow
- it deactivates the collector's own spill callback before constructing output streams or transferring row ownership
- when constructing a shared spillable row set, it delays callback activation until after the row-array lock is released

The deadlock-avoidance note at [../../thorlcr/thorutil/thmem.cpp#L1787](../../thorlcr/thorutil/thmem.cpp#L1787) is especially important: installing the shared callback while still holding the row-array lock can deadlock against the row manager's background thread.

## 6. Reusable Risks and Optimization Themes
- **Callback-state bugs are high risk**: confusion between registered and activated state can expose partially transferred state to asynchronous spill requests.
- **Reader fan-out has infrastructure cost**: shared-reader spill uses write-position callback bookkeeping during save, so more readers increase spill overhead.
- **Fragmented small spills are expensive**: repeated small committed batches can increase spill-file count, sort overhead, and downstream merge work.
- **Staging buffer size is a real tuning lever**: the fixed 500-row `readRows` allocation in `CSpillableStream` is a shared-class tuning point, especially for workloads with huge numbers of tiny groups.
- **Deadlock avoidance is designed into the API**: delayed callback activation and resize/flush ordering are there to preserve safety under memory pressure.

## 7. Why This Slice Matters
Many later Thor investigations will appear activity-specific at first, but the root cause often sits in this shared layer:

- row-pointer reuse versus rebuild
- spill callback lifetime
- shared-reader handoff costs
- commit/flush boundaries
- generic stream setup overhead

That makes this document the right base before analyzing activity families or graph execution paths.