# Thor Memory and Spilling

Thor's memory architecture is a defining part of its execution model. Unlike Roxie, Thor is expected to keep processing by spilling when memory pressure rises.

## 1. Dynamic Spilling Model
[../../devdoc/MemoryManager.md](../../devdoc/MemoryManager.md) documents Thor's dynamic spilling rules:

- buffered-row owners register callbacks with the memory manager
- callbacks are prioritized so cheaper spill actions run first
- callbacks may be told to free as much memory as possible when pressure is critical

This makes spill callbacks part of the engine's correctness contract.

## 2. Callback Safety Rules
The same documentation captures the main safety rules:

- a spill callback must not allocate memory from the row manager
- code must not allocate while holding a lock that a callback may need
- large resizes need special handling because they may trigger callbacks and move memory

When touching collectors, row arrays, or spill streams, reason about deadlock first and optimization second.

## 3. Global Memory Across Channels
Thor can run multiple channels within the same process. The global row-manager support described in [../../devdoc/MemoryManager.md](../../devdoc/MemoryManager.md) exists so shared data, such as lookup-join RHS state, can be accounted for correctly across channels.

Any optimization that caches or shares data must preserve that accounting model.

## 4. Shared Buffering and Spill Infrastructure
The common buffering/spilling machinery in `thorlcr/thorutil/thmem.cpp` deserves its own analysis surface because many Thor activities build on it indirectly.

- [../../thorlcr/thorutil/thmem.cpp#L156](../../thorlcr/thorutil/thmem.cpp#L156) defines `CSpillable`, which separates spill callback registration from activation.
- [../../thorlcr/thorutil/thmem.cpp#L1394](../../thorlcr/thorutil/thmem.cpp#L1394) shows `CThorSpillableRowArray::save()`, where only committed rows are serialized and write-position callbacks are notified.
- [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1735) shows `CThorRowCollectorBase::getStream()`, the central handoff path from buffered rows into all-memory or spill-backed output.

See [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md) for the dedicated walkthrough of these common classes.

## 5. Current Buffering and Spill Surfaces
Several concrete runtime surfaces are already worth tracking:

- In [../../thorlcr/thorutil/thmem.cpp#L425](../../thorlcr/thorutil/thmem.cpp#L425), `CSpillableStream` activates a spilling callback during construction and allocates a 500-pointer `readRows` buffer.
- In [../../thorlcr/thorutil/thmem.cpp#L1999](../../thorlcr/thorutil/thmem.cpp#L1999), `CThorRowLoader` is the main collector/loader surface for buffering rows into a spill-aware structure before returning a stream.
- In [../../thorlcr/activities/topn/thtopnslave.cpp#L112](../../thorlcr/activities/topn/thtopnslave.cpp#L112), `TopNSlaveActivity` uses `sortedRows.clearRows()` so child-query reuse can retain high-water row-pointer capacity rather than reallocating from scratch.

These are good starting points for grouped-sort, tiny-group, and child-query investigations.

## 6. Initial Heuristics
- prefer reuse of existing row-pointer capacity when groups repeat many times
- treat spill callback registration and spill-stream setup as measurable overhead, especially for tiny groups
- validate any memory optimization against callback safety, large-block resize behavior, and multi-channel accounting