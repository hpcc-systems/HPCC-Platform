# Thor Storage and Spill I/O

This document covers Thor's shared spill-file path: how temp files are created, how spilled rows are written and read back, how overflow files are merged or concatenated, and which parts of the behavior are currently visible in runtime statistics.

## 1. What This Slice Covers
This is the storage-facing layer above row buffering and below activity-specific logic. The key concerns are:

- when committed rows become spill files
- how spill files are named and owned
- when downstream readers keep spill files alive
- how multiple overflow files are consumed at the end
- which spill costs are visible in statistics and which are not

For the shared buffering contracts themselves, see [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md).

## 2. Temp-File Creation and Ownership
Thor spill files are created under the job temp directory managed by `CTempNameHandler` in [../../thorlcr/thorutil/thormisc.cpp#L625](../../thorlcr/thorutil/thormisc.cpp#L625). The shared spill path uses [../../thorlcr/thorutil/thormisc.cpp#L738](../../thorlcr/thorutil/thormisc.cpp#L738) `GetTempFilePath()` to allocate a unique file name in that directory.

At the activity layer, [../../thorlcr/graph/thgraph.hpp#L1212](../../thorlcr/graph/thgraph.hpp#L1212) `CActivityBase::createOwnedTempFile()` wraps the physical file in a `CFileOwner`.

`CFileOwner` in [../../thorlcr/thorutil/thormisc.hpp#L390](../../thorlcr/thorutil/thormisc.hpp#L390):

- owns the underlying `IFile`
- updates temp-disk accounting through `noteSize()`
- removes the physical temp file in its destructor at [../../thorlcr/thorutil/thormisc.hpp#L400](../../thorlcr/thorutil/thormisc.hpp#L400)

This means spill-file deletion is ownership-driven, not explicitly tied to the collector call that created the file.

## 3. Writer Path: Committed Rows to Spill File
The core write path is [../../thorlcr/thorutil/thmem.cpp#L1394](../../thorlcr/thorutil/thmem.cpp#L1394), `CThorSpillableRowArray::save()`.

Important details:

- only **committed rows** are serialized
- write-position callbacks can force writer flushes at exact row boundaries
- null-row handling depends on `EmptyRowSemantics`
- `StNumSpills` is incremented as part of save bookkeeping at [../../thorlcr/thorutil/thmem.cpp#L1474](../../thorlcr/thorutil/thmem.cpp#L1474)
- the temp owner is updated with the on-disk spill file size via `CFileOwner::noteSize()`

The spill grain is intentionally not one row at a time. `commitDelta` in [../../thorlcr/thorutil/thmem.hpp#L412](../../thorlcr/thorutil/thmem.hpp#L412) keeps a small writer-owned tail temporarily outside the spillable/readable committed region.

## 4. Overflow Files and Final Handoff
`CThorRowCollectorBase::spillRows()` in [../../thorlcr/thorutil/thmem.cpp#L1651](../../thorlcr/thorutil/thmem.cpp#L1651) creates one overflow temp file per spilled batch. If sorting is enabled, it sorts the committed rows before writing them out.

The final policy boundary is [../../thorlcr/thorutil/thmem.cpp#L1735](../../thorlcr/thorutil/thmem.cpp#L1735), `CThorRowCollectorBase::getStream()`.

That method decides whether output becomes:

- all-memory
- all-disk
- mixed memory/disk
- merged sorted streams
- concatenated unsorted streams

When overflow files are turned back into streams, each one is wrapped in `CStreamFileOwner` at [../../thorlcr/thorutil/thmem.cpp#L1817](../../thorlcr/thorutil/thmem.cpp#L1817). That wrapper keeps the file owner alive until downstream reading finishes.

If multiple spill files exist:

- sorted paths use `createRowStreamMerger()` at [../../thorlcr/thorutil/thmem.cpp#L1839](../../thorlcr/thorutil/thmem.cpp#L1839)
- unsorted paths use concatenation instead

This is why many small spill files can become expensive even after writing is complete: merge fan-in and reopen cost grow with spill-file count.

## 5. Single-Reader and Shared-Reader Readback
Single-reader readback happens through `CSpillableStream` at [../../thorlcr/thorutil/thmem.cpp#L416](../../thorlcr/thorutil/thmem.cpp#L416). It starts with a small in-memory buffer and later switches to a spill-backed `createRowStream()` when needed.

Shared-reader readback happens through `CSharedSpillableRowSet` at [../../thorlcr/thorutil/thmem.cpp#L303](../../thorlcr/thorutil/thmem.cpp#L303). When a spill occurs, readers reopen the spill file at a saved offset using `createRowStreamEx()` at [../../thorlcr/thorutil/thmem.cpp#L348](../../thorlcr/thorutil/thmem.cpp#L348).

This shared-reader case is more expensive on the write side because `save()` must flush at callback boundaries and publish exact file positions for reader continuity.

## 6. Spill Policy Modes
The shared collector policy is controlled by `RowCollectorSpillFlags` in [../../thorlcr/thorutil/thmem.hpp#L538](../../thorlcr/thorutil/thmem.hpp#L538):

- `rc_allMem`
- `rc_mixed`
- `rc_allDisk`
- `rc_allDiskOrAllMem`

The important practical consequence is that an earlier overflow can force later tails to disk even if some rows still remain in memory at handoff time.

This is especially visible in `rc_allDiskOrAllMem`, where the final output flips to disk-backed behavior once overflow has already occurred.

## 7. What The Stats Show Well
Thor does track several useful spill-write and temp-disk signals.

Shared mappings in [../../thorlcr/thorutil/thormisc.cpp#L81](../../thorlcr/thorutil/thormisc.cpp#L81) and [../../thorlcr/thorutil/thormisc.cpp#L105](../../thorlcr/thorutil/thormisc.cpp#L105) cover:

- spill count
- spill-file size
- spill elapsed time derived from disk-write timing
- peak temp disk usage

At the activity base, [../../thorlcr/graph/thgraph.hpp#L1202](../../thorlcr/graph/thgraph.hpp#L1202) and [../../thorlcr/graph/thgraph.hpp#L1206](../../thorlcr/graph/thgraph.hpp#L1206) expose both active and peak temp size through the temp-file tracker.

## 8. Observability Gaps
The current observability surface is still skewed toward spill writes.

Current gaps include:

- no first-class shared statistic for spill-read bytes or spill-read elapsed time
- no direct metric for merge fan-in or number of simultaneously opened spill files
- no direct counter distinguishing planned spill from forced append-failure spill in [../../thorlcr/thorutil/thmem.cpp#L1697](../../thorlcr/thorutil/thmem.cpp#L1697)
- no first-class runtime statistic for current active temp usage, even though the tracker exists
- `overflowCount`, `overflowScale()`, and `hasSpilt()` are useful API-level signals but are not promoted into the common stat mappings

This means a spill-heavy workload is often easier to detect than to explain precisely.

## 9. Reusable Storage-Level Themes
- spill cost depends on **spill-file count** as much as total spill bytes
- temp-file lifetime follows the **last live stream reference**, not the collector reset point
- shared-reader spill is a write-side amplification case because exact reader offsets must be published
- compressed sorted spill files can increase merge-time memory pressure when many files are open together
- active temp usage can remain elevated after the producer has logically moved on, because downstream streams still own the files

## 10. Why This Slice Matters
If a Thor workload spills, the performance question quickly becomes storage-shaped:

- how many spill files were created
- how long they stayed live
- whether the final path merged or concatenated them
- whether the system spent more time writing spill files or reading them back

Those questions are adjacent to, but distinct from, the buffering contracts captured in [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md). This document exists to keep that storage-facing reasoning isolated and reusable.