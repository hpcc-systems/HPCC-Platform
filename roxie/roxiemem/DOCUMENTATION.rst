========================
The Roxie Memory Manager
========================

************
Introduction
************

This memory manager started life as the memory manager which was only used for the Roxie engine.  It had several
original design goals:

* Support link counted rows.
* Provide efficient allocation of rows.
* Allow rows serialized from slaves to be used directly without being cloned first.
* Restrict the amount of memory a given roxie query can allocate.
* Isolate roxie queries from one another, so that one query can't bring
  down all the rest by allocating too much memory.

The basic design is to reserve (but not commit) a single large block of memory in the virtual address space.  This
memory is subdivided into "pages".  (These are not the same as the os virtual memory pages.  The memory manager pages
are currently defined as 1Mb in size.)

Memory is allocated from a set of "heaps.  Each heap owns a set of pages, and sub allocates memory of a
single size from those pages.  All allocations from a particular page belong to the same heap.  Rounding the requested
memory size up to the next heap-size means that memory
is not lost due to fragmentation.

Information about each heap is stored in the base of the page (using a class with virtual functions) and the
address of an allocated row is masked to determine which heap object it belongs to, and how it should be linked/released
etc.  Any pointer not in the allocated virtual address (e.g., constant data) can be linked/released with no effect.

Each allocation has a link count and an allocator id associated with it.  The allocator id represents the type of
the row, and is used to determine what destructor needs to be called when the row is destroyed.  (The row also
contains a flag to indicate if it is fully constructed so it is valid for the destructor to be called.)

An implementation of IRowManager processes all allocations for a particular roxie query.  This provides the
mechanism for limiting how much memory a query uses.

For fixed size allocations it is possible to get a more efficient interface for allocating rows.  There are options
to create unique fixed size heaps (to reduce thread contention) and packed heaps - where all rows share the same
allocator id.

(Note to self: Is there ever any advantage having a heap that is unique but not packed??)

****************
Dynamic Spilling
****************

Thor has different requirements to roxie.  In roxie, if a query exceeds its memory requirements then it is terminated.  Thor
needs to be able to spill rows and other memory to disk and continue.  This is achieved by allowing any process that
stores buffered rows to register a callback with the memory manager.  When more memory is required these are called
to free up memory, and allow the job to continues.

Each callback can specify a priority - lower priority callbacks are called first since they are assumed to have a
lower cost associated with spilling.  When more memory is required the callbacks are called in priority order until
one of them succeeds.  The can also be passed a flag to indicate it is critical to force them to free up as much memory
as possible.


Complications
=============

There are several different complications involved with the memory spilling:

* There will be many different threads allocating rows.
* Callbacks could be triggered at any time.
* There is a large scope for deadlock between the callbacks and allocations.
* It may be better to not resize a large array if rows had to be evicted to resize it.
* Filtered record streams can cause significant wasted space in the memory blocks.
* Resizing a multi-page allocation is non trivial.


Resizing Large memory blocks
============================
Some of the memory allocations cover more than one "page" - e.g., arrays used to store blocks of rows.  (These
are called huge pages internally, not to be confused with operating system support for huge pages...)  When
one of these memory blocks needs to be expanded you need to be careful:

* Allocating a new page, copying, updating the pointer (within a cs) and then freeing is safe.  Unfortunately
  it may involve copying a large chunk of memory.  It may also fail if there isn't memory for the new and old
  block, even if the existing block could have been expanded into an adjacent block.

* You can't lock, call a resize routine and update the pointer because the resize routine may need to allocate
  a new memory block- that may trigger a callback, which could in turn deadlock trying to gain the lock.
  (The callback may be from another thread...)

* Therefore the memory manager contains a call which allows you to resize a block, but with a callback
  which is used to atomically update the pointer so it always remains thead safe.


Repacking rows
==============
Occasionally you have processes which read a large number of rows and then filter them so only a few are still
held in memory.  Rows tend to be allocated in sequence through the heap pages, which can mean those few remaining
rows are scattered over many pages.  If they could all be moved to a single page it would free up a significant
amount of memory.

The memory manager contains a function to pack a set of rows into a smaller number of pages.

*MORE: Document how we fix this!*

Rules
=====
Some rules to follow when implementing callbacks:

* A callback cannot allocate any memory from the memory manager.  If it does it is likely to deadlock.

* You cannot allocate memory while holding a lock if that lock is also required by a callback.

  Again this will cause deadlock.  If it proves impossible you can use a try-lock primitive in the callback,
  but it means you won't be able to spill those rows.

* If the heaps are fragmented it may be more efficient to repack the heaps than spill to disk.

* If you're resizing a potentially big block of memory use the resize function with the callback.

*************
Shared Memory
*************

Much of the time Thor doesn't uses full memory available to it.  If you are running multiple Thor processes
on the same machine you may want to configure the system so that each Thor has a private block of memory,
but there is also a shared block of memory which can be used by whichever process needs it.

The ILargeMemCallback provides a mechanism to dynamically allocate more memory to a process as it requires it.
This could potentially be done in stages rather than all or nothing.

(Currently unused as far as I know...)
