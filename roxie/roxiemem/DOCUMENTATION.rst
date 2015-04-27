========================
The Roxie Memory Manager
========================

************
Introduction
************

This memory manager started life as the memory manager which was only used for the Roxie engine.  It had several
original design goals:

* Support link counted rows.  (When the last reference is released the row is freed.)
* Be as fast as possible on allocate and deallocate of small rows.
* Allow rows serialized from slaves to be used directly without being cloned first.
* Allow the memory used by a single query, or by all queries combined, to be limited, with graceful recovery.
* Isolate roxie queries from one another, so that one query can't bring
  down all the rest by allocating too much memory.
* Guarantee all the memory used by a query is freed when the query finishes, reducing the possibility of memory leaks.
* Predictable behaviour with no pathogenic cases.

(Note that efficient usage of memory does not appear on that list - the expectation when the memory
manager was first designed was that Roxie queries would use minimal amounts of memory and speed was
more important.  Some subsequent changes e.g., Packed heaps, and configurable bucket sizes help mitigate that.)

**************
Main Structure
**************

The basic design is to reserve (but not commit) a single large block of memory in the virtual address space.  This
memory is subdivided into "pages".  (These are not the same as the os virtual memory pages.  The memory manager pages
are currently defined as 1Mb in size.)

The page bitmap
===============
The system uses a bitmap to indicate whether each page from the global memory has been allocated. All active
IRowManager instances allocate pages from the same global memory space.
To help reduce fragmentation allocations for single pages are fulfilled from one end of the address space, while
allocations for multiple pages are fulfilled from the other.

IRowManager
===========
This provides the primary interface for allocating memory.  The size of a requested allocation is rounded up to the
next "bucket" size, and the allocation is then satisfied by the heap associated with that bucket size.  Different
engines can specify different bucket sizes - an optional list is provided to setTotalMemoryLimit.  Roxie tends to use
fewer buckets to help reduce the number of active heaps.  Thor uses larger numbers since it is more important to
minimize the memory wasted.

Roxie uses a separate instance of IRowManager for each query.  This provides the mechanism for limiting how much
memory a query uses.  Thor uses a single instance of an IRowManager for each slave/master.

Heaps
=====
Memory is allocated from a set of "heaps - where each heap allocates blocks of memory of a single size.  The heap
exclusively owns a set of heaplet (each 1 page in size), which are held in a doubly linked list, and sub allocates
memory from those heaplets.

Information about each heaplet is stored in the base of the page (using a class with virtual functions) and the
address of an allocated row is masked to determine which heap object it belongs to, and how it should be linked/released
etc.  Any pointer not in the allocated virtual address (e.g., constant data) can be linked/released with no effect.

Each heaplet contains a high water mark of the address within the page that has already been allocated (freeBase),
and a lockless singly-linked list of rows which have been released (r_block).  Releasing a row is non-blocking and
does not involve any spin locks or critical sections.  However, this means that empty pages need to be returned to
the global memory pool at another time.  (This is done in releaseEmptyPages()).

When the last row in a page is released a flag (possibleEmptyPages) is set in its associated heap.
* This is checked before trying to free pages from a particular heap, avoiding waiting on a lock and traversing
  a candidate list.

Any page which *might* contain some spare memory is added to a lockless spare memory linked list.
* Items are popped from this list when a heap fails to allocate memory from the current heaplet.  Each item is checked
  in turn if it has space before allocating a new heaplet.
* The list is also walked when checking to see which pages can be returned to the global memory.  The doubly linked
  heaplet list allows efficient freeing.

Each allocation has a link count and an allocator id associated with it.  The allocator id represents the type of
the row, and is used to determine what destructor needs to be called when the row is destroyed.  (The count for a
row also contains a flag in the top bit to indicate if it is fully constructed, and therefore valid for the
destructor to be called.)

Huge Heap
=========
A specialized heap is used to manage all allocations that require more than one page of memory.  These allocations
are not held on a free list when they are released, but each is returned directly to the global memory pool.
Allocations in the huge heap can be expanded and shrunk using the resizeRow() functions - see below.

Specialised Heaps:
==================
For fixed size allocations it is possible to get a more efficient interface for allocating rows.  There are options
to create unique fixed size heaps (to reduce thread contention) and packed heaps - where all rows share the same
allocator id.

(Note to self: Is there ever any advantage having a heap that is unique but not packed??)

****************
Dynamic Spilling
****************

Thor has additional requirements to roxie.  In roxie, if a query exceeds its memory requirements then it is terminated.
Thor needs to be able to spill rows and other memory to disk and continue.  This is achieved by allowing any process
that stores buffered rows to register a callback with the memory manager.  When more memory is required these callbacks
are called to free up memory, and allow the job to continue.

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

Callback Rules
==============
Some rules to follow when implementing callbacks:

* A callback cannot allocate any memory from the memory manager.  If it does it is likely to deadlock.

* You cannot allocate memory while holding a lock if that lock is also required by a callback.

  Again this will cause deadlock.  If it proves impossible you can use a try-lock primitive in the callback,
  but it means you won't be able to spill those rows.

* If the heaps are fragmented it may be more efficient to repack the heaps than spill to disk.

* If you're resizing a potentially big block of memory use the resize function with the callback.

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
  which is used to atomically update the pointer so it always remains thread safe.


Compacting heaps
================
Occasionally you have processes which read a large number of rows and then filter them so only a few are still
held in memory.  Rows tend to be allocated in sequence through the heap pages, which can mean those few remaining
rows are scattered over many pages.  If they could all be moved to a single page it would free up a significant
amount of memory.

The memory manager contains a function to pack a set of rows into a smaller number of pages: IRowManager->compactRows().

This works by iterating through each of the rows in a list.  If the row belongs to a heap that could be compacted,
and isn't part of a full heaplet, then the row is moved.  Since subsequent rows tend to be allocated from the same
heaplet this has the effect of compacting the rows.

*************
Shared Memory
*************

Much of the time Thor doesn't uses full memory available to it.  If you are running multiple Thor processes
on the same machine you may want to configure the system so that each Thor has a private block of memory,
but there is also a shared block of memory which can be used by whichever process needs it.

The ILargeMemCallback provides a mechanism to dynamically allocate more memory to a process as it requires it.
This could potentially be done in stages rather than all or nothing.

(Currently unused as far as I know...)

**********
Huge pages
**********

When OS processes use a large amount of memory, mapping virtual addresses to physical addresses can begin to
take a significant proportion of the execution time.  This is especially true once the TLB is not large enough to
store all the mappings.  Huge pages can significantly help with this problem by reducing the number of TLB entries
needed to cover the virtual address space.  The memory manager supports huge pages in two different ways:

Huge pages can be preallocated (e.g., with hugeadm) for exclusive use as huge pages.  If huge pages are enabled
for a particular engine, and sufficient huge pages are available to supply the memory for the memory manager, then
they will be used.

Linux kernels from 2.6.38 onward have support for transparent huge pages.  These do not need to be preallocated,
instead the operating system tries to use them behind the scenes.  HPCC version 5.2 and following takes advantage
of this feature to significantly speed memory access up when large amounts of memory are used by each process.

Preallocated huge pages tend to be more efficient, but they have the disadvantage that the operating system currently
does not reuse unused huge pages for other purposes e.g., disk cache.

There is also a memory manager option to not return the memory to the operating system when it is no longer
required.  This has the advantage of not clearing the memory whenever it is required again, but the same disadvantage
as preallocated huge pages that the unused memory cannot be used for disk cache.  We recommend this option is
selected when preallocated huge pages are in use - until the kernel allows them to be reused.
