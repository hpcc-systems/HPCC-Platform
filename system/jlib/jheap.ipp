/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */


#ifndef JHEAP_IPP
#define JHEAP_IPP

typedef unsigned long ulong;
#define PAGESIZE      0x1000U      //??Initialise from GetSystemInfo
#define PREFERRED_HEAP_ADDRESS  (void *)0x08000000
#define MAXCOMMIT     0x8000000L    // about 100M
#define CHUNK_MAX     32

#define ISCHUNKED(ptr)       (((ulong)ptr - (ulong)ChunkAllocator32::lowerBound) < MAXCOMMIT)
#define PTR2ALLOC(ptr)       (*(Allocator32 * *)((ulong)(ptr) & ~(PAGESIZE-1)))
#define FREECHUNKED(ptr)     (PTR2ALLOC(ptr)->deallocate(ptr))
#define MALLOCCHUNKED(size)  (ChunkAllocator32::allocate(size))
#define SIZEofCHUNKED(ptr)   (PTR2ALLOC(ptr)->getSize())

class Allocator32;
class ChunkAllocator32
{
public:
    ChunkAllocator32(void);
    ~ChunkAllocator32(void);

    static char * allocatePage(void);

public:
    static char * lowerBound;
    static char * upperBound;
    static char * curAlloc;
};

class Allocator32
{
public:
    Allocator32(size32_t _size, ChunkAllocator32 * _allocator);

    void            deallocate(void * ptr);
    char *          allocate();
    size32_t          getSize(void)   { return size; }

protected:
    bool            more(void);

private:
    char *          next;
    size32_t          size;
    ChunkAllocator32 * allocator;
};

#endif
