/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
