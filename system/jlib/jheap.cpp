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


#include "platform.h"

#include "jlib.hpp"
#include "jheap.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

#include "jheap.ipp"

static Allocator32 * alloc32;
static Allocator32 * alloc24;
static Allocator32 * alloc16;
static Allocator32 * alloc12;
static Allocator32 * alloc8;
static Allocator32 * alloc4;
static Allocator32 * alloc0;
static Allocator32 * subAllocator[CHUNK_MAX+1];
static ChunkAllocator32 * theAllocator;

MODULE_INIT(INIT_PRIORITY_JHEAP)
{
  theAllocator = new ChunkAllocator32;
  alloc32 = new Allocator32(32, theAllocator);
  alloc24 = new Allocator32(24, theAllocator);
  alloc16 = new Allocator32(16, theAllocator);
  alloc8 = new Allocator32(8, theAllocator);
  alloc4 = new Allocator32(4, theAllocator);
  alloc0 = new Allocator32(0, theAllocator);

  unsigned idx = CHUNK_MAX;
  while (idx > 24) subAllocator[idx--] = alloc32;
  while (idx > 16) subAllocator[idx--] = alloc24;
  while (idx > 8) subAllocator[idx--] = alloc16;
  while (idx > 4) subAllocator[idx--] = alloc8;
  while (idx > 0) subAllocator[idx--] = alloc4;
  subAllocator[0] = alloc0;
  return true;
}
MODULE_EXIT()
{
  delete alloc32;
  delete alloc24;
  delete alloc16;
  delete alloc8;
  delete alloc4;
  delete alloc0;
  delete theAllocator;
}


CriticalSection crit;

//---------------------------------------------------------------------------

char * ChunkAllocator32::lowerBound;
char * ChunkAllocator32::upperBound;
char * ChunkAllocator32::curAlloc;

ChunkAllocator32::ChunkAllocator32(void)
{
  lowerBound = (char *)VirtualAlloc(PREFERRED_HEAP_ADDRESS, MAXCOMMIT, MEM_RESERVE, PAGE_READWRITE);
  if (!lowerBound)
    lowerBound = (char *)VirtualAlloc(NULL, MAXCOMMIT, MEM_RESERVE, PAGE_READWRITE);
  if (!lowerBound)
  {
    assertThrow(lowerBound);
    upperBound = NULL;
    curAlloc = NULL;
  }
  else
  {
    upperBound = lowerBound + MAXCOMMIT;
    curAlloc = lowerBound;
  }
}

ChunkAllocator32::~ChunkAllocator32(void)
{
  VirtualFree(lowerBound, upperBound-lowerBound, MEM_RELEASE);
}


char * ChunkAllocator32::allocatePage(void)
{
  if (curAlloc >= upperBound)
    return NULL;
  char * ret = curAlloc;
  VirtualAlloc(ret, PAGESIZE, MEM_COMMIT, PAGE_READWRITE);
  curAlloc += PAGESIZE;
  return ret;
}

//---------------------------------------------------------------------------

Allocator32::Allocator32(size32_t _size, ChunkAllocator32 * _allocator)
{
  size = _size;
  allocator = _allocator;
}

char * Allocator32::allocate(void)
{
  if (next)
  {
    char * ret = next;
    next = *(char * *)ret;
    return ret;
  }
  if (more())
    return allocate();
  return NULL;
}

void Allocator32::deallocate(void * ptr)
{
  *(char * *)ptr = next;
  next = (char *)ptr;
}


bool Allocator32::more(void)
{
  if (size == 0)
    return false;
  char * cur = ChunkAllocator32::allocatePage();
  if (!cur)
    return false;

  //head of each page has a pointer to the allocator
  *(Allocator32 * *)cur = this;

  //fill the rest of the block with a linked data structure
  char * prev = NULL;
  unsigned todo = PAGESIZE - size;
  while (todo >= size)
  {
    cur += size;
    *(char * *)cur = prev;
    prev = cur;
    todo -= size;
  }
  next = cur;
  return true;
}



//---------------------------------------------------------------------------

void * chunkedNew(size32_t size)
{
  if (size <= CHUNK_MAX)
  {
    CriticalBlock block(crit);
    void * ret = subAllocator[size]->allocate();
    if (ret)
      return ret;
    if (size == 0)
      return NULL;
  }

  return malloc(size);
}

void chunkedFree(void * ptr)
{
  if (ptr)
  {
    if (ISCHUNKED(ptr))
    {
      CriticalBlock block(crit);
      FREECHUNKED(ptr);
      return;
    }

    free(ptr);
  }
}

