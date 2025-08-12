/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#ifndef JHCACHE_HPP
#define JHCACHE_HPP

#include "jiface.hpp"
#include "jhutil.hpp"
#include "hlzw.h"
#include "jcrc.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "ctfile.hpp"


// This class is used to represent a block of data requested from a particular index
// MORE: This could be extended to allow multiple blocks to be written if that helps
// traversing branches and leaves.
// Should this also have a file id as a sanity check that it is not shared between different files?
class CCachedIndexRead
{
public:
    void adjustSize(size32_t newSize) { dbgassertex(newSize <= size); size = newSize; }

    // request a buffer for data at a given offset and size - used when reading data from a file or retrieving from the cache
    byte * getBufferForUpdate(offset_t offset, size32_t writeSize);

    // Request data for a particular offset and size.  return null if no match, otherwise return a pointer
    // to the daa
    const byte * queryBuffer(offset_t offset, size32_t readSize) const;

protected:
    offset_t baseOffset = 0;
    size32_t size = 0;
    MemoryAttr data;
};


class CCacheReservation
{
// The interpretation of this data is cache implementation dependent
// Allocate with void * members to ensure it is 8 byte aligned.
    void * reserved[4];
};

//This is the interface that is used to interface with the page cache
//
// NOTE: The size of the pages in the cache is unlikely to match the size of the nodes in an index.  That means
//       the offsets passed to the cache functions will need to be aligned to the page boundary, and may not
//       match the offset of the node in the file.
//       This also means there is no benefit in passing in a hash code - because it will not match the hash calculated
//       by the node cache, unless the hash algorithm is refactored.  (May be worth exploring).
//       Any hash code calculated by the cache can be saved in the reservation to avoid recalculating it on write().
//
// It is relatively unlikely, but there may be concurrent nodeUsed(), readOrReserve() and write calls for the same offset
// (e.g. 3 different nodes may be being accessed within the same "page").  Ideally a second readOrRequest() for the same
// page should block until the first has succeeded (or the reservation is released).
//
// Size is not provided on any of the calls, because the disk page cache only caches items of a fixed size.
// If a read from a file is not a multiple of the page size it is the caller's responsibility to zero fill if necessary
// before inserting into the cache.

interface IPageCache : public IInterface
{
    virtual size32_t queryPageSize() const = 0;

    // Called when the node has been hit in the memory cache - ensure LRU is updated
    // The offset will need to be aligned to the page boundary
    virtual void noteUsed(unsigned fileId, offset_t offset) = 0;

    // Searching for a node in the disk node cache.  Return true if found, and ensure nodeData is populated
    // if it returns false then the node is not in the cache.
    virtual bool read(unsigned fileId, offset_t offset, size32_t size, CCachedIndexRead & nodeData) = 0;

    // Insert a block of data of size queryPageSize() into the disk node cache.  It may already exist.
    virtual void write(unsigned fileId, offset_t offset, const byte * data) = 0;

    // Called to gather the current state of the cache - so it can be preserved for quick cache warming.
    virtual void gatherState(ICacheInfoRecorder & recorder) = 0;
};

//Not called from outside jhtree, so no jhtree_decl
extern IPageCache * createDemoPageCache(const IPropertyTree * config);

#endif
