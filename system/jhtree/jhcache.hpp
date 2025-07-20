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


// Used to store a cache of data read from a local (or remote) file, or from a disk node cache.
class CCachedIndexRead
{
public:
    Owned<IFileIO> bufferedIO;      // MORE: This will be replaced in the future
    size32_t blockIoSize = 0;
    offset_t baseOffset = 0;
    size32_t size = 0;
    MemoryAttr data;
};

//This is the interface that is used to interface with the page cache
//
// The caller will guarantee that there will only be a single concurrent call to readNode or writeNode for the
// same nodeId.
// There may be multiple concurrent calls to noteUsed for the same nodeId, and it is possible, but very unlikely,
// that these could occur at the same time as a readNode or writeNode call.

interface IDiskNodeCache : public IInterface
{
    // Called when the node has been hit in the memory cache - ensure LRU is updated
    virtual void noteUsed(const CKeyIdAndPos & nodeId) = 0;
    // Searching for a node in the disk node cache.  Return true if found, and ensure nodeData is populated
    virtual bool readNode(const CKeyIdAndPos & nodeId, CCachedIndexRead & nodeData) = 0;
    // Insert a block of data into the disk node cache.  Depending on the configuration it may be multiple blocks
    virtual void writeNodes(const CKeyIdAndPos & nodeId, const CCachedIndexRead & nodeData) = 0;

    // Called to save the page cache state to disk
    virtual void saveState(IFile * target) = 0;
    // Called to restore the page cache state from disk
    virtual void restoreState(IFile * source) = 0;
};

interface IPropertyTree;
void jhtree_decl initDiskNodeCache(const IPropertyTree *config);

#endif
