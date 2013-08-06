/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef _ROXIEROW_INCL
#define _ROXIEROW_INCL

#include "thorhelper.hpp"
#include "roxiemem.hpp"
#include "eclhelper.hpp"


extern THORHELPER_API IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, roxiemem::RoxieHeapFlags flags);
extern THORHELPER_API IEngineRowAllocator * createCrcRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, roxiemem::RoxieHeapFlags flags);

interface IRowAllocatorMetaActIdCache : extends roxiemem::IRowAllocatorCache
{
    virtual bool remove(IOutputMetaData *meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) = 0;
    virtual IEngineRowAllocator *lookup(IOutputMetaData *meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) const = 0;
    virtual IEngineRowAllocator *ensure(IOutputMetaData * meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) = 0;
    virtual void clear() = 0;
    virtual unsigned items() const = 0;
};

interface IRowAllocatorMetaActIdCacheCallback
{
    virtual IEngineRowAllocator *createAllocator(IOutputMetaData *meta, unsigned activityId, unsigned cacheId, roxiemem::RoxieHeapFlags flags) const = 0;
};

extern THORHELPER_API IRowAllocatorMetaActIdCache *createRowAllocatorCache(IRowAllocatorMetaActIdCacheCallback *callback);

extern THORHELPER_API bool isRowCheckValid(unsigned allocatorId, const void * row);

//Inline call which avoids the call if no row checking is enabled.
inline bool RoxieRowCheckValid(unsigned allocatorId, const void * row)
{
    if (allocatorId & ALLOCATORID_CHECK_MASK)
        return isRowCheckValid(allocatorId, row);
    return true;
}

class RoxieRowLinkCounter : public CSimpleInterface, implements IRowLinkCounter
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    virtual void releaseRow(const void *row)
    {
        ReleaseRoxieRow(row);
    }
    virtual void linkRow(const void *row)
    {
        LinkRoxieRow(row);
    }
};

#endif
