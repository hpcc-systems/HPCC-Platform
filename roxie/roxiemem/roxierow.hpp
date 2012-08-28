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

#ifdef _WIN32
 #ifdef ROXIEMEM_EXPORTS
  #define roxiemem_decl __declspec(dllexport)
 #else
  #define roxiemem_decl __declspec(dllimport)
 #endif
#else
 #define roxiemem_decl
#endif

#include "roxiemem.hpp"
#include "eclhelper.hpp"

#define ALLOCATORID_CHECK_MASK  0x00300000
#define ALLOCATORID_MASK                0x000fffff

extern roxiemem_decl IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, roxiemem::RoxieHeapFlags flags);
extern roxiemem_decl IEngineRowAllocator * createCrcRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, roxiemem::RoxieHeapFlags flags);

extern roxiemem_decl bool isRowCheckValid(unsigned allocatorId, const void * row);

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
