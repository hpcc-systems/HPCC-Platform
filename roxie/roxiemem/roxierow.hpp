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

extern roxiemem_decl IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool packed);
extern roxiemem_decl IEngineRowAllocator * createCrcRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, bool packed);

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
