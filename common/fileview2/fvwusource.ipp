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

#ifndef FVWUSOURCE_IPP
#define FVWUSOURCE_IPP

#include "fvdatasource.hpp"
#include "dllserver.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"

#include "fvsource.ipp"

class WorkUnitDataSource : public FVDataSource
{
public:
    WorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid);

//interface IFvDataSource
    virtual bool isIndex() { return false; }
    virtual __int64 numRows(bool force = false);

    virtual bool init();

protected:
    unsigned __int64 totalRows;
    unsigned __int64 totalSize;
};


class FullWorkUnitDataSource : public WorkUnitDataSource
{
public:
    FullWorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid);

    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset);
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset);
    virtual bool init();

protected:
    Owned<RowBlock> rows;
};


class PagedWorkUnitDataSource : public WorkUnitDataSource
{
public:
    PagedWorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid);

    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset);
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset);
    virtual bool init();

    bool loadBlock(__int64 startRow, offset_t startOffset);

protected:
    RowCache cache;
};

#endif
