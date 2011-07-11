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
