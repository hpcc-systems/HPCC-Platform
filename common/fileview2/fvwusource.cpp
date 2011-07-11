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
#include "jliball.hpp"
#include "eclrtl.hpp"

#include "fvresultset.ipp"

#include "fileview.hpp"
#include "fvwusource.ipp"
#include "eclhelper.hpp"

#include "fvdatasource.hpp"

WorkUnitDataSource::WorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid)
{
    wuResult.set(_wuResult);
    wuid.set(_wuid);

    totalRows = wuResult->getResultTotalRowCount();
    if (totalRows == -1)
        totalRows = 0;
    totalSize = wuResult->getResultRawSize(NULL, NULL);
}

bool WorkUnitDataSource::init()
{
    return setReturnedInfoFromResult();
}


__int64 WorkUnitDataSource::numRows(bool force)
{
    return totalRows;
}

//---------------------------------------------------------------------------

FullWorkUnitDataSource::FullWorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid) : WorkUnitDataSource(_wuResult, _wuid)
{
}

bool FullWorkUnitDataSource::init()
{
    bool ok = WorkUnitDataSource::init();
    if (ok)
    {
        MemoryBuffer temp;
        MemoryBuffer2IDataVal xxx(temp);

        //Nasty.  Single sets are represented as the same way as datasets (with an extra flag for all)
        //however need to represent as a single row containing a set, which has a different format.
        if (wuResult->isResultScalar() && returnedMeta->isSingleSet())
        {
            temp.append(wuResult->getResultIsAll());
            temp.append((size32_t)wuResult->getResultRawSize(0, 0));
        }
        wuResult->getResultRaw(xxx, NULL, NULL);

        if (returnedMeta->isFixedSize())
            rows.setown(new FixedRowBlock(temp, 0, 0, returnedMeta->fixedSize()));
        else
            rows.setown(new VariableRowBlock(temp, 0, 0, returnedRecordSize, true));
    }
    return ok;
}


bool FullWorkUnitDataSource::fetchRowData(MemoryBuffer & out, __int64 offset)
{
    size32_t length;
    const void * data = rows->fetchRow(offset, length);
    if (!data)
        return false;
    out.append(length, data);
    return true;
}

bool FullWorkUnitDataSource::getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset)
{
    data = rows->getRow(row, length, offset);
    return (data != NULL);
}

//---------------------------------------------------------------------------

PagedWorkUnitDataSource::PagedWorkUnitDataSource(IConstWUResult * _wuResult, const char * _wuid) : WorkUnitDataSource(_wuResult, _wuid)
{
}


bool PagedWorkUnitDataSource::init()
{
    return WorkUnitDataSource::init();
}

bool PagedWorkUnitDataSource::getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset)
{
    if ((row < 0) || ((unsigned __int64)row > totalRows))
        return false;

    RowLocation location;
    loop
    {
        if (cache.getCacheRow(row, location))
        {
            length = location.matchLength;
            data = location.matchRow;
            return true;
        }

        if (!loadBlock(location.bestRow, location.bestOffset))
            return false;
    }
}


bool PagedWorkUnitDataSource::fetchRowData(MemoryBuffer & out, __int64 offset)
{
    MemoryBuffer temp;
    MemoryBuffer2IDataVal wrapper(out); 
    wuResult->getResultRaw(wrapper, offset, returnedMeta->getMaxRecordSize(), NULL, NULL);
    if (temp.length() == 0)
        return false;
    return true;
}


bool PagedWorkUnitDataSource::loadBlock(__int64 startRow, offset_t startOffset)
{
    MemoryBuffer temp;
    MemoryBuffer2IDataVal xxx(temp); 
    RowBlock * rows;
    if (returnedMeta->isFixedSize())
    {
        unsigned fixedSize = returnedMeta->fixedSize();
        unsigned readSize = (WU_BLOCK_SIZE / fixedSize) * fixedSize;
        wuResult->getResultRaw(xxx, startOffset, readSize, NULL, NULL);
        if (temp.length() == 0)
            return false;
        rows = new FixedRowBlock(temp, startRow, startOffset, fixedSize);
    }
    else
    {
        wuResult->getResultRaw(xxx, startOffset, WU_BLOCK_SIZE, NULL, NULL);
        if (temp.length() == 0)
            return false;
        rows = new VariableRowBlock(temp, startRow, startOffset, returnedRecordSize, startOffset + temp.length() == totalSize);
    }
    cache.addRowsOwn(rows);
    return true;
}
