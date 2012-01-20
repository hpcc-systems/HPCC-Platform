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

#include "jexcept.hpp"
#include "roxierow.hpp"
#include "thorcommon.ipp"

class RoxieEngineRowAllocatorBase : public CInterface, implements IEngineRowAllocator
{
public:
    RoxieEngineRowAllocatorBase(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId)
        : rowManager(_rowManager), meta(_meta)
    {
        activityId = _activityId;
        allocatorId = _allocatorId;
    }

    IMPLEMENT_IINTERFACE

//interface IEngineRowsetAllocator
    virtual byte * * createRowset(unsigned count)
    {
        if (count == 0)
            return NULL;
        return (byte **) rowManager.allocate(count * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);
    }

    virtual void releaseRowset(unsigned count, byte * * rowset)
    {
        rtlReleaseRowset(count, rowset);
    }

    virtual byte * * linkRowset(byte * * rowset)
    {
        return rtlLinkRowset(rowset);
    }

    virtual byte * * appendRowOwn(byte * * rowset, unsigned newRowCount, void * row)
    {
        byte * * expanded = doReallocRows(rowset, newRowCount-1, newRowCount);
        expanded[newRowCount-1] = (byte *)row;
        return expanded;
    }

    virtual byte * * reallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        //New rows (if any) aren't cleared....
        return doReallocRows(rowset, oldRowCount, newRowCount);
    }

    virtual void releaseRow(const void * row)
    {
        ReleaseRoxieRow(row);
    }

    virtual void * linkRow(const void * row)
    {
        LinkRoxieRow(row);
        return const_cast<void *>(row);
    }

    virtual IOutputMetaData * queryOutputMeta()
    {
        return meta.queryOriginal();
    }
    virtual unsigned queryActivityId()
    {
        return activityId;
    }
    virtual StringBuffer &getId(StringBuffer &idStr)
    {
        return idStr.append(activityId); // MORE - may want more context info in here
    }
    virtual IOutputRowSerializer *createRowSerializer(ICodeContext *ctx)
    {
        return meta.createRowSerializer(ctx, activityId);
    }
    virtual IOutputRowDeserializer *createRowDeserializer(ICodeContext *ctx)
    {
        return meta.createRowDeserializer(ctx, activityId);
    }

protected:
    inline byte * * doReallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        if (!rowset)
            return createRowset(newRowCount);

        //This would be more efficient if previous capacity was stored by the caller - or if capacity() is more efficient
        if (newRowCount * sizeof(void *) <= RoxieRowCapacity(rowset))
            return rowset;

        size32_t capacity;
        return (byte * *)rowManager.resizeRow(rowset, oldRowCount * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED, capacity);
    }

protected:
    roxiemem::IRowManager & rowManager;
    const CachedOutputMetaData meta;
    unsigned activityId;
    unsigned allocatorId;
};

//General purpose row allocator here for reference - should be removed once spcialised versions are created
class RoxieEngineRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId)
        : RoxieEngineRowAllocatorBase(_rowManager, _meta, _activityId, _allocatorId)
    {
    }

    virtual void * createRow()
    {
        size32_t allocSize = meta.getInitialSize();
        return rowManager.allocate(allocSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        const size32_t allocSize = meta.getInitialSize();
        void *ret = rowManager.allocate(allocSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
        //more: allocate could return the allocated size, but that would penalise the fixed row case
        allocatedSize = RoxieRowCapacity(ret);
        return ret;
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        return rowManager.resizeRow(row, size, newSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED, size);
    }

    virtual void * finalizeRow(size32_t finalSize, void * row, size32_t oldSize)
    {
        unsigned id = allocatorId | ACTIVITY_FLAG_ISREGISTERED;
        if (meta.needsDestruct()) id |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
        return rowManager.finalizeRow(row, oldSize, finalSize, id);
    }
};

class RoxieEngineFixedRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineFixedRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool packed)
        : RoxieEngineRowAllocatorBase(_rowManager, _meta, _activityId, _allocatorId)
    {
        unsigned flags = packed ? roxiemem::RHFpacked : roxiemem::RHFnone;
        if (meta.needsDestruct())
            flags |= roxiemem::RHFhasdestructor;
        heap.setown(rowManager.createFixedRowHeap(meta.getFixedSize(), allocatorId | ACTIVITY_FLAG_ISREGISTERED, (roxiemem::RoxieHeapFlags)flags));
    }

    virtual void * createRow()
    {
        return heap->allocate();
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        allocatedSize = meta.getFixedSize();
        return heap->allocate();
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        throwUnexpected();
        return NULL;
    }

    virtual void * finalizeRow(size32_t finalSize, void * row, size32_t oldSize)
    {
        if (!meta.needsDestruct())
            return row;
        return heap->finalizeRow(row);
    }

protected:
    Owned<roxiemem::IFixedRowHeap> heap;
};

class RoxieEngineVariableRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineVariableRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool _packed)
        : RoxieEngineRowAllocatorBase(_rowManager, _meta, _activityId, _allocatorId), packed(_packed)
    {
        unsigned flags = packed ? roxiemem::RHFpacked : roxiemem::RHFnone;
        if (meta.needsDestruct())
            flags |= roxiemem::RHFhasdestructor;
        heap.setown(rowManager.createVariableRowHeap(allocatorId | ACTIVITY_FLAG_ISREGISTERED, (roxiemem::RoxieHeapFlags)flags));
    }

    virtual void * createRow()
    {
        size32_t allocSize = meta.getInitialSize();
        size32_t capacity;
        return heap->allocate(allocSize, capacity);
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        const size32_t allocSize = meta.getInitialSize();
        return heap->allocate(allocSize, allocatedSize);
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        return heap->resizeRow(row, size, newSize, size);
    }

    virtual void * finalizeRow(size32_t finalSize, void * row, size32_t oldSize)
    {
        if (!meta.needsDestruct() && !packed)
            return row;
        return heap->finalizeRow(row, oldSize, finalSize);
    }

protected:
    Owned<roxiemem::IVariableRowHeap> heap;
    bool packed;    // may not be needed - depends on implementation
};


IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, bool packed)
{
#if 0
    //old code
    return new RoxieEngineRowAllocator(rowManager, meta, activityId, allocatorId);
#else
    if (meta->getFixedSize() != 0)
        return new RoxieEngineFixedRowAllocator(rowManager, meta, activityId, allocatorId, packed);
    else
        return new RoxieEngineVariableRowAllocator(rowManager, meta, activityId, allocatorId, packed);
#endif
}
