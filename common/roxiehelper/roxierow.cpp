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

//Classes can be used to configure the allocator, and add extra data to the end.
//The checking needs to be done by setting a bit in the allocatorid
class NoCheckingHelper
{
public:
    enum {
        extraSize = 0,
        allocatorCheckFlag = 0x00000000
    };
    static inline void setCheck(size32_t size, void * ptr) {}
    static inline bool isValid(const void * ptr) { return true; }
};

//NOTE: If a row requires checking then the row will also have the bit set to indicate it requires a destructor
//so that rows are checked on destructon.
//Therefore checking if the destructor is set for a row in isValid() to protect us from uninitialised crcs.
class Crc16CheckingHelper
{
public:
    enum {
        extraSize = sizeof(unsigned short),
        allocatorCheckFlag = 0x00100000|ACTIVITY_FLAG_NEEDSDESTRUCTOR
    };
    static inline void setCheck(size32_t size, void * _ptr)
    {
        byte * ptr = static_cast<byte *>(_ptr);
        size32_t capacity = RoxieRowCapacity(ptr);
        memset(ptr+size, 0, capacity - size - extraSize);
        unsigned short * check = reinterpret_cast<unsigned short *>(ptr + capacity - extraSize);
        *check = crc16(ptr, capacity-extraSize, 0);
    }
    static inline bool isValid(const void * _ptr)
    {
        if (RoxieRowHasDestructor(_ptr))
        {
            const byte * ptr = static_cast<const byte *>(_ptr);
            size32_t capacity = RoxieRowCapacity(ptr);
            const unsigned short * check = reinterpret_cast<const unsigned short *>(ptr + capacity - extraSize);
            return *check == crc16(ptr, capacity-extraSize, 0);
        }
        return true;
    }
};

class Sum16CheckingHelper
{
public:
    enum {
        extraSize = sizeof(unsigned short),
        allocatorCheckFlag = 0x00200000|ACTIVITY_FLAG_NEEDSDESTRUCTOR
    };
    static inline void setCheck(size32_t size, void * _ptr)
    {
        byte * ptr = static_cast<byte *>(_ptr);
        size32_t capacity = RoxieRowCapacity(ptr);
        memset(ptr+size, 0, capacity - size - extraSize);
        unsigned short * check = reinterpret_cast<unsigned short *>(ptr + capacity - extraSize);
        *check = chksum16(ptr, capacity-extraSize);
    }
    static inline bool isValid(const void * _ptr)
    {
        if (RoxieRowHasDestructor(_ptr))
        {
            const byte * ptr = static_cast<const byte *>(_ptr);
            size32_t capacity = RoxieRowCapacity(ptr);
            const unsigned short * check = reinterpret_cast<const unsigned short *>(ptr + capacity - extraSize);
            return chksum16(ptr, capacity-extraSize) == *check;
        }
        return true;
    }
};

bool isRowCheckValid(unsigned allocatorId, const void * row)
{
    switch (allocatorId & ALLOCATORID_CHECK_MASK)
    {
    case NoCheckingHelper::allocatorCheckFlag:
        return true;
    case Crc16CheckingHelper::allocatorCheckFlag:
        return Crc16CheckingHelper::isValid(row);
    case Sum16CheckingHelper::allocatorCheckFlag:
        return Sum16CheckingHelper::isValid(row);
    default:
        UNIMPLEMENTED;
    }
}

//--------------------------------------------------------------------------------------------------------------------

//More: Function to calculate the total size of a row - requires access to a rowallocator.

//--------------------------------------------------------------------------------------------------------------------
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

template <class CHECKER>
class RoxieEngineFixedRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineFixedRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool packed)
        : RoxieEngineRowAllocatorBase(_rowManager, _meta, _activityId, _allocatorId)
    {
        unsigned flags = packed ? roxiemem::RHFpacked : roxiemem::RHFnone;
        if (meta.needsDestruct() || CHECKER::allocatorCheckFlag)
            flags |= roxiemem::RHFhasdestructor;
        heap.setown(rowManager.createFixedRowHeap(meta.getFixedSize(), allocatorId | ACTIVITY_FLAG_ISREGISTERED | CHECKER::allocatorCheckFlag, (roxiemem::RoxieHeapFlags)flags));
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
        if (!meta.needsDestruct() && !CHECKER::allocatorCheckFlag)
            return row;
        CHECKER::setCheck(finalSize, row);
        return heap->finalizeRow(row);
    }

protected:
    Owned<roxiemem::IFixedRowHeap> heap;
};

template <class CHECKER>
class RoxieEngineVariableRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineVariableRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, bool packed)
        : RoxieEngineRowAllocatorBase(_rowManager, _meta, _activityId, _allocatorId)
    {
        unsigned flags = packed ? roxiemem::RHFpacked : roxiemem::RHFnone;
        if (meta.needsDestruct() || CHECKER::allocatorCheckFlag)
            flags |= roxiemem::RHFhasdestructor;
        heap.setown(rowManager.createVariableRowHeap(allocatorId | ACTIVITY_FLAG_ISREGISTERED | CHECKER::allocatorCheckFlag, (roxiemem::RoxieHeapFlags)flags));
    }

    virtual void * createRow()
    {
        size32_t allocSize = meta.getInitialSize();
        size32_t capacity;
        return heap->allocate(allocSize+CHECKER::extraSize, capacity);
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        const size32_t allocSize = meta.getInitialSize();
        return heap->allocate(allocSize+CHECKER::extraSize, allocatedSize);
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        return heap->resizeRow(row, size, newSize+CHECKER::extraSize, size);
    }

    virtual void * finalizeRow(size32_t finalSize, void * row, size32_t oldSize)
    {
        if (!meta.needsDestruct() && !CHECKER::allocatorCheckFlag)
            return row;
        void * newrow = heap->finalizeRow(row, oldSize, finalSize+CHECKER::extraSize);
        CHECKER::setCheck(finalSize, newrow);
        return newrow;
    }

protected:
    Owned<roxiemem::IVariableRowHeap> heap;
};


IEngineRowAllocator * createRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, bool packed)
{
    if (meta->getFixedSize() != 0)
        return new RoxieEngineFixedRowAllocator<NoCheckingHelper>(rowManager, meta, activityId, allocatorId, packed);
    else
        return new RoxieEngineVariableRowAllocator<NoCheckingHelper>(rowManager, meta, activityId, allocatorId, packed);
}

IEngineRowAllocator * createCrcRoxieRowAllocator(roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, bool packed)
{
    if (meta->getFixedSize() != 0)
        return new RoxieEngineFixedRowAllocator<Crc16CheckingHelper>(rowManager, meta, activityId, allocatorId, packed);
    else
        return new RoxieEngineVariableRowAllocator<Crc16CheckingHelper>(rowManager, meta, activityId, allocatorId, packed);
}
