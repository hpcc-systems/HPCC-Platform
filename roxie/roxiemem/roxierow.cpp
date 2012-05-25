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
#include "jcrc.hpp"

#include "thorcommon.ipp" // for CachedOutputMetaData

#include "roxierow.hpp"

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

//This is here as demonstration of an alternative implementation...  crc16 is possibly a bit expensive.
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
    case NoCheckingHelper::allocatorCheckFlag & ALLOCATORID_CHECK_MASK:
        return true;
    case Crc16CheckingHelper::allocatorCheckFlag & ALLOCATORID_CHECK_MASK:
        return Crc16CheckingHelper::isValid(row);
    case Sum16CheckingHelper::allocatorCheckFlag & ALLOCATORID_CHECK_MASK:
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
        heap.setown(rowManager.createFixedRowHeap(meta.getFixedSize()+CHECKER::extraSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED | CHECKER::allocatorCheckFlag, (roxiemem::RoxieHeapFlags)flags));
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
        void * row = heap->allocate(allocSize+CHECKER::extraSize, allocatedSize);
        //This test should get constant folded to avoid the decrement when not checked.
        if (CHECKER::extraSize)
            allocatedSize -= CHECKER::extraSize;
        return row;
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        size32_t oldsize = size;  // don't need to include the extra checking bytes
        void * newrow = heap->resizeRow(row, oldsize, newSize+CHECKER::extraSize, size);
        if (CHECKER::extraSize)
            size -= CHECKER::extraSize;
        return newrow;
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


#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>
#define ASSERT(a) { if (!(a)) CPPUNIT_ASSERT(a); }

namespace roxiemem {

class RoxieRowAllocatorTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieRowAllocatorTests );
        CPPUNIT_TEST(testChecking);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieRowAllocatorTests() : logctx(queryDummyContextLogger())
    {
        setTotalMemoryLimit(40*HEAP_ALIGNMENT_SIZE, 0, NULL);
    }

    ~RoxieRowAllocatorTests()
    {
        releaseRoxieHeap();
    }

protected:
    class CheckingRowAllocatorCache : public IRowAllocatorCache
    {
    public:
        CheckingRowAllocatorCache() { numFailures = 0; }
        virtual unsigned getActivityId(unsigned cacheId) const { return 0; }
        virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const { return out.append(cacheId); }
        virtual void onDestroy(unsigned cacheId, void *row) const
        {
            if (!RoxieRowCheckValid(cacheId, row))
                ++numFailures;
        }
        virtual void checkValid(unsigned cacheId, const void *row) const
        {
            if (!RoxieRowCheckValid(cacheId, row))
                ++numFailures;
        }

        mutable unsigned numFailures;
    };

    class DummyOutputMeta : public IOutputMetaData, public CInterface
    {
    public:
        DummyOutputMeta(size32_t _minSize, size32_t _fixedSize) : minSize(_minSize), fixedSize(_fixedSize) {}
        IMPLEMENT_IINTERFACE

        virtual size32_t getRecordSize(const void *rec) { return minSize; }
        virtual size32_t getFixedSize() const { return fixedSize; }
        virtual size32_t getMinRecordSize() const { return minSize; }
        virtual void toXML(const byte * self, IXmlWriter & out) {}
        virtual unsigned getVersion() const { return 0; }
        virtual unsigned getMetaFlags() { return 0; }
        virtual IOutputMetaData * querySerializedMeta() { return this; }

        virtual void destruct(byte * self) {}
        virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}

        size32_t minSize;
        size32_t fixedSize;
    };

    void testAllocator(IOutputMetaData * meta, bool packed, unsigned low, unsigned high, int modify, bool checking)
    {
        CheckingRowAllocatorCache cache;
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, &cache);
        Owned<IEngineRowAllocator> alloc = checking ? createCrcRoxieRowAllocator(*rm, meta, 0, 0, packed) : createRoxieRowAllocator(*rm, meta, 0, 0, packed);

        for (unsigned size=low; size <= high; size++)
        {
            unsigned capacity;
            unsigned prevFailures = cache.numFailures;
            void * row = alloc->createRow(capacity);
            if (low != high)
                row = alloc->resizeRow(size, row, capacity);
            for (unsigned i1=0; i1 < capacity; i1++)
                ((byte *)row)[i1] = i1;
            const void * final = alloc->finalizeRow(capacity, row, capacity);
            for (unsigned i2=0; i2 < capacity; i2++)
            {
                ASSERT(((byte *)row)[i2] == i2);
            }

            if (modify != 0)
            {
                if (modify < 0)
                    ((byte *)row)[0]++;
                else
                    ((byte *)row)[size-1]++;
            }
            ReleaseRoxieRow(row);
            if (modify == 0)
            {
                ASSERT(prevFailures == cache.numFailures);
            }
            else
            {
                ASSERT(prevFailures+1 == cache.numFailures);
            }
        }
    }

    void testAllocator(IOutputMetaData * meta, bool packed, unsigned low, unsigned high)
    {
        testAllocator(meta, packed, low, high, 0, false);
        testAllocator(meta, packed, low, high, 0, true);
        testAllocator(meta, packed, low, high, -1, true);
        testAllocator(meta, packed, low, high, +1, true);
    }

    void testChecking()
    {
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, NULL);

        for (unsigned fixedSize=1; fixedSize<64; fixedSize++)
        {
            DummyOutputMeta meta(fixedSize, fixedSize);
            testAllocator(&meta, false, fixedSize, fixedSize);
            testAllocator(&meta, true, fixedSize, fixedSize);
        }

        for (unsigned varSize=1; varSize<64; varSize++)
        {
            DummyOutputMeta meta(varSize, 0);
            testAllocator(&meta, false, varSize, varSize);
            testAllocator(&meta, false, 1, varSize);
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( RoxieRowAllocatorTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieRowAllocatorTests, "RoxieRowAllocatorTests" );

} // namespace roxiemem
#endif
