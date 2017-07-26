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
//so that rows are checked on destruction.
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
        memsize_t capacity = RoxieRowCapacity(ptr);
        if (capacity < size + extraSize)
            throw MakeStringException(0, "Data was written past the end of the row - allocated %d, written %d", (size32_t)(capacity - extraSize), size);
        memset(ptr+size, 0, capacity - size - extraSize);
        unsigned short * check = reinterpret_cast<unsigned short *>(ptr + capacity - extraSize);
        *check = crc16(ptr, capacity-extraSize, 0);
    }
    static inline bool isValid(const void * _ptr)
    {
        if (RoxieRowHasDestructor(_ptr))
        {
            const byte * ptr = static_cast<const byte *>(_ptr);
            memsize_t capacity = RoxieRowCapacity(ptr);
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
        memsize_t capacity = RoxieRowCapacity(ptr);
        if (capacity < size + extraSize)
            throw MakeStringException(0, "Data was written past the end of the row - allocated %d, written %d", (size32_t)(capacity - extraSize), size);
        memset(ptr+size, 0, capacity - size - extraSize);
        unsigned short * check = reinterpret_cast<unsigned short *>(ptr + capacity - extraSize);
        *check = chksum16(ptr, capacity-extraSize);
    }
    static inline bool isValid(const void * _ptr)
    {
        if (RoxieRowHasDestructor(_ptr))
        {
            const byte * ptr = static_cast<const byte *>(_ptr);
            memsize_t capacity = RoxieRowCapacity(ptr);
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
class RoxieEngineRowAllocatorBase : implements IEngineRowAllocator, public CInterface
{
public:
    RoxieEngineRowAllocatorBase(IRowAllocatorMetaActIdCache * _cache, roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, roxiemem::RoxieHeapFlags _createFlags)
        : createFlags(_createFlags), cache(_cache), rowManager(_rowManager), meta(_meta)
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
    virtual unsigned queryActivityId() const
    {
        return activityId;
    }
    virtual StringBuffer &getId(StringBuffer &idStr)
    {
        return idStr.append(activityId); // MORE - may want more context info in here
    }
    virtual IOutputRowSerializer *createDiskSerializer(ICodeContext *ctx)
    {
        return meta.createDiskSerializer(ctx, activityId);
    }
    virtual IOutputRowDeserializer *createDiskDeserializer(ICodeContext *ctx)
    {
        return meta.createDiskDeserializer(ctx, activityId);
    }
    virtual IOutputRowSerializer *createInternalSerializer(ICodeContext *ctx)
    {
        return meta.createInternalSerializer(ctx, activityId);
    }
    virtual IOutputRowDeserializer *createInternalDeserializer(ICodeContext *ctx)
    {
        return meta.createInternalDeserializer(ctx, activityId);
    }
    virtual IEngineRowAllocator *createChildRowAllocator(const RtlTypeInfo *type)
    {
        CriticalBlock block(cs); // Not very likely but better be safe
        if (children.empty())
        {
            for (unsigned i =0;;i++)
            {
                IOutputMetaData * childMeta = meta.queryChildMeta(i);
                if (!childMeta)
                    break;
                children.append(*cache->ensure(childMeta, activityId, createFlags));
            }
        }
        ForEachItemIn(i, children)
        {
            IEngineRowAllocator & cur = children.item(i);
            if (cur.queryOutputMeta()->queryTypeInfo() == type)
                return LINK(&cur);
        }
        return NULL;
    }

protected:
    inline byte * * doReallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        if (!rowset)
            return createRowset(newRowCount);

        //Occasionally (in aggregates) we may try and append to a shared rowset.  In this case we need to clone the
        //target rowset.  It could be that the rowset is unshared immediately, but that is inefficient at worst.
        if (RoxieRowIsShared(rowset))
        {
            byte * * newset = createRowset(newRowCount);
            for (unsigned i=0; i < oldRowCount; i++)
            {
                byte * cur = rowset[i];
                LinkRoxieRow(cur);
                newset[i] = cur;
            }
            ReleaseRoxieRow(rowset);
            return newset;
        }

        //This would be more efficient if previous capacity was stored by the caller - or if capacity() is more efficient
        if (newRowCount * sizeof(void *) <= RoxieRowCapacity(rowset))
            return rowset;

        memsize_t capacity;
        void * ptr = (void *)rowset;
        rowManager.resizeRow(capacity, ptr, oldRowCount * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);
        return (byte * *)ptr;
    }

protected:
    static CriticalSection cs; // Very unlikely to have contention, so share between all allocators
    roxiemem::RoxieHeapFlags createFlags;
    IRowAllocatorMetaActIdCache * cache;
    roxiemem::IRowManager & rowManager;
    const CachedOutputMetaData meta;
    unsigned activityId;
    unsigned allocatorId;
    IArrayOf<IEngineRowAllocator> children;
};

CriticalSection RoxieEngineRowAllocatorBase::cs;

template <class CHECKER>
class RoxieEngineFixedRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineFixedRowAllocator(IRowAllocatorMetaActIdCache * _cache, roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, roxiemem::RoxieHeapFlags _flags)
        : RoxieEngineRowAllocatorBase(_cache, _rowManager, _meta, _activityId, _allocatorId, _flags)
    {
        unsigned flags = _flags;
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

    virtual void gatherStats(CRuntimeStatisticCollection & stats) override
    {
        heap->gatherStats(stats);
    }

protected:
    Owned<roxiemem::IFixedRowHeap> heap;
};

template <class CHECKER>
class RoxieEngineVariableRowAllocator : public RoxieEngineRowAllocatorBase
{
public:
    RoxieEngineVariableRowAllocator(IRowAllocatorMetaActIdCache * _cache, roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId, roxiemem::RoxieHeapFlags _flags)
        : RoxieEngineRowAllocatorBase(_cache, _rowManager, _meta, _activityId, _allocatorId, _flags)
    {
        unsigned flags = _flags;
        if (meta.needsDestruct() || CHECKER::allocatorCheckFlag)
            flags |= roxiemem::RHFhasdestructor;
        heap.setown(rowManager.createVariableRowHeap(allocatorId | ACTIVITY_FLAG_ISREGISTERED | CHECKER::allocatorCheckFlag, (roxiemem::RoxieHeapFlags)flags));
    }

    virtual void * createRow()
    {
        memsize_t allocSize = meta.getInitialSize();
        memsize_t capacity;
        return heap->allocate(allocSize+CHECKER::extraSize, capacity);
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        const memsize_t allocSize = meta.getInitialSize();
        memsize_t newCapacity; // always initialised by allocate
        void * row = heap->allocate(allocSize+CHECKER::extraSize, newCapacity);
        //This test should get constant folded to avoid the decrement when not checked.
        if (CHECKER::extraSize)
            newCapacity -= CHECKER::extraSize;
        allocatedSize = (size32_t)newCapacity;
        return row;
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        const size32_t oldsize = size;  // don't need to include the extra checking bytes
        memsize_t newCapacity; // always initialised by resizeRow
        void * newrow = heap->resizeRow(row, oldsize, newSize+CHECKER::extraSize, newCapacity);
        if (CHECKER::extraSize)
            newCapacity -= CHECKER::extraSize;
        size = (size32_t)newCapacity;
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

    virtual void gatherStats(CRuntimeStatisticCollection & stats) override
    {
        heap->gatherStats(stats);
    }

protected:
    Owned<roxiemem::IVariableRowHeap> heap;
};


IEngineRowAllocator * createRoxieRowAllocator(IRowAllocatorMetaActIdCache * cache, roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, roxiemem::RoxieHeapFlags flags)
{
    if (meta->getFixedSize() != 0)
        return new RoxieEngineFixedRowAllocator<NoCheckingHelper>(cache, rowManager, meta, activityId, allocatorId, flags);
    else
        return new RoxieEngineVariableRowAllocator<NoCheckingHelper>(cache, rowManager, meta, activityId, allocatorId, flags);
}

IEngineRowAllocator * createCrcRoxieRowAllocator(IRowAllocatorMetaActIdCache * cache, roxiemem::IRowManager & rowManager, IOutputMetaData * meta, unsigned activityId, unsigned allocatorId, roxiemem::RoxieHeapFlags flags)
{
    if (meta->getFixedSize() != 0)
        return new RoxieEngineFixedRowAllocator<Crc16CheckingHelper>(cache, rowManager, meta, activityId, allocatorId, flags);
    else
        return new RoxieEngineVariableRowAllocator<Crc16CheckingHelper>(cache, rowManager, meta, activityId, allocatorId, flags);
}

#pragma pack(push,1) // hashing on members, so ensure contiguous
struct AllocatorKey
{
    IOutputMetaData *meta;
    unsigned activityId;
    roxiemem::RoxieHeapFlags flags;
    AllocatorKey(IOutputMetaData *_meta, unsigned _activityId, roxiemem::RoxieHeapFlags _flags)
        : meta(_meta), activityId(_activityId), flags(_flags)
    {
    }
    bool operator==(AllocatorKey const &other) const
    {
        return (meta == other.meta) && (activityId == other.activityId) && (flags == other.flags);
    }
};
#pragma pack(pop)

class CAllocatorCacheItem : public OwningHTMapping<IEngineRowAllocator, AllocatorKey>
{
    Linked<IOutputMetaData> meta;
    unsigned allocatorId;
public:
    CAllocatorCacheItem(IEngineRowAllocator *allocator, unsigned _allocatorId, AllocatorKey &key)
        : OwningHTMapping<IEngineRowAllocator, AllocatorKey>(*allocator, key), allocatorId(_allocatorId)
    {
        meta.set(key.meta);
    }
    unsigned queryAllocatorId() const { return allocatorId; }
};

class CAllocatorCache : public CSimpleInterfaceOf<IRowAllocatorMetaActIdCache>
{
    OwningSimpleHashTableOf<CAllocatorCacheItem, AllocatorKey> cache;
    IArrayOf<IEngineRowAllocator> allAllocators;
    mutable SpinLock allAllocatorsLock;
    Owned<roxiemem::IRowManager> rowManager;
    IRowAllocatorMetaActIdCacheCallback *callback;

    inline CAllocatorCacheItem *_lookup(IOutputMetaData *meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) const
    {
        AllocatorKey key(meta, activityId, flags);
        return cache.find(key);
    }
public:
    CAllocatorCache(IRowAllocatorMetaActIdCacheCallback *_callback) : callback(_callback)
    {
    }
// IRowAllocatorMetaActIdCache
    virtual IEngineRowAllocator *ensure(IOutputMetaData * meta, unsigned activityId, roxiemem::RoxieHeapFlags flags)
    {
        SpinBlock b(allAllocatorsLock);
        for (;;)
        {
            CAllocatorCacheItem *container = _lookup(meta, activityId, flags);
            if (container)
            {
                if (0 == (roxiemem::RHFunique & flags))
                    return LINK(&container->queryElement());
                // if in cache but unique, reuse allocatorId
                SpinUnblock b(allAllocatorsLock);
                return callback->createAllocator(this, meta, activityId, container->queryAllocatorId(), flags);
            }
            // NB: a RHFunique allocator, will cause 1st to be added to 'allAllocators'
            // subsequent requests for the same type of unique allocator, will share same allocatorId
            // resulting in the 1st allocator being reused by all instances for onDestroy() etc.

            assertex(allAllocators.ordinality() < ALLOCATORID_MASK);
            unsigned allocatorId = allAllocators.ordinality();
            IEngineRowAllocator *ret;
            {
                SpinUnblock b(allAllocatorsLock);
                ret = callback->createAllocator(this, meta, activityId, allocatorId, flags);
                assertex(ret);
            }
            if (allocatorId == allAllocators.ordinality())
            {
                AllocatorKey key(meta, activityId, flags);
                container = new CAllocatorCacheItem(LINK(ret), allocatorId, key);
                cache.replace(*container);
                allAllocators.append(*LINK(ret));

                return ret;
            }
            else
            {
                // someone has used the allocatorId I was going to use.. release and try again (hopefully happens very seldom)
                ret->Release();
            }
        }
    }
    virtual unsigned items() const
    {
        return allAllocators.ordinality();
    }
// roxiemem::IRowAllocatorCache
    virtual unsigned getActivityId(unsigned cacheId) const
    {
        unsigned allocatorIndex = (cacheId & ALLOCATORID_MASK);
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(allocatorIndex))
            return allAllocators.item(allocatorIndex).queryActivityId();
        else
        {
            //assert(false);
            return UNKNOWN_ACTIVITY; // Used for tracing, better than a crash...
        }
    }
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const
    {
        unsigned allocatorIndex = (cacheId & ALLOCATORID_MASK);
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(allocatorIndex))
            return allAllocators.item(allocatorIndex).getId(out);
        else
        {
            assert(false);
            return out.append("unknown"); // Used for tracing, better than a crash...
        }
    }
    virtual void onDestroy(unsigned cacheId, void *row) const
    {
        IEngineRowAllocator *allocator;
        unsigned allocatorIndex = (cacheId & ALLOCATORID_MASK);
        {
            SpinBlock b(allAllocatorsLock); // just protect the access to the array - don't keep locked for the call of destruct or may deadlock
            if (allAllocators.isItem(allocatorIndex))
                allocator = &allAllocators.item(allocatorIndex);
            else
            {
                assert(false);
                return;
            }
        }
        if (!RoxieRowCheckValid(cacheId, row))
        {
            throw MakeStringException(0, "ERROR: crc check failure destroying row!");
        }
        allocator->queryOutputMeta()->destruct((byte *) row);
    }
    virtual void onClone(unsigned cacheId, void *row) const
    {
        IEngineRowAllocator *allocator;
        unsigned allocatorIndex = (cacheId & ALLOCATORID_MASK);
        {
            SpinBlock b(allAllocatorsLock); // just protect the access to the array - don't keep locked for the call of destruct or may deadlock
            if (allAllocators.isItem(allocatorIndex))
                allocator = &allAllocators.item(allocatorIndex);
            else
            {
                assert(false);
                return;
            }
        }
        if (!RoxieRowCheckValid(cacheId, row))
        {
            throw MakeStringException(0, "ERROR: crc check failure cloning row!");
        }
        //This should only be called if the destructor needs to be called - so don't bother checking
        ChildRowLinkerWalker walker;
        allocator->queryOutputMeta()->walkIndirectMembers((const byte *)row, walker);
    }
    virtual void checkValid(unsigned cacheId, const void *row) const
    {
        if (!RoxieRowCheckValid(cacheId, row))
        {
            throw MakeStringException(0, "ERROR: crc check failure checking row!");
        }
    }
};

IRowAllocatorMetaActIdCache *createRowAllocatorCache(IRowAllocatorMetaActIdCacheCallback *callback)
{
    return new CAllocatorCache(callback);
}



#ifdef _USE_CPPUNIT
#include "unittests.hpp"

namespace roxierowtests {
using namespace roxiemem;

class RoxieRowAllocatorTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieRowAllocatorTests );
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testChecking);
        CPPUNIT_TEST(testCleanup);
        CPPUNIT_TEST(testAllocatorCache);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieRowAllocatorTests() : logctx(queryDummyContextLogger())
    {
    }

    ~RoxieRowAllocatorTests()
    {
    }

protected:
    class CheckingRowAllocatorCache : public CSimpleInterface, public IRowAllocatorCache
    {
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CheckingRowAllocatorCache() { numFailures = 0; }
        virtual unsigned getActivityId(unsigned cacheId) const { return 0; }
        virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const { return out.append(cacheId); }
        virtual void onDestroy(unsigned cacheId, void *row) const
        {
            if (!RoxieRowCheckValid(cacheId, row))
                ++numFailures;
        }
        virtual void onClone(unsigned cacheId, void *row) const
        {
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
        virtual IOutputMetaData * querySerializedDiskMeta() { return this; }

        virtual void destruct(byte * self) {}
        virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
        virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
        virtual IOutputMetaData * queryChildMeta(unsigned i) { return NULL; }
        virtual const RtlRecord &queryRecordAccessor(bool expand) const { UNIMPLEMENTED; }

        size32_t minSize;
        size32_t fixedSize;
    };

    void testAllocator(IOutputMetaData * meta, roxiemem::RoxieHeapFlags flags, unsigned low, unsigned high, int modify, bool checking)
    {
        CheckingRowAllocatorCache cache;
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, &cache);
        Owned<IEngineRowAllocator> alloc = checking ? createCrcRoxieRowAllocator(NULL, *rm, meta, 0, 0, flags) : createRoxieRowAllocator(NULL, *rm, meta, 0, 0, flags);

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

    void testAllocator(IOutputMetaData * meta, roxiemem::RoxieHeapFlags flags, unsigned low, unsigned high)
    {
        testAllocator(meta, flags, low, high, 0, false);
        testAllocator(meta, flags, low, high, 0, true);
        testAllocator(meta, flags, low, high, -1, true);
        testAllocator(meta, flags, low, high, +1, true);
    }

    void testSetup()
    {
        setTotalMemoryLimit(false, true, false, 40*HEAP_ALIGNMENT_SIZE, 0, NULL, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    void testChecking()
    {
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, NULL);

        for (unsigned fixedSize=1; fixedSize<64; fixedSize++)
        {
            DummyOutputMeta meta(fixedSize, fixedSize);
            testAllocator(&meta, RHFnone, fixedSize, fixedSize);
            testAllocator(&meta, RHFpacked, fixedSize, fixedSize);
        }

        for (unsigned varSize=1; varSize<64; varSize++)
        {
            DummyOutputMeta meta(varSize, 0);
            testAllocator(&meta, RHFnone, varSize, varSize);
            testAllocator(&meta, RHFnone, 1, varSize);
        }
    }

    void testAllocatorCache()
    {
        IArrayOf<IOutputMetaData> metas;
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, NULL);
        class CAllocatorCallback : implements IRowAllocatorMetaActIdCacheCallback
        {
            IRowManager *rm;
        public:
            CAllocatorCallback(IRowManager *_rm) : rm(_rm)
            {
            }
            virtual IEngineRowAllocator *createAllocator(IRowAllocatorMetaActIdCache * cache, IOutputMetaData *meta, unsigned activityId, unsigned cacheId, roxiemem::RoxieHeapFlags flags) const
            {
                return createRoxieRowAllocator(cache, *rm, meta, activityId, cacheId, flags);
            }
        } callback(rm);
        Owned<IRowAllocatorMetaActIdCache> allocatorCache = createRowAllocatorCache(&callback);
        // create 64 allocators, 32 different activityId's
        for (unsigned fixedSize=1; fixedSize<=64; fixedSize++)
        {
            DummyOutputMeta *meta = new DummyOutputMeta(fixedSize, fixedSize);
            metas.append(*meta);

            unsigned activityId = 1 + ((fixedSize-1) % 32); // i.e. make an id, so half are duplicates
            Owned<IEngineRowAllocator> allocator = allocatorCache->ensure(meta, activityId, roxiemem::RHFnone);
        }
        // test that 64 in cache
        ASSERT(allocatorCache->items() == 64);

        // test ensure again
        for (unsigned fixedSize=1; fixedSize<=64; fixedSize++)
        {
            unsigned activityId = 1 + ((fixedSize-1) % 32); // i.e. make an id, so half are duplicates
            IOutputMetaData *meta = &metas.item(fixedSize-1); // from 1st round
            Owned<IEngineRowAllocator> allocator = allocatorCache->ensure(meta, activityId, roxiemem::RHFnone);
        }
        ASSERT(allocatorCache->items() == 64);

        metas.kill();
        allocatorCache.clear();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( RoxieRowAllocatorTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieRowAllocatorTests, "RoxieRowAllocatorTests" );

} // namespace roxiemem
#endif
