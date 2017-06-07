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

#include "jio.hpp"
#include "jfile.hpp"
#include "jtime.hpp"
#include "jsort.hpp"
#include "sockfile.hpp"

#include "rtlkey.hpp"
#include "jhtree.hpp"

#include "thorstep.ipp"

#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"
#include "thsortu.hpp"
#include "../hashdistrib/thhashdistribslave.ipp"

#include "thdiskbaseslave.ipp"
#include "thindexreadslave.ipp"

enum AdditionStats { AS_Seeks, AS_Scans };
class CIndexReadSlaveBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
protected:
    StringAttr logicalFilename;
    IArrayOf<IPartDescriptor> partDescs;
    IHThorIndexReadBaseArg *helper;
    IHThorSourceLimitTransformExtra * limitTransformExtra;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IOutputRowSerializer> serializer;
    bool localKey = false;
    size32_t seekGEOffset = 0;
    __int64 lastSeeks = 0, lastScans = 0;
    UInt64Array _statsArr;
    SpinLock statLock;  // MORE: Can this be avoided by passing in the delta?
    unsigned __int64 *statsArr = nullptr;
    size32_t fixedDiskRecordSize = 0;
    rowcount_t progress = 0;
    unsigned currentKM = 0;
    bool eoi = false;
    IArrayOf<IKeyManager> keyManagers;
    IKeyManager *currentManager = nullptr;
    Owned<IKeyManager> keyMergerManager;
    Owned<IKeyIndexSet> keyIndexSet;
    bool keyMergerInUse = false;

    class TransformCallback : implements IThorIndexCallback , public CSimpleInterface
    {
    protected:
        IKeyManager *keyManager;
        offset_t filepos;
    public:
        TransformCallback() { keyManager = NULL; };
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface)

    //IThorIndexCallback
        virtual unsigned __int64 getFilePosition(const void *row)
        {
            return filepos;
        }
        virtual byte *lookupBlob(unsigned __int64 id) 
        { 
            size32_t dummy;
            return (byte *) keyManager->loadBlob(id, dummy); 
        }
        offset_t & getFPosRef() { return filepos; }
        void setManager(IKeyManager *_keyManager)
        {
            finishedRow();
            keyManager = _keyManager;
        }
        void finishedRow()
        {
            if (keyManager)
                keyManager->releaseBlobs(); 
        }
        void clearManager()
        {
            keyManager = NULL;
        }
    } callback;

    virtual bool keyed() { return false; }
    virtual void setManager(IKeyManager *manager)
    {
        clearManager();
        currentManager = manager;
        callback.setManager(manager);
        resetLastStats();
        helper->createSegmentMonitors(currentManager);
        currentManager->finishSegmentMonitors();
        currentManager->reset();
        keyMergerInUse = (manager == keyMergerManager);
    }
    void clearManager()
    {
        if (currentManager)
        {
            callback.clearManager();
            if (keyMergerInUse)
                keyMergerManager->reset(); // JCSMORE - not entirely sure why necessary, if not done, resetPending is false. Should releaseSegmentMonitors() handle?
            currentManager->releaseSegmentMonitors();
            currentManager = nullptr;
            keyMergerInUse = false;
        }
    }
    const void *createKeyedLimitOnFailRow()
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t sz = limitTransformExtra->transformOnKeyedLimitExceeded(row);
        if (sz)
            return row.finalizeRowClear(sz);
        return NULL;
    }
    const void *nextKey()
    {
        if (eoi)
            return nullptr;
        const void *ret = nullptr;
        dbgassertex(currentManager);
        for (;;)
        {
            if (currentManager->lookup(true))
            {
                noteStats(currentManager->querySeeks(), currentManager->queryScans());
                ret = (const void *)currentManager->queryKeyBuffer(callback.getFPosRef());
            }
            if (ret || keyMergerManager)
                break;
            ++currentKM;
            if (currentKM >= keyManagers.ordinality())
                break;
            else
                setManager(&keyManagers.item(currentKM));
        }
        if (NULL == ret || keyed())
        {
            eoi = true;
            return nullptr;
        }
        return ret;
    }
public:
    CIndexReadSlaveBase(CGraphElementBase *container) 
        : CSlaveActivity(container)
    {
        helper = (IHThorIndexReadBaseArg *)container->queryHelper();
        limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(helper->selectInterface(TAIsourcelimittransformextra_1));
        fixedDiskRecordSize = helper->queryDiskRecordSize()->querySerializedDiskMeta()->getFixedSize(); // 0 if variable and unused
        reInit = 0 != (helper->getFlags() & (TIRvarfilename|TIRdynamicfilename));
    }
    rowcount_t getCount(const rowcount_t keyedLimit, bool hard)
    {
        if (0 == partDescs.ordinality())
            return 0;
        // Note - don't use merger's count - it doesn't work
        unsigned __int64 count = 0;
        for (unsigned p=0; p<partDescs.ordinality(); p++)
        {
            IKeyManager &keyManager = keyManagers.item(p);
            setManager(&keyManager);
            if (hard) // checkCount checks hard key count only.
                count += keyManager.checkCount(keyedLimit-count); // part max, is total limit [keyedLimit] minus total so far [count]
            else
                count += keyManager.getCount();
            noteStats(keyManager.querySeeks(), keyManager.queryScans());
            if (count > keyedLimit)
                break;
        }
        clearManager();
        return (rowcount_t)count;
    }
    rowcount_t sendGetCount(rowcount_t count)
    {
        if (container.queryLocalOrGrouped())
            return count;
        sendPartialCount(*this, count);
        return getFinalCount(*this);
    }
    inline void resetLastStats()
    {
        lastSeeks = lastScans = 0;
    }
    inline void noteStats(unsigned seeks, unsigned scans)
    {
        SpinBlock b(statLock);
        statsArr[AS_Seeks] += seeks-lastSeeks;
        statsArr[AS_Scans] += scans-lastScans;
        lastSeeks = seeks;
        lastScans = scans;
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        data.read(logicalFilename);
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data); // channel to pass back partial counts for aggregation
        unsigned parts;
        data.read(parts);
        if (parts)
            deserializePartFileDescriptors(data, partDescs);
        localKey = partDescs.ordinality() ? partDescs.item(0).queryOwner().queryProperties().getPropBool("@local", false) : false;
        allocator.set(queryRowAllocator());
        deserializer.set(queryRowDeserializer());
        serializer.set(queryRowSerializer());
        helper->setCallback(&callback);
        _statsArr.append(0);
        _statsArr.append(0);
        statsArr = _statsArr.getArray();
        lastSeeks = lastScans = 0;
        ForEachItemIn(p, partDescs)
        {
            IPartDescriptor &part = partDescs.item(p);
            RemoteFilename rfn;
            part.getFilename(0, rfn);
            StringBuffer filePath;
            rfn.getPath(filePath);

            Owned<IDelayedFile> lfile = queryThor().queryFileCache().lookup(*this, logicalFilename, part);
            Owned<IKeyManager> klManager;

            unsigned crc=0;
            part.getCrc(crc);

            if ((localKey && partDescs.ordinality()>1) || seekGEOffset) // for now at least, no remote key support if stepping or merging
            {
                Owned<IKeyIndex> keyIndex = createKeyIndex(filePath, crc, *lfile, false, false);
                klManager.setown(createLocalKeyManager(keyIndex, fixedDiskRecordSize, nullptr));
                if (!keyIndexSet)
                    keyIndexSet.setown(createKeyIndexSet());
                keyIndexSet->addIndex(keyIndex.getClear());
            }
            else
            {
                bool allowRemote = getOptBool("remoteKeyFilteringEnabled");
                bool forceRemote = allowRemote ? getOptBool("forceDafilesrv") : false; // can only force remote, if forceDafilesrv and remoteKeyFilteringEnabled are enabled.
                klManager.setown(createKeyManager(filePath, fixedDiskRecordSize, crc, lfile, allowRemote, forceRemote));
            }
            keyManagers.append(*klManager.getClear());
        }
    }
    // IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();

        eoi = false;
        if (partDescs.ordinality())
        {
            if (keyMergerManager)
                setManager(keyMergerManager);
            else
                setManager(&keyManagers.item(0));
        }
        else
            eoi = true;
    }
    virtual void reset() override
    {
        PARENT::reset();
        currentKM = 0;
        clearManager();
    }
    void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        mb.append(progress);
        ForEachItemIn(s, _statsArr)
            mb.append(_statsArr.item(s));
    }
};

class CIndexReadSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexReadArg *helper;
    rowcount_t rowLimit = RCMAX, stopAfter = 0;
    bool keyedLimitSkips = false, first = false, needTransform = false, optimizeSteppedPostFilter = false, steppingEnabled = false;
    rowcount_t keyedLimit = RCMAX, helperKeyedLimit = RCMAX;
    rowcount_t keyedLimitCount = RCMAX;
    unsigned keyedProcessed = 0;
    ISteppingMeta *rawMeta;
    ISteppingMeta *projectedMeta;
    IInputSteppingMeta *inputStepping;
    IRangeCompare *stepCompare;
    IHThorSteppedSourceExtra *steppedExtra;
    CSteppingMeta steppingMeta;
    UnsignedArray seekSizes;

    const void *getNextRow()
    {
        RtlDynamicRowBuilder ret(allocator);
        for (;;)
        {
            const void *r = nextKey();
            if (!r)
                break;
            if (needTransform)
            {
                size32_t sz = helper->transform(ret, r);
                if (sz)
                {
                    callback.finishedRow();
                    return ret.finalizeRowClear(sz);
                }
            }
            else
            {
                size32_t sz = queryRowMetaData()->getRecordSize(r);
                memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                return ret.finalizeRowClear(sz);
            }
        }
        return nullptr;
    }
    virtual void setManager(IKeyManager *manager) override
    {
        PARENT::setManager(manager);
        if (stopAfter && !helper->transformMayFilter())
            manager->setChooseNLimit(stopAfter);
    }
    const void *nextKeyGE(const void *seek, unsigned numFields)
    {
        assertex(keyMergerManager.get());
        const byte *rawSeek = (const byte *)seek + seekGEOffset;
        unsigned seekSize = seekSizes.item(numFields-1);
        if (projectedMeta)
        {
            byte *temp = (byte *) alloca(seekSize);
            //GH: Is it overkill to use a builder as the target here??
            RtlStaticRowBuilder tempBuilder(temp - seekGEOffset, seekGEOffset+seekSize);
            helper->mapOutputToInput(tempBuilder, seek, numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
            rawSeek = (byte *)temp;
        }
        if (!currentManager->lookupSkip(rawSeek, seekGEOffset, seekSize))
            return NULL;
        noteStats(currentManager->querySeeks(), currentManager->queryScans());
        const byte *row = currentManager->queryKeyBuffer(callback.getFPosRef());
#ifdef _DEBUG
        if (memcmp(row + seekGEOffset, rawSeek, seekSize) < 0)
            assertex("smart seek failure");
#endif
        if (keyed())
        {
            eoi = true;
            return NULL;
        }
        return row;
    }
    const void *getNextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        RtlDynamicRowBuilder ret(allocator);
        size32_t seekSize = seekSizes.item(numFields-1);
        for (;;)
        {
            const void *r = nextKeyGE(seek, numFields);
            if (!r)
                break;
#ifdef _DEBUG
            if (seek && memcmp((byte *)r + seekGEOffset, seek, seekSize) < 0)
                assertex(!"smart seek failure");
#endif
            if (needTransform)
            {
                size32_t sz = helper->transform(ret, r);
                if (sz)
                {
                    callback.finishedRow();
                    return ret.finalizeRowClear(sz);
                }
                else
                {
                    if (optimizeSteppedPostFilter && stepExtra.returnMismatches())
                    {
                        if (memcmp(ret.getSelf() + seekGEOffset, seek, seekSize) != 0)
                        {
                            size32_t sz = helper->unfilteredTransform(ret, r);
                            if (sz)
                            {
                                wasCompleteMatch = false;
                                callback.finishedRow();
                                return ret.finalizeRowClear(sz);
                            }
                        }
                    }
                }
            }
            else
            {
                size32_t sz = queryRowMetaData()->getRecordSize(r);
                memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                return ret.finalizeRowClear(sz);
            }
        }
        return nullptr;
    }

    const void *checkLimit(bool &limitHit)
    {
        limitHit = false;
        if (RCMAX != keyedLimitCount)
        {
            if (keyedLimitCount <= keyedLimit)
                keyedLimitCount = sendGetCount(keyedLimitCount);
            else if (!container.queryLocalOrGrouped())
                sendPartialCount(*this, keyedLimitCount);
            OwnedConstThorRow ret;
            if (keyedLimitCount > keyedLimit)
            {
                limitHit = true;
                eoi = true;
                if (container.queryLocalOrGrouped() || firstNode())
                {
                    if (0 == (TIRkeyedlimitskips & helper->getFlags()))
                    {
                        if (0 != (TIRkeyedlimitcreates & helper->getFlags()))
                            ret.setown(createKeyedLimitOnFailRow());
                        else
                            helper->onKeyedLimitExceeded(); // should throw exception
                    }
                }
            }
            keyedLimit = RCMAX; // don't check again [ during get() / stop() ]
            keyedLimitCount = RCMAX;
            if (ret.get())
            {
                dataLinkIncrement();
                return ret.getClear();
            }
        }
        return NULL;
    }

public:
    CIndexReadSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexReadArg *)queryContainer().queryHelper();
        rawMeta = helper->queryRawSteppingMeta();
        projectedMeta = helper->queryProjectedSteppingMeta();
        steppedExtra = static_cast<IHThorSteppedSourceExtra *>(helper->selectInterface(TAIsteppedsourceextra_1));
        optimizeSteppedPostFilter = (helper->getFlags() & TIRunfilteredtransform) != 0;
        steppingEnabled = 0 != container.queryJob().getWorkUnitValueInt("steppingEnabled", 0);
        if (rawMeta)
        {
            //should check that no translation, also should check all keys in maxFields list can actually be keyed.
            const CFieldOffsetSize * fields = rawMeta->queryFields();
            unsigned maxFields = rawMeta->getNumFields();
            seekGEOffset = fields[0].offset;
            seekSizes.ensure(maxFields);
            seekSizes.append(fields[0].size);
            for (unsigned i=1; i < maxFields; i++)
                seekSizes.append(seekSizes.item(i-1) + fields[i].size);
        }
        appendOutputLinked(this);
    }
    virtual bool keyed()
    {
        ++keyedProcessed;
        if (keyedLimit == RCMAX)
            return false;
        // NB - this is only checking if local limit exceeded (skip case previously checked)
        if (keyedProcessed > keyedLimit)
        {
            helper->onKeyedLimitExceeded(); // should throw exception
            return true;
        }
        return false;
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        CIndexReadSlaveBase::init(data, slaveData);

        if (rawMeta)
        {
            bool hasPostFilter = helper->transformMayFilter() && optimizeSteppedPostFilter;
            if (projectedMeta)
                steppingMeta.init(projectedMeta, hasPostFilter);
            else
                steppingMeta.init(rawMeta, hasPostFilter);
        }
        if (keyIndexSet)
            keyMergerManager.setown(createKeyMerger(keyIndexSet, fixedDiskRecordSize, seekGEOffset, NULL));
    }

// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        stopAfter = (rowcount_t)helper->getChooseNLimit();

        PARENT::start();

        needTransform = helper->needTransform();
        helperKeyedLimit = (rowcount_t)helper->getKeyedLimit();
        rowLimit = (rowcount_t)helper->getRowLimit(); // MORE - if no filtering going on could keyspan to get count
        if (0 != (TIRlimitskips & helper->getFlags()))
            rowLimit = RCMAX;
        if (!helper->canMatchAny())
            helperKeyedLimit = keyedLimit = RCMAX; // disable
        else if (keyedLimit != RCMAX)
        {
            if (TIRkeyedlimitskips & helper->getFlags())
                keyedLimitSkips = true;
        }

        first = true;
        keyedLimit = helperKeyedLimit;
        keyedLimitCount = RCMAX;
        keyedProcessed = 0;
        if (steppedExtra)
            steppingMeta.setExtra(steppedExtra);

        if (currentManager)
        {
            if ((keyedLimit != RCMAX && (keyedLimitSkips || (helper->getFlags() & TIRcountkeyedlimit) != 0)))
            {
                IKeyManager *_currentManager = currentManager;
                keyedLimitCount = getCount(keyedLimit, true);
                setManager(_currentManager);
            }
        }
        else if ((keyedLimit != RCMAX && (keyedLimitSkips || (helper->getFlags() & TIRcountkeyedlimit) != 0)))
        {
            eoi = false;
            keyedLimitCount = 0;
        }
        else
            eoi = true;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }

// IRowStream
    virtual void stop() override
    {
        if (RCMAX != keyedLimit)
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        clearManager();
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi) 
            return nullptr;
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            OwnedConstThorRow limitRow = checkLimit(limitHit);
            if (limitHit)
            {
                eoi = true;
                return limitRow.getClear();
            }
            if (0 == partDescs.ordinality())
            {
                eoi = true;
                return nullptr;
            }
        }
        OwnedConstThorRow row = getNextRow();
        if (row)
        {
            ++progress;
            if (getDataLinkCount() >= rowLimit)
            {
                helper->onLimitExceeded(); // should throw exception
                eoi = true;
                return nullptr;
            }
            dataLinkIncrement();
            return row.getClear();
        }
        eoi = true;
        return nullptr;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) override
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        if (eoi) 
            return nullptr;
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            OwnedConstThorRow limitRow = checkLimit(limitHit);
            if (limitHit)
                return limitRow.getClear();
        }
        OwnedConstThorRow row = getNextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (row)
        {
            ++progress;
            if (getDataLinkCount() >= rowLimit)
            {
                helper->onLimitExceeded(); // should throw exception
                eoi = true;
                return NULL;
            }
            dataLinkIncrement();
            return row.getClear();
        }
        eoi = true;
        return nullptr;
    }
    IInputSteppingMeta *querySteppingMeta()
    {
        if (rawMeta && steppingEnabled && (0 == (helper->getFlags() & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates))))
            return &steppingMeta;
        return nullptr;
    }

friend class CIndexReadHelper;
};

CActivityBase *createIndexReadSlave(CGraphElementBase *container)
{
    return new CIndexReadSlaveActivity(container);
}


/////////////////////////////////////////////////////////////

class CIndexGroupAggregateSlaveActivity : public CIndexReadSlaveBase, implements IHThorGroupAggregateCallback
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexGroupAggregateArg *helper;
    bool gathered, merging;
    Owned<IAggregateTable> localAggTable;
    Owned<IRowStream> aggregateStream;
    memsize_t maxMem;
    Owned<IHashDistributor> distributor;
    bool done = false;

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CIndexGroupAggregateSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexGroupAggregateArg *)container.queryHelper();
        merging = false;
        appendOutputLinked(this);
    }
// IHThorGroupAggregateCallback
    virtual void processRow(const void *next)
    {
        localAggTable->addRow(next);
    }
// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
        localAggTable.setown(createRowAggregator(*this, *helper, *helper));
        localAggTable->init(queryRowAllocator());
        gathered = false;
        done = false;
    }
// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (done)
            return NULL;
        if (!gathered)
        {
            gathered = true;
            try
            {
                ForEachItemIn(p, partDescs)
                {
                    IKeyManager &keyManager = keyManagers.item(p);
                    setManager(&keyManager);
                    while (keyManager.lookup(true))
                    {
                        ++progress;
                        noteStats(keyManager.querySeeks(), keyManager.queryScans());
                        helper->processRow(keyManager.queryKeyBuffer(callback.getFPosRef()), this);
                        callback.finishedRow();
                    }
                    clearManager();
                }
                ActPrintLog("INDEXGROUPAGGREGATE: Local aggregate table contains %d entries", localAggTable->elementCount());
                if (!container.queryLocal() && container.queryJob().querySlaves()>1)
                {
                    Owned<IRowStream> localAggStream = localAggTable->getRowStream(true);
                    BooleanOnOff tf(merging);
                    aggregateStream.setown(mergeLocalAggs(distributor, *this, *helper, *helper, localAggStream, mpTag));
                }
                else
                    aggregateStream.setown(localAggTable->getRowStream(false));
            }
            catch (IException *e)
            {
                if (!isOOMException(e))
                    throw e;
                throw checkAndCreateOOMContextException(this, e, "aggregating using hash table", localAggTable->elementCount(), helper->queryDiskRecordSize(), NULL);
            }
        }
        const void *next = aggregateStream->nextRow();
        if (next)
        {
            dataLinkIncrement();
            return next;
        }
        done = true;
        return NULL;
    }
    virtual void abort()
    {
        CIndexReadSlaveBase::abort();
        if (merging)
            queryJobChannel().queryJobComm().cancel(0, mpTag);
    }
};

CActivityBase *createIndexGroupAggregateSlave(CGraphElementBase *container) { return new CIndexGroupAggregateSlaveActivity(container); }


/////////////////////////////////////////////////////////////


class CIndexCountSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexCountArg *helper;
    rowcount_t choosenLimit = 0;
    rowcount_t preknownTotalCount = 0;
    bool totalCountKnown = false;
    bool done = false;

public:
    CIndexCountSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = static_cast <IHThorIndexCountArg *> (container.queryHelper());
        appendOutputLinked(this);
    }
    virtual void setManager(IKeyManager *manager) override
    {
        PARENT::setManager(manager);
        if (choosenLimit && !helper->hasFilter())
            manager->setChooseNLimit(choosenLimit);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        choosenLimit = (rowcount_t)helper->getChooseNLimit();
        PARENT::start();
        if (!helper->canMatchAny())
        {
            totalCountKnown = true;
            preknownTotalCount = 0;
        }
        done = false;
    }

// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (done)
            return NULL;
        done = true;
        rowcount_t totalCount = 0;
        if (totalCountKnown)
        {
            totalCount = preknownTotalCount;
            if (!container.queryLocalOrGrouped() && !firstNode())
                return NULL;
        }
        else
        {
            if (helper->hasFilter())
            {
                ForEachItemIn(p, partDescs)
                {
                    IKeyManager &keyManager = keyManagers.item(p);
                    setManager(&keyManager);
                    for (;;)
                    {
                        bool l = keyManager.lookup(true);
                        noteStats(keyManager.querySeeks(), keyManager.queryScans());
                        if (!l)
                            break;
                        ++progress;
                        totalCount += helper->numValid(keyManager.queryKeyBuffer(callback.getFPosRef()));
                        callback.finishedRow();
                        if ((totalCount > choosenLimit))
                            break;
                    }
                    clearManager();
                    if ((totalCount > choosenLimit))
                        break;
                }
            }
            else
                totalCount = getCount(choosenLimit, false);
            if (!container.queryLocalOrGrouped())
            {
                sendPartialCount(*this, totalCount);
                if (!firstNode())
                    return NULL;
                totalCount = getFinalCount(*this);
            }
            if (totalCount > choosenLimit)
                totalCount = choosenLimit;
        }
        RtlDynamicRowBuilder result(allocator);
        // these should serialize using helper function?
        size32_t orsz = queryRowMetaData()->getMinRecordSize();
        size32_t sz;
        if (1 == orsz)
        {
            assertex(choosenLimit <= 255);
            byte *dst1 = (byte *)result.ensureCapacity(sizeof(byte),NULL);
            *dst1 = (byte)totalCount;
            sz = sizeof(byte);
        }
        else
        {
            assertex(sizeof(unsigned __int64) == orsz);
            unsigned __int64 *dst8 = (unsigned __int64 *)result.ensureCapacity(sizeof(unsigned __int64),NULL);
            *dst8 = (unsigned __int64)totalCount;
            sz = sizeof(unsigned __int64);
        }
        dataLinkIncrement();
        return result.finalizeRowClear(sz);     
    }
    virtual void abort()
    {
        CIndexReadSlaveBase::abort();
        cancelReceiveMsg(0, mpTag);
    }
};

CActivityBase *createIndexCountSlave(CGraphElementBase *container)
{
    return new CIndexCountSlaveActivity(container);
}


class CIndexNormalizeSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    bool expanding = false;
    IHThorIndexNormalizeArg *helper;
    rowcount_t keyedLimit = RCMAX, rowLimit = RCMAX, stopAfter = 0, keyedProcessed = 0, keyedLimitCount = RCMAX;

    const void * createNextRow()
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t sz = helper->transform(row);
        if (sz==0)
            return NULL;
        if (getDataLinkCount() >= rowLimit)
        {
            helper->onLimitExceeded();
            return NULL;
        }
        dataLinkIncrement();
        return row.finalizeRowClear(sz);
    }

public:
    CIndexNormalizeSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexNormalizeArg *)container.queryHelper();
        appendOutputLinked(this);
    }

    virtual bool keyed() override
    {
        ++keyedProcessed;
        if (keyedLimit == RCMAX)
            return false;
        // NB - this is only checking if local limit exceeded (skip case previously checked)
        if (keyedProcessed > keyedLimit)
        {
            helper->onKeyedLimitExceeded(); // should throw exception
            return true;
        }
        return false;
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        PARENT::start();
        keyedLimit = (rowcount_t)helper->getKeyedLimit();
        rowLimit = (rowcount_t)helper->getRowLimit();
        if (helper->getFlags() & TIRlimitskips)
            rowLimit = RCMAX;
        expanding = false;
        keyedProcessed = 0;
        keyedLimitCount = RCMAX;
        if (keyedLimit != RCMAX && (helper->getFlags() & TIRcountkeyedlimit) != 0)
            keyedLimitCount = getCount(keyedLimit, true);
    }

// IRowStream
    virtual void stop() override
    {
        if (RCMAX != keyedLimit)
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi)
            return nullptr;

        rowcount_t c = getDataLinkCount();
        if ((stopAfter && c==stopAfter)) // NB: only slave limiter, global performed in chained choosen activity
            return nullptr;

        if (RCMAX != keyedLimitCount)
        {
            if (keyedLimitCount <= keyedLimit)
                keyedLimitCount = sendGetCount(keyedLimitCount);
            else if (!container.queryLocalOrGrouped())
                sendPartialCount(*this, keyedLimitCount);
            if (keyedLimitCount > keyedLimit)
            {
                eoi = true;
                keyedLimit = RCMAX; // don't check again [ during get() / stop() ]
                keyedLimitCount = RCMAX;
                if (0 == (TIRkeyedlimitskips & helper->getFlags()))
                {
                    if (0 != (TIRkeyedlimitcreates & helper->getFlags()))
                    {
                        eoi = true;
                        OwnedConstThorRow row = createKeyedLimitOnFailRow();
                        dataLinkIncrement();
                        return row.getClear();
                    }
                    else
                        helper->onKeyedLimitExceeded(); // should throw exception
                }
                return nullptr;
            }
            keyedLimit = RCMAX; // don't check again [ during get() / stop() ]
            keyedLimitCount = RCMAX;
        }

        for (;;)
        {
            if (expanding)
            {
                for (;;)
                {
                    expanding = helper->next();
                    if (!expanding)
                        break;

                    OwnedConstThorRow row = createNextRow();
                    if (row)
                        return row.getClear();
                }
            }

            for (;;)
            {
                callback.finishedRow();
                const void *rec = nextKey();
                if (rec)
                {
                    ++progress;
                    expanding = helper->first(rec);
                    if (expanding)
                    {
                        OwnedConstThorRow row = createNextRow();
                        if (row)
                            return row.getClear();
                        break;
                    }
                }
                else
                {
                    eoi = true;
                    return nullptr;
                }
            }
        }
    }
};

CActivityBase *createIndexNormalizeSlave(CGraphElementBase *container) { return new CIndexNormalizeSlaveActivity(container); }

class CIndexAggregateSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    bool hadElement = false;
    bool done = false;
    IHThorIndexAggregateArg *helper;
    CPartialResultAggregator aggregator;

public:
    CIndexAggregateSlaveActivity(CGraphElementBase *_container) 
        : CIndexReadSlaveBase(_container), aggregator(*this)
    {
        helper = (IHThorIndexAggregateArg *)container.queryHelper();
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.isSource = true;
        info.totalRowsMin = 0;
        info.totalRowsMax = 1;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
        hadElement = false;
        done = false;
    }

// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (done)
            return nullptr;
        done = true;

        RtlDynamicRowBuilder row(allocator);
        helper->clearAggregate(row);
        for (;;)
        {
            const void *r = nextKey();
            if (!r)
                break;
            hadElement = true;
            helper->processRow(row, r);
            callback.finishedRow();
        }
        if (container.queryLocalOrGrouped())
        {
            dataLinkIncrement();
            size32_t sz = allocator->queryOutputMeta()->getRecordSize(row.getSelf());
            return row.finalizeRowClear(sz);
        }
        else
        {
            OwnedConstThorRow ret;
            if (hadElement)
            {
                size32_t sz = allocator->queryOutputMeta()->getRecordSize(row.getSelf());
                ret.setown(row.finalizeRowClear(sz));
            }
            aggregator.sendResult(ret.get());
            if (firstNode())
            {
                ret.setown(aggregator.getResult());
                if (ret)
                {
                    dataLinkIncrement();
                    return ret.getClear();
                }
            }
            return nullptr;
        }
    }  
};

CActivityBase *createIndexAggregateSlave(CGraphElementBase *container) { return new CIndexAggregateSlaveActivity(container); }

