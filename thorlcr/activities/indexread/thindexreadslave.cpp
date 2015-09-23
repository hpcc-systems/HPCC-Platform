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

static IKeyManager *getKeyManager(IKeyIndex *keyIndex, IHThorIndexReadBaseArg *helper, size32_t fixedDiskRecordSize)
{
    Owned<IKeyManager> klManager = createKeyManager(keyIndex, fixedDiskRecordSize, NULL);
    helper->createSegmentMonitors(klManager);
    klManager->finishSegmentMonitors();
    klManager->reset();
    return klManager.getClear();
}

static IKeyIndex *openKeyPart(CActivityBase *activity, IPartDescriptor &partDesc)
{
    RemoteFilename rfn;
    partDesc.getFilename(0, rfn);
    StringBuffer filePath;
    rfn.getPath(filePath);
    unsigned crc=0;
    partDesc.getCrc(crc);
    Owned<IDelayedFile> lfile = queryThor().queryFileCache().lookup(*activity, partDesc);
    return createKeyIndex(filePath.str(), crc, *lfile, false, false);
}



class CIndexReadSlaveBase;
class CIndexPartHandlerHelper
{
protected:
    CIndexReadSlaveBase &activity;
    Linked<IPartDescriptor> partDesc;
    Owned<IKeyIndex> keyIndex;
    Owned<IKeyManager> klManager;
    MemoryAttr keybuf;
    size32_t recSize;
    bool eoi;

public:
    CIndexPartHandlerHelper(CIndexReadSlaveBase &_activity);
    rowcount_t getCount(const rowcount_t &keyedLimit);
    const void *nextKey();  // NB not link counted row
    const void *getKeyPtr(); // nor this

    void setPart(IPartDescriptor *_partDesc, unsigned partNoSerialized);

    void reset();

friend class CIndexReadSlaveBase;
};

enum AdditionStats { AS_Seeks, AS_Scans };
class CIndexReadSlaveBase : public CSlaveActivity
{
protected:
    StringAttr logicalFilename;
    IArrayOf<IPartDescriptor> partDescs;
    IHThorIndexReadBaseArg *helper;
    IHThorSourceLimitTransformExtra * limitTransformExtra;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IOutputRowSerializer> serializer;
    bool localKey;
    __int64 lastSeeks, lastScans;
    UInt64Array _statsArr;
    SpinLock statLock;
    unsigned __int64 *statsArr;
    size32_t fixedDiskRecordSize;
    rowcount_t progress;

    class TransformCallback : public CSimpleInterface, implements IThorIndexCallback 
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
    void setManager(IKeyManager *manager)
    {
        callback.setManager(manager);
        resetLastStats();
    }
    const void *createKeyedLimitOnFailRow()
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t sz = limitTransformExtra->transformOnKeyedLimitExceeded(row);
        if (sz)
            return row.finalizeRowClear(sz);
        return NULL;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexReadSlaveBase(CGraphElementBase *container) 
        : CSlaveActivity(container)
    {
        helper = (IHThorIndexReadBaseArg *)container->queryHelper();
        localKey = false;
        fixedDiskRecordSize = helper->queryDiskRecordSize()->querySerializedDiskMeta()->getFixedSize(); // 0 if variable and unused
        progress = 0;
        reInit = 0 != (helper->getFlags() & (TIRvarfilename|TIRdynamicfilename));
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
    inline void incScan()
    {
        SpinBlock b(statLock);
        lastScans++;
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
        limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(helper->selectInterface(TAIsourcelimittransformextra_1));
        helper->setCallback(&callback);
        _statsArr.append(0);
        _statsArr.append(0);
        statsArr = _statsArr.getArray();
        lastSeeks = lastScans = 0;
    }
    void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        mb.append(progress);
        ForEachItemIn(s, _statsArr)
            mb.append(_statsArr.item(s));
    }

friend class CIndexPartHandlerHelper;
};

CIndexPartHandlerHelper::CIndexPartHandlerHelper(CIndexReadSlaveBase &_activity) 
    : activity(_activity)
{
    eoi = false;
    recSize = 0;
}

rowcount_t CIndexPartHandlerHelper::getCount(const rowcount_t &keyedLimit)
{
    assertex(partDesc);
    unsigned __int64 count = klManager->checkCount(keyedLimit);
    assertex(count == (rowcount_t)count);
    activity.noteStats(klManager->querySeeks(), klManager->queryScans());
    klManager->reset();
    activity.resetLastStats();
    return (rowcount_t)count;
}

void CIndexPartHandlerHelper::setPart(IPartDescriptor *_partDesc, unsigned partNoSerialized)
{
    reset();
    partDesc.set(_partDesc);
    keyIndex.setown(openKeyPart(&activity, *partDesc));
    recSize = keyIndex->keySize(); 
    klManager.setown(getKeyManager(keyIndex, activity.helper, activity.fixedDiskRecordSize));
    activity.setManager(klManager);
}

const void *CIndexPartHandlerHelper::nextKey()
{
    if (klManager->lookup(true))
    {
        activity.noteStats(klManager->querySeeks(), klManager->queryScans());
        return (const void *)klManager->queryKeyBuffer(activity.callback.getFPosRef());
    }
    return NULL;
}

const void *CIndexPartHandlerHelper::getKeyPtr()
{
    if (eoi) 
        return NULL; 
    const void *r = nextKey();
    if (NULL == r || activity.keyed())
        return NULL;
    return r;
}

void CIndexPartHandlerHelper::reset()
{
    activity.callback.clearManager();
    if (klManager.get())
    {
        klManager->releaseSegmentMonitors();
        klManager.clear();
    }
    keyIndex.clear();
    partDesc.clear();
}


interface IRowStreamStepping : extends IRowStream
{
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) = 0;
};

class CIndexReadSlaveActivity : public CIndexReadSlaveBase, public CThorDataLink
{
    IHThorIndexReadArg *helper;
    rowcount_t rowLimit, stopAfter;
    bool keyedLimitSkips, first, eoi, needTransform, optimizeSteppedPostFilter, steppingEnabled;
    Owned<IRowStreamStepping> out;
    rowcount_t keyedLimit, helperKeyedLimit;
    rowcount_t keyedLimitCount;
    unsigned keyedProcessed;
    ISteppingMeta *rawMeta;
    ISteppingMeta *projectedMeta;
    IInputSteppingMeta *inputStepping;
    IRangeCompare *stepCompare;
    IHThorSteppedSourceExtra *steppedExtra;
    CSteppingMeta steppingMeta;
    size32_t seekGEOffset;
    UnsignedArray seekSizes;

    class CIndexReadHelper : public CSimpleInterface, implements IRowStreamStepping
    {
        CIndexReadSlaveActivity &activity;
    protected:
        Owned<IKeyManager> klManager;
        MemoryAttr keybuf;

        unsigned currentPart;
        bool merger;
        size32_t recSize;
        bool eoi;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CIndexReadHelper(CIndexReadSlaveActivity &_activity) : activity(_activity)
        {
            eoi = false;
            recSize = 0;
            currentPart = 0;
            merger = false;
            init();
        }
        void init()
        {
            Owned<IKeyIndex> keyIndex;
            if (activity.seekGEOffset || activity.localKey)
            {
                Owned<IKeyIndexSet> keyIndexSet = createKeyIndexSet();
                ForEachItemIn(p, activity.partDescs)
                {
                    keyIndex.setown(openKeyPart(&activity, activity.partDescs.item(p)));
                    keyIndexSet->addIndex(keyIndex.getClear());
                }
                klManager.setown(createKeyMerger(keyIndexSet, activity.fixedDiskRecordSize, activity.seekGEOffset, NULL));
                merger = true;
            }
            else
            {
                keyIndex.setown(openKeyPart(&activity, activity.partDescs.item(0)));
                klManager.setown(createKeyManager(keyIndex, activity.fixedDiskRecordSize, NULL));
            }
            activity.helper->createSegmentMonitors(klManager);
            klManager->finishSegmentMonitors();
            klManager->reset();
            activity.setManager(klManager);
            activity.resetLastStats();
        }
        rowcount_t getCount(const rowcount_t &keyedLimit)
        {
            unsigned __int64 count = 0;
            // Note - don't use merger's count - it doesn't work
            ForEachItemIn(p, activity.partDescs)
            {
                activity.callback.clearManager();
                klManager->releaseSegmentMonitors();
                Owned<IKeyIndex> keyIndex = openKeyPart(&activity, activity.partDescs.item(p));
                klManager.setown(getKeyManager(keyIndex, activity.helper, activity.fixedDiskRecordSize));
                activity.setManager(klManager);
                count += klManager->checkCount(keyedLimit-count); // part max, is total limit [keyedLimit] minus total so far [count]
                activity.noteStats(klManager->querySeeks(), klManager->queryScans());
                if (count > keyedLimit)
                    break;
            }
            activity.callback.clearManager();
            klManager->releaseSegmentMonitors();
            init();
            return (rowcount_t)count;
        }
        const void *nextKey()
        {
            if (eoi) 
                return NULL;
            const void *ret = NULL;
            loop
            {
                if (klManager->lookup(true))
                {
                    activity.noteStats(klManager->querySeeks(), klManager->queryScans());
                    ret = (const void *)klManager->queryKeyBuffer(activity.callback.getFPosRef());
                }
                if (ret || merger)
                    break;
                ++currentPart;
                if (currentPart >= activity.partDescs.ordinality())
                    break;
                else
                {
                    activity.callback.clearManager();
                    klManager->releaseSegmentMonitors();
                    Owned<IKeyIndex> keyIndex = openKeyPart(&activity, activity.partDescs.item(currentPart));
                    klManager.setown(getKeyManager(keyIndex, activity.helper, activity.fixedDiskRecordSize));
                    activity.setManager(klManager);
                }
            }
            if (NULL == ret || activity.keyed())
            {
                eoi = true;
                return NULL;
            }
            return ret;
        }
        const void *nextKeyGE(const void *seek, unsigned numFields)
        {
            assertex(merger);
            const byte *rawSeek = (const byte *)seek + activity.seekGEOffset;
            unsigned seekSize = activity.seekSizes.item(numFields-1);
            if (activity.projectedMeta)
            {
                byte *temp = (byte *) alloca(seekSize);
                //GH: Is it overkill to use a builder as the target here??
                RtlStaticRowBuilder tempBuilder(temp - activity.seekGEOffset, activity.seekGEOffset+seekSize);
                activity.helper->mapOutputToInput(tempBuilder, seek, numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
                rawSeek = (byte *)temp;
            }
            if (!klManager->lookupSkip(rawSeek, activity.seekGEOffset, seekSize))
                return NULL;
            activity.noteStats(klManager->querySeeks(), klManager->queryScans());
            const byte *row = klManager->queryKeyBuffer(activity.callback.getFPosRef());
#ifdef _DEBUG
            if (memcmp(row + activity.seekGEOffset, rawSeek, seekSize) < 0)
                assertex("smart seek failure");
#endif
            if (activity.keyed())
            {
                eoi = true;
                return NULL;
            }
            return row;
        }
// IRowStreamStepping impl.
        virtual void stop()
        {
            klManager.clear();
        }
        virtual const void *nextRow()
        {
            RtlDynamicRowBuilder ret(activity.allocator);
            loop
            {
                const void *r = nextKey();
                if (!r)
                    break;
                if (activity.needTransform)
                {
                    size32_t sz = activity.helper->transform(ret, r);
                    if (sz)
                    {
                        activity.callback.finishedRow();
                        return ret.finalizeRowClear(sz);
                    }
                }
                else
                {
                    size32_t sz = activity.queryRowMetaData()->getRecordSize(r);
                    memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                    return ret.finalizeRowClear(sz);
                }
            }
            return NULL;
        }
        virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
        {
            RtlDynamicRowBuilder ret(activity.allocator);
            size32_t seekSize = activity.seekSizes.item(numFields-1);
            loop
            {
                const void *r = nextKeyGE(seek, numFields);
                if (!r)
                    break;
#ifdef _DEBUG
                if (seek && memcmp((byte *)r + activity.seekGEOffset, seek, seekSize) < 0)
                    assertex(!"smart seek failure");
#endif
                if (activity.needTransform)
                {
                    size32_t sz = activity.helper->transform(ret, r);
                    if (sz)
                    {
                        activity.callback.finishedRow();
                        return ret.finalizeRowClear(sz);
                    }
                    else
                    {
                        if (activity.optimizeSteppedPostFilter && stepExtra.returnMismatches())
                        {
                            if (memcmp(ret.getSelf() + activity.seekGEOffset, seek, seekSize) != 0)
                            {
                                size32_t sz = activity.helper->unfilteredTransform(ret, r);
                                if (sz)
                                {
                                    wasCompleteMatch = false;
                                    activity.callback.finishedRow();
                                    return ret.finalizeRowClear(sz);
                                }
                            }
                        }
                    }
                }
                else
                {
                    size32_t sz = activity.queryRowMetaData()->getRecordSize(r);
                    memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                    return ret.finalizeRowClear(sz);
                }
            }
            return NULL;
        }
    };
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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexReadSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container), CThorDataLink(this)
    {
        keyedLimitSkips = false;
        first = true;
        eoi = false;
        helperKeyedLimit = keyedLimit = RCMAX;
        keyedLimitCount = RCMAX;
        keyedProcessed = 0;
        helper = (IHThorIndexReadArg *)queryContainer().queryHelper();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        needTransform = helper->needTransform();
        rawMeta = helper->queryRawSteppingMeta();
        projectedMeta = helper->queryProjectedSteppingMeta();
        steppedExtra = static_cast<IHThorSteppedSourceExtra *>(helper->selectInterface(TAIsteppedsourceextra_1));
        optimizeSteppedPostFilter = (helper->getFlags() & TIRunfilteredtransform) != 0;
        seekGEOffset = 0;
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
            bool hasPostFilter = helper->transformMayFilter() && optimizeSteppedPostFilter;
            if (projectedMeta)
                steppingMeta.init(projectedMeta, hasPostFilter);
            else
                steppingMeta.init(rawMeta, hasPostFilter);
        }
    }
    ~CIndexReadSlaveActivity()
    {
        out.clear();
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
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CIndexReadSlaveBase::init(data, slaveData);

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
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        first = true;
        eoi = false;
        keyedLimit = helperKeyedLimit;
        keyedLimitCount = RCMAX;
        keyedProcessed = 0;
        if (steppedExtra)
            steppingMeta.setExtra(steppedExtra);

        if (partDescs.ordinality())
        {
            Owned<CIndexReadHelper> indexReadHelper = new CIndexReadHelper(*this);
            if ((keyedLimit != RCMAX && (keyedLimitSkips || (helper->getFlags() & TIRcountkeyedlimit) != 0)))
                keyedLimitCount = indexReadHelper->getCount(keyedLimit);
            eoi = false;
            out.setown(indexReadHelper.getClear());
        }
        else if ((keyedLimit != RCMAX && (keyedLimitSkips || (helper->getFlags() & TIRcountkeyedlimit) != 0)))
            keyedLimitCount = 0;            
        else
            eoi = true; // otherwise delayed until calc. in nextRow()
        dataLinkStart();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() { return false; }

// IRowStream
    virtual void stop()
    {
        if (RCMAX != keyedLimit)
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        if (out)
        {
            callback.clearManager();
            out->stop();
            out.clear();
        }
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi) 
            return NULL;
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            OwnedConstThorRow limitRow = checkLimit(limitHit);
            if (limitHit)
            {
                eoi = true;
                return limitRow.getClear();
            }
            if (!out)
            {
                eoi = true;
                return NULL;
            }
        }
        OwnedConstThorRow row = out->nextRow();
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
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        if (eoi) 
            return NULL;
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            OwnedConstThorRow limitRow = checkLimit(limitHit);
            if (limitHit)
                return limitRow.getClear();
        }
        OwnedConstThorRow row = out->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
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
        return NULL;
    }
    IInputSteppingMeta *querySteppingMeta()
    {
        if (rawMeta && steppingEnabled && (0 == (helper->getFlags() & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates))))
            return &steppingMeta;
        return NULL;
    }
  
friend class CIndexReadHelper;
};

CActivityBase *createIndexReadSlave(CGraphElementBase *container)
{
    return new CIndexReadSlaveActivity(container);
}


/////////////////////////////////////////////////////////////

class CIndexGroupAggregateSlaveActivity : public CIndexReadSlaveBase, public CThorDataLink, implements IHThorGroupAggregateCallback
{
    IHThorIndexGroupAggregateArg *helper;
    bool gathered, eoi, merging;
    Owned<RowAggregator> localAggTable;
    memsize_t maxMem;
    Owned<IHashDistributor> distributor;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexGroupAggregateSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container), CThorDataLink(this)
    {
        helper = (IHThorIndexGroupAggregateArg *)container.queryHelper();
        merging = false;
    }
// IHThorGroupAggregateCallback
    virtual void processRow(const void *next)
    {
        localAggTable->addRow(next);
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CIndexReadSlaveBase::init(data, slaveData);
        appendOutputLinked(this);
    }
// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() { return false; }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        localAggTable.setown(new RowAggregator(*helper, *helper));
        localAggTable->start(queryRowAllocator());
        gathered = eoi = false;
        dataLinkStart();
    }
// IRowStream
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi) 
            return NULL;
        if (!gathered)
        {
            gathered = true;
            ForEachItemIn(p, partDescs)
            {
                IPartDescriptor &partDesc = partDescs.item(p);
                Owned<IKeyIndex> keyIndex = openKeyPart(this, partDesc);
                Owned<IKeyManager> klManager = getKeyManager(keyIndex, helper, fixedDiskRecordSize);
                setManager(klManager);
                while (klManager->lookup(true))
                {
                    ++progress;
                    noteStats(klManager->querySeeks(), klManager->queryScans());
                    helper->processRow(klManager->queryKeyBuffer(callback.getFPosRef()), this);
                    callback.finishedRow();
                }
                callback.clearManager();
            }
            ActPrintLog("INDEXGROUPAGGREGATE: Local aggregate table contains %d entries", localAggTable->elementCount());
            if (!container.queryLocal() && container.queryJob().querySlaves()>1)
            {
                BooleanOnOff tf(merging);
                bool ordered = 0 != (TDRorderedmerge & helper->getFlags());
                localAggTable.setown(mergeLocalAggs(distributor, *this, *helper, *helper, localAggTable, mpTag, ordered));
            }
        }       
        Owned<AggregateRowBuilder> next = localAggTable->nextResult();
        if (next)
        {
            dataLinkIncrement();
            return next->finalizeRowClear();
        }
        eoi = true;
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


class CIndexCountSlaveActivity : public CIndexReadSlaveBase, public CThorDataLink
{
    bool eoi;
    IHThorIndexCountArg *helper;
    rowcount_t choosenLimit;
    rowcount_t preknownTotalCount;
    bool totalCountKnown;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexCountSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container), CThorDataLink(this)
    {
        helper = static_cast <IHThorIndexCountArg *> (container.queryHelper());
        preknownTotalCount = 0;
        totalCountKnown = false;
        preknownTotalCount = 0;
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CIndexReadSlaveBase::init(data, slaveData);
        choosenLimit = (rowcount_t)helper->getChooseNLimit();
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() { return false; }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        eoi = false;
        if (!helper->canMatchAny())
        {
            totalCountKnown = true;
            preknownTotalCount = 0;
        }
        dataLinkStart();
    }

// IRowStream
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi)
            return NULL;
        rowcount_t totalCount = 0;
        eoi = true;
        if (totalCountKnown)
        {
            totalCount = preknownTotalCount;
            if (!container.queryLocalOrGrouped() && !firstNode())
                return NULL;
        }
        else
        {
            ForEachItemIn(p, partDescs)
            {
                IPartDescriptor &partDesc = partDescs.item(p);
                Owned<IKeyIndex> keyIndex = openKeyPart(this, partDesc);
                Owned<IKeyManager> klManager = getKeyManager(keyIndex, helper, fixedDiskRecordSize);
                setManager(klManager);
                if (helper->hasFilter())
                {
                    loop
                    {
                        bool l = klManager->lookup(true);
                        noteStats(klManager->querySeeks(), klManager->queryScans());
                        if (!l)
                            break;
                        ++progress;
                        totalCount += helper->numValid(klManager->queryKeyBuffer(callback.getFPosRef()));
                        callback.finishedRow();
                        if ((totalCount > choosenLimit))
                            break;
                    }
                }
                else
                    totalCount += (rowcount_t)klManager->getCount();
                callback.clearManager();
                if ((totalCount > choosenLimit))
                    break;
            }
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
            assertex(sizeof(byte) == choosenLimit);
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


class CIndexNormalizeSlaveActivity : public CIndexReadSlaveBase, public CThorDataLink
{
    bool eoi, expanding;
    IHThorIndexNormalizeArg *helper;
    rowcount_t keyedLimit, rowLimit, stopAfter, keyedProcessed, keyedLimitCount;
    unsigned currentPart;
    CIndexPartHandlerHelper partHelper;

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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexNormalizeSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container), CThorDataLink(this), partHelper(*this)
    {
        helper = (IHThorIndexNormalizeArg *)container.queryHelper();
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
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CIndexReadSlaveBase::init(data, slaveData);
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() { return false; }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        keyedLimit = (rowcount_t)helper->getKeyedLimit();
        rowLimit = (rowcount_t)helper->getRowLimit();
        if (helper->getFlags() & TIRlimitskips)
            rowLimit = RCMAX;
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        expanding = false;
        keyedProcessed = 0;
        keyedLimitCount = RCMAX;
        currentPart = 0;
        if (keyedLimit != RCMAX && (helper->getFlags() & TIRcountkeyedlimit) != 0)
        {
            keyedLimitCount = 0;
            ForEachItemIn(p, partDescs)
            {
                partHelper.setPart(&partDescs.item(p), p);
                keyedLimitCount += partHelper.getCount(keyedLimit);
                if (keyedLimitCount > keyedLimit) break;
            }
        }
        if (partDescs.ordinality())
        {
            eoi = false;
            partHelper.setPart(&partDescs.item(0), 0);
        }
        else
            eoi = true;
        dataLinkStart();
    }

// IRowStream
    virtual void stop()
    {
        if (RCMAX != keyedLimit)
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        dataLinkStop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi)
            return NULL;

        rowcount_t c = getDataLinkCount();
        if ((stopAfter && c==stopAfter)) // NB: only slave limiter, global performed in chained choosen activity
            return NULL;
            
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
                return NULL;
            }
            keyedLimit = RCMAX; // don't check again [ during get() / stop() ]
            keyedLimitCount = RCMAX;
        }

        loop
        {
            if (expanding)
            {
                loop
                {
                    expanding = helper->next();
                    if (!expanding)
                        break;

                    OwnedConstThorRow row = createNextRow();
                    if (row)
                        return row.getClear();
                }
            }

            loop
            {
                callback.finishedRow();
                const void *rec = partHelper.nextKey();
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
                    partHelper.reset();
                    ++currentPart;
                    if (currentPart >= partDescs.ordinality())
                    {
                        eoi = true;
                        return NULL;
                    }
                    partHelper.setPart(&partDescs.item(currentPart), currentPart);
                }
            }
        }
    }
};

CActivityBase *createIndexNormalizeSlave(CGraphElementBase *container) { return new CIndexNormalizeSlaveActivity(container); }

class CIndexAggregateSlaveActivity : public CIndexReadSlaveBase, public CThorDataLink
{
    bool eoi, hadElement;
    IHThorIndexAggregateArg *helper;
    CIndexPartHandlerHelper partHelper;
    unsigned partn;
    CPartialResultAggregator aggregator;

    const void *nextKey()
    {
        loop
        {
            const void *r = partHelper.nextKey();
            if (r)
            {
                ++progress;
                return r;
            }
            else
            {
                ++partn;
                if (partn>=partDescs.ordinality())
                    return NULL;
                partHelper.setPart(&partDescs.item(partn), partn);
            }
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIndexAggregateSlaveActivity(CGraphElementBase *_container) 
        : CIndexReadSlaveBase(_container), CThorDataLink(this), partHelper(*this), aggregator(*this)
    {
        helper = (IHThorIndexAggregateArg *)container.queryHelper();
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CIndexReadSlaveBase::init(data, slaveData);
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.totalRowsMin = 0;
        info.totalRowsMax = 1;
        // MORE TBD
    }
    virtual bool isGrouped() { return false; }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        eoi = hadElement = false;
        partn = 0;
        dataLinkStart();
    }

// IRowStream
    virtual void stop()
    {
        dataLinkStop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi)
            return NULL;
        eoi = true;

        RtlDynamicRowBuilder row(allocator);
        helper->clearAggregate(row);
        if (partDescs.ordinality())
        {
            partHelper.setPart(&partDescs.item(0), 0);
            loop
            {
                const void *r = nextKey();
                if (!r) 
                    break;
                hadElement = true;
                helper->processRow(row, r);
                callback.finishedRow();
            }
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
            return NULL;
        }
    }  
};

CActivityBase *createIndexAggregateSlave(CGraphElementBase *container) { return new CIndexAggregateSlaveActivity(container); }

