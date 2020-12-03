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

#include "platform.h"

#include "jfile.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsocket.hpp"
#include "jsort.hpp"
#include "jtime.hpp"

#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "rtlcommon.hpp"
#include "rtlkey.hpp"
#include "rtlrecord.hpp"
#include "eclhelper.hpp" // tmp for IHThor..Arg interfaces.

#include "rmtfile.hpp"
#include "rmtclient.hpp"

#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thsortu.hpp"
#include "thexception.hpp"
#include "thactivityutil.ipp"

#include "../hashdistrib/thhashdistribslave.ipp"
#include "thdiskreadslave.ipp"

#define ASYNC_BUFFER_SIZE   64 * 1024       // 64k

#define RECORD_BUFFER_SIZE  64 * 1024       // 64k

//////////////////////////////////////////////

class CDiskReadSlaveActivityRecord : public CDiskReadSlaveActivityBase, implements IIndexReadContext
{
protected:
    IArrayOf<IKeySegmentMonitor> segMonitors;
    IConstArrayOf<IFieldFilter> fieldFilters;
    bool grouped;
    bool isFixedDiskWidth;
    bool needTransform = false;
    bool hasMatchFilter = false;
    size32_t diskRowMinSz;
    unsigned numSegFieldsUsed = 0;
    rowcount_t totalProgress = 0;
    rowcount_t stopAfter = 0;
    rowcount_t remoteLimit = 0;
    rowcount_t limit = 0;

    // return a ITranslator based on published format in part and expected/format
    ITranslator *getTranslators(IPartDescriptor &partDesc)
    {
        RecordTranslationMode translationMode = getTranslationMode(*this);

        unsigned expectedFormatCrc = helper->getDiskFormatCrc();
        IOutputMetaData *expectedFormat = helper->queryDiskRecordSize();

        Linked<IOutputMetaData> publishedFormat;
        unsigned publishedFormatCrc = 0;

        const char *kind = queryFileKind(&partDesc.queryOwner());
        if (strisame(kind, "flat") || (RecordTranslationMode::AlwaysDisk == translationMode))
        {
            IPropertyTree const &props = partDesc.queryOwner().queryProperties();
            publishedFormat.setown(getDaliLayoutInfo(props));
            if (publishedFormat)
                publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
        }

        unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
        IOutputMetaData *projectedFormat = helper->queryProjectedDiskRecordSize();
        return ::getTranslators("rowstream", expectedFormatCrc, expectedFormat, publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode);
    }
public:
    CDiskReadSlaveActivityRecord(CGraphElementBase *_container, IHThorArg *_helper=NULL) 
        : CDiskReadSlaveActivityBase(_container, _helper)
    {
        helper = (IHThorDiskReadArg *)queryHelper();
        IOutputMetaData *diskRowMeta = helper->queryDiskRecordSize()->querySerializedDiskMeta();
        isFixedDiskWidth = diskRowMeta->isFixedSize();
        diskRowMinSz = diskRowMeta->getMinRecordSize();
        grouped = false;
    }

// IIndexReadContext impl.
    virtual void append(IKeySegmentMonitor *segment)
    {
        if (segment->isWild())
            segment->Release();
        else
        {
            segMonitors.append(*segment);
            if (segment->numFieldsRequired() > numSegFieldsUsed)
                numSegFieldsUsed = segment->numFieldsRequired();
        }
    }

    virtual void append(FFoption option, const IFieldFilter * filter)
    {
        if (filter->isWild())
            filter->Release();
        else
        {
            fieldFilters.append(*filter);
            if (filter->queryFieldIndex() > numSegFieldsUsed)
                numSegFieldsUsed = filter->queryFieldIndex();
        }
    }

    virtual void start()
    {
        CDiskReadSlaveActivityBase::start();
        fieldFilters.kill();
        segMonitors.kill();
        helper->createSegmentMonitors(this);
    }

    virtual void serializeStats(MemoryBuffer &mb) override
    {
        if (partHandler)
            diskProgress = totalProgress + partHandler->queryProgress();
        CDiskReadSlaveActivityBase::serializeStats(mb);
    }
friend class CDiskRecordPartHandler;
};

/////////////////////////////////////////////////

void mergeStats(CRuntimeStatisticCollection & stats, IExtRowStream * in)
{
    if (in)
    {
        ForEachItemIn(iStat, stats)
        {
            StatisticKind kind = stats.getKind(iStat);
            stats.mergeStatistic(kind, in->getStatistic(kind));
        }
    }
}

class CDiskRecordPartHandler : public CDiskPartHandlerBase
{
    Owned<IExtRowStream> in;
protected:
    CDiskReadSlaveActivityRecord &activity;
    RtlDynamicRowBuilder outBuilder;
public:
    CDiskRecordPartHandler(CDiskReadSlaveActivityRecord &activity);
    ~CDiskRecordPartHandler();
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) const override;
    virtual void open();
    virtual void close(CRC32 &fileCRC);
    offset_t getLocalOffset()
    {
        return in->getLastRowOffset();
    }
    inline const void *nextRow()
    {
        return in->nextRow();
    }
    inline const byte *prefetchRow()
    {
        return in->prefetchRow();
    }
    inline void prefetchDone()
    {
        in->prefetchDone();
    }
    virtual void gatherStats(CRuntimeStatisticCollection & merged)
    {
        CriticalBlock block(inputCs); // Ensure iFileIO remains valid for the duration of mergeStats()
        CDiskPartHandlerBase::gatherStats(merged);
        mergeStats(merged, in);
    }
    virtual unsigned __int64 queryProgress() override
    {
        CriticalBlock block(inputCs);
        if (in)
            return in->queryProgress();
        else
            return 0;
    }
};

/////////////////////////////////////////////////

CDiskRecordPartHandler::CDiskRecordPartHandler(CDiskReadSlaveActivityRecord &_activity) 
    : CDiskPartHandlerBase(_activity), activity(_activity), outBuilder(nullptr)
{
    outBuilder.setAllocator(activity.queryRowAllocator());
}

CDiskRecordPartHandler::~CDiskRecordPartHandler()
{
}



void CDiskRecordPartHandler::getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) const
{
    if (!partDesc->queryProperties().hasProp("@size"))
    {
        info.totalRowsMin = 0;          
        info.totalRowsMax = -1;
        info.unknownRowsOutput = true;
        return;
    }
    offset_t filesize = partDesc->queryProperties().getPropInt64("@size");
    if (activity.isFixedDiskWidth) 
    {
        if (compressed && !blockCompressed)
        {
            info.totalRowsMin = filesize/(activity.diskRowMinSz*2);
            info.totalRowsMax = filesize * 2; // ~ extreme case
        }
        else
        {
            info.totalRowsMin = filesize/activity.diskRowMinSz;
            info.totalRowsMax = info.totalRowsMin;
        }
    }
    else
    {
        info.totalRowsMin = filesize?1:0;  
        info.totalRowsMax = activity.diskRowMinSz?(filesize/activity.diskRowMinSz):filesize;
    }
}

void CDiskRecordPartHandler::open()
{
    // free last part and note progress
    Owned<IExtRowStream> partStream;
    {
        CriticalBlock block(inputCs);
        partStream.swap(in);
    }
    if (partStream)
        activity.totalProgress += partStream->queryProgress();
    partStream.clear();

    unsigned rwFlags = 0;
    if (checkFileCrc) // NB: if compressed, this will be turned off by base class
        rwFlags |= rw_crc;
    if (activity.grouped)
        rwFlags |= rw_grouped;

    IOutputMetaData *projectedFormat = activity.helper->queryProjectedDiskRecordSize();
    IOutputMetaData *expectedFormat = activity.helper->queryDiskRecordSize();
    Owned<ITranslator> translator = activity.getTranslators(*partDesc);
    IOutputMetaData *actualFormat = translator ? &translator->queryActualFormat() : expectedFormat;
    bool tryRemoteStream = actualFormat->queryTypeInfo()->canInterpret() && actualFormat->queryTypeInfo()->canSerialize() &&
                           projectedFormat->queryTypeInfo()->canInterpret() && projectedFormat->queryTypeInfo()->canSerialize();

    /* If part can potentially be remotely streamed, 1st check if any part is local,
     * then try to remote stream, and otherwise failover to legacy remote access
     */
    if (tryRemoteStream)
    {
        std::vector<unsigned> remoteCandidates;
        // scan for non remote candidate part 1st
        for (unsigned copy=0; copy<partDesc->numCopies(); copy++)
        {
            RemoteFilename rfn;
            partDesc->getFilename(copy, rfn);
            if (!isRemoteReadCandidate(activity, rfn))
            {
                StringBuffer path;
                rfn.getPath(path);
                Owned<IFile> iFile = createIFile(path);
                try
                {
                    if (iFile->exists())
                    {
                        remoteCandidates.clear();
                        break;
                    }
                }
                catch (IException *e)
                {
                    ActPrintLog(&activity, e, "CDiskRecordPartHandler::open()");
                    e->Release();
                }
            }
            else
                remoteCandidates.push_back(copy);
        }
        Owned<IException> remoteReadException;
        StringBuffer remoteReadExceptionPath;
        for (unsigned &copy: remoteCandidates) // only if no local part found above
        {
            RemoteFilename rfn;
            partDesc->getFilename(copy, rfn);

            StringBuffer path;
            rfn.getLocalPath(path);
            // Open a stream from remote file, having passed actual, expected, projected, and filters to it
            SocketEndpoint ep(rfn.queryEndpoint());
            setDafsEndpointPort(ep);

            RowFilter actualFilter;
            if (activity.fieldFilters.ordinality())
            {
                if (actualFormat != expectedFormat && translator->queryKeyedTranslator())
                    translator->queryKeyedTranslator()->translate(actualFilter, activity.fieldFilters);
                else
                    actualFilter.appendFilters(activity.fieldFilters);
            }
            Owned<IRemoteFileIO> iRemoteFileIO = createRemoteFilteredFile(ep, path, actualFormat, projectedFormat, actualFilter, compressed, activity.grouped, activity.remoteLimit);
            if (iRemoteFileIO)
            {
                StringBuffer tmp;
                iRemoteFileIO->addVirtualFieldMapping("logicalFilename", logicalFilename.get());
                iRemoteFileIO->addVirtualFieldMapping("baseFpos", tmp.clear().append(fileBaseOffset).str());
                iRemoteFileIO->addVirtualFieldMapping("partNum", tmp.clear().append(partDesc->queryPartIndex()).str());
                rfn.getPath(path.clear());
                filename.set(path);
                checkFileCrc = false;

                try
                {
                    iRemoteFileIO->ensureAvailable(); // force open now, because want to failover to other copies if fails (e.g. remote part is missing)
                }
                catch (IException *e)
                {
#ifdef _DEBUG
                    EXCLOG(e, nullptr);
#endif
                    if (remoteReadException)
                        e->Release(); // only record 1st
                    else
                    {
                        remoteReadException.setown(e);
                        remoteReadExceptionPath.set(filename);
                    }
                    continue; // try next copy and ultimately failover to local when no more copies
                }
                partStream.setown(createRowStreamEx(iRemoteFileIO, activity.queryProjectedDiskRowInterfaces(), 0, (offset_t)-1, (unsigned __int64)-1, rwFlags, nullptr, this));
                ActPrintLog(&activity, "%s[part=%d]: reading remote dafilesrv file '%s' (logical file = %s)", kindStr, which, filename.get(), activity.logicalFilename.get());
                break;
            }
        }
        if (remoteReadException)
        {
            VStringBuffer msg("Remote streaming failure, failing over to direct read for: '%s'. ", remoteReadExceptionPath.str());
            remoteReadException->errorMessage(msg);
            Owned<IThorException> e2 = MakeActivityWarning(&activity, TE_RemoteReadFailure, "%s", msg.str());
            activity.fireException(e2);
        }
    }

    // either local file was found, or no streamable remote parts
    if (!partStream)
    {
        CDiskPartHandlerBase::open(); // NB: base opens an IFile

        rwFlags |= DEFAULT_RWFLAGS;

        if (compressed)
        {
            rwFlags |= rw_compress;
            partStream.setown(createRowStream(iFile, activity.queryProjectedDiskRowInterfaces(), rwFlags, activity.eexp, translator, this));
            if (!partStream.get())
            {
                if (!blockCompressed)
                    throw MakeStringException(-1,"Unsupported compressed file format: %s", filename.get());
                else
                    throw MakeActivityException(&activity, 0, "Failed to open block compressed file '%s'", filename.get());
            }
        }
        else
            partStream.setown(createRowStream(iFile, activity.queryProjectedDiskRowInterfaces(), rwFlags, nullptr, translator, this));

        if (!partStream)
            throw MakeActivityException(&activity, 0, "Failed to open file '%s'", filename.get());
        ActPrintLog(&activity, "%s[part=%d]: %s (%s)", kindStr, which, activity.isFixedDiskWidth ? "fixed" : "variable", filename.get());
        partStream->setFilters(activity.fieldFilters);
    }

    {
        CriticalBlock block(inputCs);
        in.setown(partStream.getClear());
    }
}

void CDiskRecordPartHandler::close(CRC32 &fileCRC)
{
    Owned<IExtRowStream> partStream;
    {
        CriticalBlock block(inputCs);
        partStream.setown(in.getClear());
    }
    if (partStream)
    {
        partStream->stop(&fileCRC);
        mergeStats(fileStats, partStream);
    }
}

/////////////////////////////////////////////////

class CDiskReadSlaveActivity : public CDiskReadSlaveActivityRecord
{
    typedef CDiskReadSlaveActivityRecord PARENT;

    class CDiskPartHandler : public CDiskRecordPartHandler
    {
        CDiskReadSlaveActivity &activity;
public:
        CDiskPartHandler(CDiskReadSlaveActivity &_activity) 
            : CDiskRecordPartHandler(_activity), activity(_activity)
        {
        }
        virtual const void *nextRow()
        {
            if (!eoi && !activity.queryAbortSoon())
            {
                try
                {
                    if (activity.needTransform)
                    {
                        for (;;)
                        {
                            const byte *row = CDiskRecordPartHandler::prefetchRow();
                            if (!row)
                            {
                                if (!activity.grouped)
                                    break;
                                if (!firstInGroup)
                                {
                                    firstInGroup = true;
                                    return nullptr;
                                }
                                row = CDiskRecordPartHandler::prefetchRow();
                                if (!row)
                                    break;
                            }
                            if (likely(!activity.hasMatchFilter || activity.helper->canMatch(row)))
                            {
                                // NB: rows from prefetch are filtered and translated
                                size32_t sz = activity.helper->transform(outBuilder.ensureRow(), row);
                                CDiskRecordPartHandler::prefetchDone();
                                if (sz)
                                {
                                    firstInGroup = false;
                                    return outBuilder.finalizeRowClear(sz);
                                }
                            }
                            else
                                CDiskRecordPartHandler::prefetchDone();
                        }
                    }
                    else
                    {
                        for (;;)
                        {
                            OwnedConstThorRow row = CDiskRecordPartHandler::nextRow();
                            if (!row)
                            {
                                if (!activity.grouped)
                                    break;
                                if (!firstInGroup)
                                {
                                    firstInGroup = true;
                                    return NULL;
                                }
                                row.setown(CDiskRecordPartHandler::nextRow());
                                if (!row)
                                    break;
                            }
                            if (likely(!activity.hasMatchFilter || activity.helper->canMatch(row)))
                            {
                                firstInGroup = false;
                                return row.getClear();
                            }
                        }
                    }
                }
                catch (IException *e)
                {
                    eoi = true;
                    StringBuffer s;
                    e->errorMessage(s);
                    s.append(" - handling file: ").append(filename.get());
                    IException *e2 = MakeActivityException(&activity, e, "%s", s.str());
                    e->Release();
                    throw e2;
                }
            }
            eoi = true;
            return NULL;
        }

        virtual void getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) const override
        {
            CDiskRecordPartHandler::getMetaInfo(info, partDesc);
            if (activity.helper->transformMayFilter() || activity.hasMatchFilter || (TDRkeyed & activity.helper->getFlags()))
            {
                info.totalRowsMin = 0; // all bets off! 
                info.unknownRowsOutput = info.canReduceNumRows = true;
                info.byteTotal = (offset_t)-1;
            }
            else
                info.fastThrough = true;
        }
    };

    class PgRecordSize : implements IRecordSize, public CSimpleInterface
    {
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        PgRecordSize(IRecordSize *_rcSz)
        {
            rcSz = LINK(_rcSz);
        }
        ~PgRecordSize()
        {
            rcSz->Release();
        }
        virtual size32_t getRecordSize(const void *rec)
        {
            return rcSz->getRecordSize(rec) + 1;
        }
        virtual size32_t getFixedSize() const 
        {
            return rcSz->getFixedSize()?(rcSz->getFixedSize()+1):0;
        }
    private:
        IRecordSize *rcSz;
    };

public:
    bool unsorted = false, countSent = false;
    IRowStream *out = nullptr;

    IHThorDiskReadArg *helper;

    CDiskReadSlaveActivity(CGraphElementBase *_container, IHThorArg *_helper) : CDiskReadSlaveActivityRecord(_container, _helper)
    {
        helper = (IHThorDiskReadArg *)queryHelper();
        unsorted = 0 != (TDRunsorted & helper->getFlags());
        grouped = 0 != (TDXgrouped & helper->getFlags());
        needTransform = helper->needTransform() || (TDRkeyed & helper->getFlags());
        hasMatchFilter = helper->hasMatchFilter();
        appendOutputLinked(this);
    }
    ~CDiskReadSlaveActivity()
    {
        ::Release(out);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityRecord::init(data, slaveData);
        partHandler.setown(new CDiskPartHandler(*this));

        if (partDescs.ordinality())
        {
            IPartDescriptor &firstPart = partDescs.item(0);
            bool dfsGrouped = firstPart.queryOwner().isGrouped(); // must be consistent across parts.
            if (dfsGrouped != grouped) // NB: will already have warned in master->wuid.
                ActPrintLog("Dfsgrouping vs codegen grouping differ, DFS grouping takes precedence. DfsGrouping=%s", dfsGrouped?"true":"false");
            grouped = dfsGrouped;
            if (isFixedDiskWidth && grouped) diskRowMinSz++;
        }
        if (grouped)
        {
            if (unsorted)
            {
                Owned<IException> e = MakeActivityWarning(this, 0, "Diskread - ignoring 'unsorted' because marked 'grouped'");
                fireException(e);
                unsorted = false;
            }
        }
    }
    virtual void kill()
    {
        if (out)
        {
            out->Release();
            out = NULL;
        }
        CDiskReadSlaveActivityRecord::kill();
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        if (!gotMeta)
        {
            gotMeta = true;
            initMetaInfo(cachedMetaInfo);
            cachedMetaInfo.isSource = true;
            getPartsMetaInfo(cachedMetaInfo, partDescs.ordinality(), ((IArrayOf<IPartDescriptor> &) partDescs).getArray(), partHandler);
        }
        info = cachedMetaInfo;
        if (info.totalRowsMin==info.totalRowsMax)
            ActPrintLog("DISKREAD: Number of rows to read: %" I64F "d", info.totalRowsMin);
        else
            ActPrintLog("DISKREAD: Number of rows to read: %" I64F "d (min), %" I64F "d (max)", info.totalRowsMin, info.totalRowsMax);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDiskReadSlaveActivityRecord::start();
        if (helper->getFlags() & TDRlimitskips)
            limit = RCMAX;
        else
            limit = (rowcount_t)helper->getRowLimit();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        if (!helper->transformMayFilter() && !hasMatchFilter)
        {
            remoteLimit = stopAfter;
            if (limit && (limit < remoteLimit))
                remoteLimit = limit+1; // 1 more to ensure triggered when received back. // JCSMORE remote side could handle skip too..
        }
        out = createSequentialPartHandler(partHandler, partDescs, grouped); // **
    }
    virtual bool isGrouped() const override { return grouped; }

// IRowStream
    virtual void stop()
    {
        if (out)
        {
            out->stop();
            out->Release();
            out = NULL;
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (NULL == out) // guard against, but shouldn't happen
            return NULL;
        OwnedConstThorRow ret = out->nextRow();
        if (!ret)
            return NULL;
        rowcount_t c = getDataLinkCount();
        if (stopAfter && (c >= stopAfter))  // NB: only slave limiter, global performed in chained choosen activity
            return NULL;
        if (c >= limit) // NB: only slave limiter, global performed in chained limit activity
        {
            helper->onLimitExceeded();
            return NULL;
        }
        dataLinkIncrement();
        return ret.getClear();
    }

friend class CDiskPartHandler;
};


CActivityBase *createDiskReadSlave(CGraphElementBase *container, IHThorArg *helper)
{
    return new CDiskReadSlaveActivity(container, helper);
}


//
// CDiskNormalizeSlave
//

class CDiskNormalizeSlave : public CDiskReadSlaveActivityRecord
{
    typedef CDiskReadSlaveActivityRecord PARENT;

    class CNormalizePartHandler : public CDiskRecordPartHandler
    {
        typedef CDiskRecordPartHandler PARENT;

        CDiskNormalizeSlave &activity;
        const void * nextrow;

    public:
        CNormalizePartHandler(CDiskNormalizeSlave &_activity) 
            : CDiskRecordPartHandler(_activity), activity(_activity)
        {
            nextrow = NULL;
        }
        virtual void close(CRC32 &fileCRC) override
        {
            if (nextrow)
                prefetchDone();
            PARENT::close(fileCRC);
        }
        const void *nextRow() override
        {
            // logic here is a bit obscure
            if (eoi || activity.queryAbortSoon())
            {
                eoi = true;
                return NULL;
            }
            for (;;)
            {
                if (nextrow)
                {
                    while (activity.helper->next())
                    {
                        size32_t sz = activity.helper->transform(outBuilder.ensureRow());
                        if (sz) 
                            return outBuilder.finalizeRowClear(sz);
                    }
                    prefetchDone();
                }
                nextrow = prefetchRow();
                if (!nextrow)
                    break;
                if (activity.helper->first(nextrow))
                {
                    size32_t sz = activity.helper->transform(outBuilder.ensureRow());
                    if (sz)
                        return outBuilder.finalizeRowClear(sz);
                    continue; // into next loop above
                }
                nextrow = NULL;
                prefetchDone();
            }
            eoi = true;
            return NULL;
        }
  
    };

    IHThorDiskNormalizeArg *helper;
    IRowStream *out = nullptr;

public:
    CDiskNormalizeSlave(CGraphElementBase *_container) 
        : CDiskReadSlaveActivityRecord(_container)
    {
        helper = (IHThorDiskNormalizeArg *)queryHelper();
        appendOutputLinked(this);
    }
    ~CDiskNormalizeSlave()
    {
        ::Release(out);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityRecord::init(data, slaveData);
        partHandler.setown(new CNormalizePartHandler(*this));
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        if (!gotMeta)
        {
            gotMeta = true;
            initMetaInfo(cachedMetaInfo);
            cachedMetaInfo.isSource = true;
            getPartsMetaInfo(cachedMetaInfo, partDescs.ordinality(), ((IArrayOf<IPartDescriptor> &) partDescs).getArray(), partHandler);
            cachedMetaInfo.unknownRowsOutput = true; // JCSMORE
        }
        info = cachedMetaInfo;
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDiskReadSlaveActivityRecord::start();
        if (helper->getFlags() & TDRlimitskips)
            limit = RCMAX;
        else
            limit = (rowcount_t)helper->getRowLimit();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        out = createSequentialPartHandler(partHandler, partDescs, false);
    }
    virtual bool isGrouped() const override { return false; }

// IRowStream
    virtual void stop()
    {
        if (out)
        {
            out->stop();
            out->Release();
            out = NULL;
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!out)
            return NULL;
        OwnedConstThorRow ret = out->nextRow();
        if (!ret)
            return NULL;
        rowcount_t c = getDataLinkCount();
        if (stopAfter && (c >= stopAfter)) // NB: only slave limiter, global performed in chained choosen activity
            return NULL;
        if (c >= limit) // NB: only slave limiter, global performed in chained limit activity
        {
            helper->onLimitExceeded();
            return NULL;
        }
        dataLinkIncrement();
        return ret.getClear();
    }

friend class CNormalizePartHandler;
};


CActivityBase *createDiskNormalizeSlave(CGraphElementBase *container)
{
    return new CDiskNormalizeSlave(container);
}

class CDiskSimplePartHandler : public CDiskRecordPartHandler
{
public:
    CDiskSimplePartHandler(CDiskReadSlaveActivityRecord &activity) : CDiskRecordPartHandler(activity)
    {
        eoi = false;
    }

    const void *nextRow()
    {
        if (!eoi)
        {
            try
            {
                if (!activity.queryAbortSoon())
                {
                    OwnedConstThorRow row = CDiskRecordPartHandler::nextRow();
                    if (row) 
                        return row.getClear();
                }
            }
            catch (IException *e)
            {
                StringBuffer s;
                e->errorMessage(s);
                s.append(" - handling file: ").append(filename.get());
                IException *e2 = MakeActivityException(&activity, e, "%s", s.str());
                e->Release();
                eoi = true;
                throw e2;
            }
        }
        eoi = true;
        return NULL;
    }
};

class CDiskAggregateSlave : public CDiskReadSlaveActivityRecord
{
    typedef CDiskReadSlaveActivityRecord PARENT;

    IHThorDiskAggregateArg *helper;
    Owned<IEngineRowAllocator> allocator;
    bool eoi, hadElement;
    CPartialResultAggregator aggregator;

public:
    CDiskAggregateSlave(CGraphElementBase *_container) 
        : CDiskReadSlaveActivityRecord(_container), aggregator(*this)
    {
        helper = (IHThorDiskAggregateArg *)queryHelper();
        eoi = false;
        allocator.set(queryRowAllocator());
        appendOutputLinked(this);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityRecord::init(data, slaveData);
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
        partHandler.setown(new CDiskSimplePartHandler(*this));
    }
    virtual void abort()
    {
        CDiskReadSlaveActivityRecord::abort();
        if (!container.queryLocalOrGrouped() && firstNode())
            aggregator.cancelGetResult();
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        info.totalRowsMin = 0;
        info.totalRowsMax = 1;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDiskReadSlaveActivityRecord::start();
        eoi = hadElement = false;
    }

// IRowStream
    virtual void stop()
    {
        if (partHandler)
            partHandler->stop();
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eoi)
            return NULL;
        RtlDynamicRowBuilder row(allocator);
        helper->clearAggregate(row);
        unsigned part = 0;
        while (!abortSoon && part<partDescs.ordinality())
        {
            partHandler->setPart(&partDescs.item(part));
            ++part;
            for (;;)
            {
                OwnedConstThorRow nextrow =  partHandler->nextRow();
                if (!nextrow)
                    break;
                hadElement = true;
                helper->processRow(row, nextrow); // can change row size TBD
            }
        }
        eoi = true;
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

CActivityBase *createDiskAggregateSlave(CGraphElementBase *container)
{
    return new CDiskAggregateSlave(container);
}


class CDiskCountSlave : public CDiskReadSlaveActivityRecord
{
    typedef CDiskReadSlaveActivityRecord PARENT;

    IHThorDiskCountArg *helper;
    rowcount_t preknownTotalCount = 0;
    bool eoi = false, totalCountKnown = false;

public:
    CDiskCountSlave(CGraphElementBase *_container) : CDiskReadSlaveActivityRecord(_container)
    {
        helper = (IHThorDiskCountArg *)queryHelper();
        appendOutputLinked(this);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityRecord::init(data, slaveData);
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
        data.read(totalCountKnown);
        data.read(preknownTotalCount);
        partHandler.setown(new CDiskSimplePartHandler(*this));
    }
    virtual void abort()
    {
        CDiskReadSlaveActivityRecord::abort();
        if (!container.queryLocalOrGrouped() && firstNode())
            cancelReceiveMsg(0, mpTag);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        if (totalCountKnown)
            info.fastThrough = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDiskReadSlaveActivityRecord::start();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        if (!helper->hasFilter())
            remoteLimit = stopAfter;
        eoi = false;
        if (!helper->canMatchAny())
        {
            totalCountKnown = true;
            preknownTotalCount = 0;
        }
    }

// IRowStream
    virtual void stop()
    {
        if (partHandler)
            partHandler->stop();
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eoi)
            return NULL;
        unsigned __int64 totalCount = 0;
        eoi = true;
        if (totalCountKnown)
        {
            totalCount = preknownTotalCount;
            if (!container.queryLocalOrGrouped() && !firstNode())
                return NULL;
        }
        else
        {
            unsigned part = 0;
            while (!abortSoon && part<partDescs.ordinality())
            {
                partHandler->setPart(&partDescs.item(part));
                ++part;
                for (;;) {
                    OwnedConstThorRow nextrow = partHandler->nextRow();
                    if (!nextrow)
                        break;
                    totalCount += helper->numValid(nextrow);
                    if (totalCount > stopAfter)
                        break;
                }
                if (totalCount > stopAfter)
                    break;
            }
            if (!container.queryLocalOrGrouped())
            {
                sendPartialCount(*this, (rowcount_t)totalCount);
                if (!firstNode())
                    return NULL;
                totalCount = getFinalCount(*this);
            }
            if (totalCount > stopAfter)
                totalCount = stopAfter;
        }
        RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
        CThorStreamDeserializerSource ssz(sizeof(totalCount),&totalCount);
        size32_t sz = queryRowDeserializer()->deserialize(rowBuilder,ssz);
        dataLinkIncrement();
        return rowBuilder.finalizeRowClear(sz);
    }
};


CActivityBase *createDiskCountSlave(CGraphElementBase *container)
{
    return new CDiskCountSlave(container);
}

class CDiskGroupAggregateSlave 
  : public CDiskReadSlaveActivityRecord, implements IHThorGroupAggregateCallback
{
    typedef CDiskReadSlaveActivityRecord PARENT;

    IHThorDiskGroupAggregateArg *helper;
    bool gathered, eoi;
    Owned<IAggregateTable> localAggTable;
    Owned<IRowStream> aggregateStream;
    Owned<IEngineRowAllocator> allocator;
    bool merging;
    Owned<IHashDistributor> distributor;
    
public:
    IMPLEMENT_IINTERFACE_USING(CDiskReadSlaveActivityRecord);

    CDiskGroupAggregateSlave(CGraphElementBase *_container) 
        : CDiskReadSlaveActivityRecord(_container)
    {
        helper = (IHThorDiskGroupAggregateArg *)queryHelper();
        merging = false;
        appendOutputLinked(this);
    }

// IHThorGroupAggregateCallback
    virtual void processRow(const void *next)
    {
        localAggTable->addRow(next);
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityRecord::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
        partHandler.setown(new CDiskSimplePartHandler(*this));
        allocator.set(queryRowAllocator());
    }
    virtual void abort()
    {
        CDiskReadSlaveActivityRecord::abort();
        if (merging)
            queryJobChannel().queryJobComm().cancel(0, mpTag);
    }
// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDiskReadSlaveActivityRecord::start();
        gathered = eoi = false;
        localAggTable.setown(createRowAggregator(*this, *helper, *helper));
        localAggTable->init(queryRowAllocator());
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
// IRowStream
    virtual void stop() override
    {
        if (partHandler)
            partHandler->stop();
        if (aggregateStream)
        {
            aggregateStream->stop();
            if (distributor)
            {
                distributor->disconnect(true);
                distributor->join();
            }
            aggregateStream.clear();
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eoi)
            return NULL;
        if (!gathered)
        {
            try
            {
                unsigned part = 0;
                while (!abortSoon && part<partDescs.ordinality())
                {
                    partHandler->setPart(&partDescs.item(part));
                    ++part;
                    for (;;)
                    {
                        OwnedConstThorRow nextrow = partHandler->nextRow();
                        if (!nextrow)
                            break;
                        helper->processRow(nextrow, this);
                    }
                }
                gathered = true;
                ActPrintLog("DISKGROUPAGGREGATE: Local aggregate table contains %d entries", localAggTable->elementCount());

                if (!container.queryLocalOrGrouped() && container.queryJob().querySlaves()>1)
                {
                    Owned<IRowStream> localAggStream = localAggTable->getRowStream(true);
                    BooleanOnOff onOff(merging);
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
        eoi = true;
        return NULL;
    }
};

CActivityBase *createDiskGroupAggregateSlave(CGraphElementBase *container)
{
    return new CDiskGroupAggregateSlave(container);
}
