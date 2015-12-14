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
#include "jsort.hpp"

#include "jfile.hpp"
#include "jlzw.hpp"
#include "jset.hpp"

#include "commonext.hpp"
#include "dadfs.hpp"

#include "thactivityutil.ipp"
#include "backup.hpp"

#include "slave.ipp"
#include "thbuf.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thormisc.hpp"
#include "thorport.hpp"

//#define TRACE_STARTSTOP_EXCEPTIONS

#ifdef _DEBUG
//#define _FULL_TRACE
#endif

#define MAX_ROW_ARRAY_SIZE (0x100000*64) // 64MB



#define TRANSFER_TIMEOUT (60*60*1000)
#define JOIN_TIMEOUT (10*60*1000)


#ifdef _MSC_VER
#pragma warning(push)
#pragma warning( disable : 4355 )
#endif
class ThorLookaheadCache: public IThorDataLink, public CSimpleInterface
{
    rowcount_t count;
    Linked<IThorDataLink> in;
    Owned<ISmartRowBuffer> smartbuf;
    size32_t bufsize;
    CActivityBase &activity;
    bool allowspill, preserveGrouping;
    ISmartBufferNotify *notify;
    bool running;
    bool stopped;
    rowcount_t required;
    Semaphore startsem;
    bool started;
    Owned<IException> startexception;
    Owned<IException> getexception;
    bool asyncstart;

    class Cthread: public Thread
    {
        ThorLookaheadCache &parent;
    public:
        Cthread(ThorLookaheadCache &_parent)
            : Thread("ThorLookaheadCache"), parent(_parent)
        {
        }
        int run()
        {
            return parent.run();
        }
    } thread;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    void doNotify()
    {
        if (notify)
            notify->onInputFinished(count);
        if (smartbuf)
            smartbuf->queryWriter()->flush();
    }

    int run()
    {
        if (!started) {
            try {
                in->start();
                started = true;
            }
            catch(IException * e)
            {
                ActPrintLog(&activity, e, "ThorLookaheadCache starting input");
                startexception.setown(e);
                if (asyncstart) 
                    notify->onInputStarted(startexception);
                running = false;
                stopped = true;
                startsem.signal();
                return 0;
            }
        }
        try {
            StringBuffer temp;
            if (allowspill)
                GetTempName(temp,"lookahd",true);
            assertex(bufsize);
            if (allowspill)
                smartbuf.setown(createSmartBuffer(&activity, temp.str(), bufsize, queryRowInterfaces(in)));
            else
                smartbuf.setown(createSmartInMemoryBuffer(&activity, queryRowInterfaces(in), bufsize));
            if (notify) 
                notify->onInputStarted(NULL);
            startsem.signal();
            IRowWriter *writer = smartbuf->queryWriter();
            if (preserveGrouping)
            {
                while (required&&running)
                {
                    OwnedConstThorRow row = in->nextRow();
                    if (!row)
                    {
                        row.setown(in->nextRow());
                        if (!row)
                            break;
                        else
                            writer->putRow(NULL); // eog
                    }
                    ++count;
                    writer->putRow(row.getClear());
                    if (required!=RCUNBOUND)
                        required--;
                }
            }
            else
            {
                while (required&&running)
                {
                    OwnedConstThorRow row = in->ungroupedNextRow();
                    if (!row)
                        break;
                    ++count;
                    writer->putRow(row.getClear());
                    if (required!=RCUNBOUND)
                        required--;
                }
            }
        }
        catch(IException * e)
        {
            ActPrintLog(&activity, e, "ThorLookaheadCache get exception");
            getexception.setown(e);
        }

        // notify and flush async, as these can block, but we do not want to block in->stop()
        // especially if this is a spilling read ahead, where use case scenarios include not wanting to
        // block the upstream input.
        // An example is a firstn which if stop() it not called, it may block
        // other nodes from pulling because it is blocked upstream on full buffers (which can be discarded
        // on stop()), and those in turn are blocking other arms of the graph.
        class CNotifyThread : implements IThreaded
        {
            CThreaded threaded;
            ThorLookaheadCache &owner;
        public:
            CNotifyThread(ThorLookaheadCache &_owner) : threaded("Lookahead-CNotifyThread"), owner(_owner)
            {
                threaded.init(this);
            }
            ~CNotifyThread()
            {
                loop
                {
                    if (threaded.join(60000))
                        break;
                    PROGLOG("Still waiting on lookahead CNotifyThread thread to complete");
                }
            }
        // IThreaded impl.
            virtual void main()
            {
                owner.doNotify();
            }
        } notifyThread(*this);

        running = false;
        try
        {
            if (in)
                in->stop();
        }
        catch(IException * e)
        {
            ActPrintLog(&activity, e, "ThorLookaheadCache stop exception");
            if (!getexception.get())
                getexception.setown(e);
        }
        // NB: Will wait on CNotifyThread to finish before returning
        return 0;
    }
        

    ThorLookaheadCache(CActivityBase &_activity, IThorDataLink *_in,size32_t _bufsize,bool _allowspill,bool _preserveGrouping, rowcount_t _required,ISmartBufferNotify *_notify, bool _instarted, IDiskUsage *_iDiskUsage)
        : thread(*this), activity(_activity), in(_in)
    {
#ifdef _FULL_TRACE
        ActPrintLog(&activity, "ThorLookaheadCache create %x",(unsigned)(memsize_t)this);
#endif
        asyncstart = false;
        allowspill = _allowspill;
        preserveGrouping = _preserveGrouping;
        assertex((unsigned)-1 != _bufsize); // no longer supported
        bufsize = _bufsize?_bufsize:(0x40000*3); // use .75 MB buffer if bufsize omitted
        notify = _notify;
        running = true;
        required = _required;
        count = 0;
        stopped = true;
        started = _instarted;
    }

    ~ThorLookaheadCache()
    {
        if (!thread.join(1000*60))
            ActPrintLogEx(&activity.queryContainer(), thorlog_all, MCuserWarning, "ThorLookaheadCache join timedout");
    }

    void start()
    {
#ifdef _FULL_TRACE
        ActPrintLog(&activity, "ThorLookaheadCache start %x",(unsigned)(memsize_t)this);
#endif
        stopped = false;
        asyncstart = notify&&notify->startAsync();
        thread.start();
        if (!asyncstart) {
            startsem.wait();
            if (startexception) 
                throw startexception.getClear();
        }
    }

    void stop()
    {
#ifdef _FULL_TRACE
        ActPrintLog(&activity, "ThorLookaheadCache stop %x",(unsigned)(memsize_t)this);
#endif
        if (!stopped) {
            running = false;
            if (smartbuf)
                smartbuf->stop(); // just in case blocked
            thread.join();
            stopped = true;
            if (getexception) 
                throw getexception.getClear();
        }
    }

    const void *nextRow()
    {
        OwnedConstThorRow row = smartbuf->nextRow();
        if (getexception) 
            throw getexception.getClear();
        if (!row) {
#ifdef _FULL_TRACE
            ActPrintLog(&activity, "ThorLookaheadCache eos %x",(unsigned)(memsize_t)this);
#endif
        }
        return row.getClear();
    }

    bool isGrouped() { return preserveGrouping; }
            
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        memset(&info,0,sizeof(info));
        in->getMetaInfo(info);
        // more TBD
    }

    CActivityBase *queryFromActivity()
    {
        return in->queryFromActivity();
    }
    void dataLinkSerialize(MemoryBuffer &mb)
    {
        // no serialization information (yet)
    }
    unsigned __int64 queryTotalCycles() const { return in->queryTotalCycles(); }
    unsigned __int64 queryEndCycles() const { return in->queryEndCycles(); }
    virtual void debugRequest(MemoryBuffer &msg) { return in->debugRequest(msg); }
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif


IThorDataLink *createDataLinkSmartBuffer(CActivityBase *activity, IThorDataLink *in, size32_t bufsize, bool allowspill, bool preserveGrouping, rowcount_t maxcount, ISmartBufferNotify *notify, bool instarted, IDiskUsage *iDiskUsage)
{
    return new ThorLookaheadCache(*activity, in,bufsize,allowspill,preserveGrouping,maxcount,notify,instarted,iDiskUsage);
}


void initMetaInfo(ThorDataLinkMetaInfo &info)
{
    memset(&info,0,sizeof(info));
    //info.rowsdone = xx;
    info.totalRowsMin = 0;          
    info.totalRowsMax = -1; // rely on inputs to set
    info.spilled = (offset_t)-1;
    info.byteTotal = (offset_t)-1;
    info.rowsOutput = 0;
}




void CThorDataLink::initMetaInfo(ThorDataLinkMetaInfo &info)
{
    ::initMetaInfo(info);
    info.rowsOutput = getDataLinkCount();
    // more
}

void CThorDataLink::calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink *link)
{
    if (!info.unknownRowsOutput&&link&&((info.totalRowsMin<=0)||(info.totalRowsMax<0))) {
        ThorDataLinkMetaInfo prev;
        link->getMetaInfo(prev);
        if (info.totalRowsMin<=0) {
            if (!info.canReduceNumRows)
                info.totalRowsMin = prev.totalRowsMin;
            else
                info.totalRowsMin = 0;
        }
        if (info.totalRowsMax<0) {
            if (!info.canIncreaseNumRows) {
                info.totalRowsMax = prev.totalRowsMax;
                if (info.totalRowsMin>info.totalRowsMax)
                    info.totalRowsMax = -1;
            }
        }
        if (((offset_t)-1 != prev.byteTotal) && info.totalRowsMin == info.totalRowsMax)
            info.byteTotal = prev.byteTotal;
    }
    else if (info.totalRowsMin<0)
        info.totalRowsMin = 0; // a good bet

}

void CThorDataLink::calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink **link,unsigned ninputs)
{
    if (!link||(ninputs<=1)) {
        calcMetaInfoSize(info,link&&(ninputs==1)?link[0]:NULL);
        return ;
    }
    if (!info.unknownRowsOutput) {
        __int64 min=0;
        __int64 max=0;
        for (unsigned i=0;i<ninputs;i++ ) {
            if (link[i]) {
                ThorDataLinkMetaInfo prev;
                link[i]->getMetaInfo(prev);
                if (min>=0) {
                    if (prev.totalRowsMin>=0)
                        min += prev.totalRowsMin;
                    else
                        min = -1;
                }
                if (max>=0) {
                    if (prev.totalRowsMax>=0)
                        max += prev.totalRowsMax;
                    else
                        max = -1;
                }
            }
        }
        if (info.totalRowsMin<=0) {
            if (!info.canReduceNumRows)
                info.totalRowsMin = min;
            else
                info.totalRowsMin = 0;
        }
        if (info.totalRowsMax<0) {
            if (!info.canIncreaseNumRows) {
                info.totalRowsMax = max;
                if (info.totalRowsMin>info.totalRowsMax)
                    info.totalRowsMax = -1;
            }
        }
    }
    else if (info.totalRowsMin<0)
        info.totalRowsMin = 0; // a good bet
}

void CThorDataLink::calcMetaInfoSize(ThorDataLinkMetaInfo &info, ThorDataLinkMetaInfo *infos,unsigned num)
{
    if (!infos||(num<=1)) {
        if (1 == num)
            info = infos[0];
        return;
    }
    if (!info.unknownRowsOutput) {
        __int64 min=0;
        __int64 max=0;
        for (unsigned i=0;i<num;i++ ) {
            ThorDataLinkMetaInfo &prev = infos[i];
            if (min>=0) {
                if (prev.totalRowsMin>=0)
                    min += prev.totalRowsMin;
                else
                    min = -1;
            }
            if (max>=0) {
                if (prev.totalRowsMax>=0)
                    max += prev.totalRowsMax;
                else
                    max = -1;
            }
        }
        if (info.totalRowsMin<=0) {
            if (!info.canReduceNumRows)
                info.totalRowsMin = min;
            else
                info.totalRowsMin = 0;
        }
        if (info.totalRowsMax<0) {
            if (!info.canIncreaseNumRows) {
                info.totalRowsMax = max;
                if (info.totalRowsMin>info.totalRowsMax)
                    info.totalRowsMax = -1;
            }
        }
    }
    else if (info.totalRowsMin<0)
        info.totalRowsMin = 0; // a good bet
}

static bool canStall(CActivityBase *act)
{
    if (!act)
        return false;
    unsigned i=0;
    IThorDataLink *inp;
    while ((inp=((CSlaveActivity *)act)->queryInput(i++))!=NULL) {
        ThorDataLinkMetaInfo info;
        inp->getMetaInfo(info);
        if (info.canStall)
            return true;
        if (!info.isSource&&!info.buffersInput&&!info.canBufferInput)
            if (canStall((CSlaveActivity *)inp->queryFromActivity()))
                return true;
    }
    return false;
}

bool isSmartBufferSpillNeeded(CActivityBase *act)
{
    // two part - forward and reverse checking
    // first reverse looking for stalling activities
    if (!canStall((CSlaveActivity *)act))
        return false;
    // now check
    return true;
}

bool checkSavedFileCRC(IFile * ifile, bool & timesDiffer, unsigned & storedCrc)
{
    StringBuffer s(ifile->queryFilename());
    s.append(".crc");
    Owned<IFile> crcFile = createIFile(s.str());
    size32_t crcSz = (size32_t)crcFile->size();
    Owned<IFileIO> crcIO = crcFile->open(IFOread);
    bool performCrc = false;
    timesDiffer = false;

    if (crcIO)
    {
        Owned<IFileIOStream> crcStream = createIOStream(crcIO);

        if (sizeof(storedCrc) == crcSz) // backward compat. if = in size to just crc (no date stamps)
        {
            verifyex(crcSz == crcStream->read(crcSz, &storedCrc));
            performCrc = true;
        }
        else
        {
            size32_t sz;
            verifyex(sizeof(sz) == crcStream->read(sizeof(sz), &sz));
            void *mem = malloc(sz);
            MemoryBuffer mb;
            mb.setBuffer(sz, mem, true);
            verifyex(sz == crcStream->read(sz, mem));

            CDateTime storedCreateTime(mb);
            CDateTime storedModifiedTime(mb);

            CDateTime createTime, modifiedTime, accessedTime;
            ifile->getTime(&createTime, &modifiedTime, &accessedTime);

            if (!storedCreateTime.equals(createTime) || !storedModifiedTime.equals(modifiedTime))
                timesDiffer = true;
            else
            {
                mb.read(storedCrc);
                performCrc = true;
            }
        }
    }
    return performCrc;
}



static void _doReplicate(CActivityBase *activity, IPartDescriptor &partDesc, ICopyFileProgress *iProgress)
{
    StringBuffer primaryName;
    getPartFilename(partDesc, 0, primaryName);;
    RemoteFilename rfn;
    IFileDescriptor &fileDesc = partDesc.queryOwner();
    unsigned copies = partDesc.numCopies();
    unsigned c=1;
    for (; c<copies; c++)
    {
        unsigned replicateCopy;
        unsigned clusterNum = partDesc.copyClusterNum(c, &replicateCopy);
        rfn.clear();
        partDesc.getFilename(c, rfn);
        StringBuffer dstName;
        rfn.getPath(dstName);
        assertex(dstName.length());

        if (replicateCopy>0 )  
        {
            try
            {
                queryThor().queryBackup().backup(dstName.str(), primaryName.str());
            }
            catch (IException *e)
            {
                Owned<IThorException> re = MakeActivityWarning(activity, e, "Failed to create replicate file '%s'", dstName.str());
                e->Release();
                activity->fireException(re);
            }
        }
        else // another primary
        {
            ActPrintLog(activity, "Copying to primary %s", dstName.str());
            StringBuffer tmpName(dstName.str());
            tmpName.append(".tmp");
            OwnedIFile tmpIFile = createIFile(tmpName.str());
            OwnedIFile srcFile = createIFile(primaryName.str());
            CFIPScope fipScope(tmpName.str());
            try
            {
                try
                {
                    ensureDirectoryForFile(dstName.str());
                    ::copyFile(tmpIFile, srcFile, 0x100000, iProgress);
                }
                catch (IException *e)
                {
                    IThorException *re = MakeActivityException(activity, e, "Failed to copy to tmp file '%s' from source file '%s'", tmpIFile->queryFilename(), srcFile->queryFilename());
                    e->Release();
                    throw re;
                }
                try
                {
                    OwnedIFile dstIFile = createIFile(dstName.str());
                    dstIFile->remove();
                    tmpIFile->rename(pathTail(dstName.str()));
                }
                catch (IException *e)
                {
                    IThorException *re = ThorWrapException(e, "Failed to rename '%s' to '%s'", tmpName.str(), dstName.str());
                    e->Release();
                    throw re;
                }
            }
            catch (IException *)
            {
                try { tmpIFile->remove(); }
                catch (IException *e) { ActPrintLog(&activity->queryContainer(), e); e->Release(); }
                throw;
            }
        }
    }
}

void cancelReplicates(CActivityBase *activity, IPartDescriptor &partDesc)
{
    RemoteFilename rfn;
    IFileDescriptor &fileDesc = partDesc.queryOwner();
    unsigned copies = partDesc.numCopies();
    unsigned c=1;
    for (; c<copies; c++)
    {
        unsigned replicateCopy;
        unsigned clusterNum = partDesc.copyClusterNum(c, &replicateCopy);
        rfn.clear();
        partDesc.getFilename(c, rfn);
        StringBuffer dstName;
        rfn.getPath(dstName);
        assertex(dstName.length());

        if (replicateCopy>0)  
        {
            try
            {
                queryThor().queryBackup().cancel(dstName.str());
            }
            catch (IException *e)
            {
                Owned<IThorException> re = MakeActivityException(activity, e, "Error cancelling backup '%s'", dstName.str());
                ActPrintLog(&activity->queryContainer(), e);
                e->Release();
            }
        }
    }
}

void doReplicate(CActivityBase *activity, IPartDescriptor &partDesc, ICopyFileProgress *iProgress)
{
    try
    {
        _doReplicate(activity, partDesc, iProgress);
    }
    catch (IException *e)
    {
        Owned<IThorException> e2 = MakeActivityWarning(activity, e, "doReplicate");
        e->Release();
        activity->fireException(e2);
    }
}

class CWriteHandler : public CSimpleInterface, implements IFileIO
{
    Linked<IFileIO> primaryio;
    Linked<IFile> primary;
    StringBuffer primaryName;
    ICopyFileProgress *iProgress;
    bool *aborted;
    CActivityBase &activity;
    IPartDescriptor &partDesc;
    bool remote;
    CFIPScope fipScope;
    unsigned twFlags;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWriteHandler(CActivityBase &_activity, IPartDescriptor &_partDesc, IFile *_primary, IFileIO *_primaryio, ICopyFileProgress *_iProgress, unsigned _twFlags, bool *_aborted)
        : activity(_activity), partDesc(_partDesc), primary(_primary), primaryio(_primaryio), iProgress(_iProgress), twFlags(_twFlags), aborted(_aborted), fipScope(primary->queryFilename())
    {
        RemoteFilename rfn;
        partDesc.getFilename(0, rfn);
        remote = !rfn.isLocal();
        rfn.getPath(primaryName);
        if (globals->getPropBool("@replicateAsync", true))
            cancelReplicates(&activity, partDesc);
    }
    ~CWriteHandler()
    {
        primaryio.clear(); // should close
        if (aborted && *aborted)
        {
            primary->remove(); // i.e. never completed, so remove partial (temp) primary
            return;
        }
        if (twFlags & TW_RenameToPrimary)
        {
            OwnedIFile tmpIFile;
            CFIPScope fipScope;
            if (remote && !(twFlags & TW_External))
            {
                StringBuffer tmpName(primaryName.str());
                tmpName.append(".tmp");
                tmpIFile.setown(createIFile(tmpName.str()));
                fipScope.set(tmpName.str());
                try
                {
                    try
                    {
                        ensureDirectoryForFile(primaryName.str());
                        ::copyFile(tmpIFile, primary, 0x100000, iProgress);
                    }
                    catch (IException *e)
                    {
                        IThorException *re = ThorWrapException(e, "Failed to copy local temp file '%s' to remote temp location '%s'", primary->queryFilename(), tmpIFile->queryFilename());
                        e->Release();
                        throw re;
                    }
                }
                catch (IException *)
                {
                    try { tmpIFile->remove(); }
                    catch (IException *e) { ActPrintLog(&activity.queryContainer(), e); e->Release(); }
                }
            }
            else
                tmpIFile.setown(createIFile(primary->queryFilename()));
            try
            {
                try
                {
                    OwnedIFile dstIFile = createIFile(primaryName.str());
                    dstIFile->remove();
                    tmpIFile->rename(pathTail(primaryName.str()));
                }
                catch (IException *e)
                {
                    IThorException *re = ThorWrapException(e, "Failed to rename '%s' to '%s'", tmpIFile->queryFilename(), primaryName.str());
                    e->Release();
                    throw re;
                }
            }
            catch (IException *)
            {
                try { primary->remove(); }
                catch (IException *e) { ActPrintLog(&activity.queryContainer(), e); e->Release(); }
                throw;
            }
            primary->remove();
            fipScope.clear();
        }
        if (partDesc.numCopies()>1)
            _doReplicate(&activity, partDesc, iProgress);
    }
// IFileIO impl.
    virtual size32_t read(offset_t pos, size32_t len, void * data) { return primaryio->read(pos, len, data); }
    virtual offset_t size() { return primaryio->size(); }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) { return primaryio->write(pos, len, data); }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=-1) { return primaryio->appendFile(file, pos, len); }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return primaryio->getStatistic(kind); }
    virtual void setSize(offset_t size) { primaryio->setSize(size); }
    virtual void flush() { primaryio->flush(); }
    virtual void close() { primaryio->close(); }
};

IFileIO *createMultipleWrite(CActivityBase *activity, IPartDescriptor &partDesc, unsigned recordSize, unsigned twFlags, bool &compress, ICompressor *ecomp, ICopyFileProgress *iProgress, bool *aborted, StringBuffer *_outLocationName)
{
    StringBuffer outLocationNameI;
    StringBuffer &outLocationName = _outLocationName?*_outLocationName:outLocationNameI;

    RemoteFilename rfn;
    partDesc.getFilename(0, rfn);
    StringBuffer primaryName;
    rfn.getPath(primaryName);
    if (twFlags & TW_Direct)
    {
        if (0 == outLocationName.length())
            outLocationName.append(primaryName.str());
    }
    else
    {
        // use temp name
        GetTempName(outLocationName, "partial");
        if (rfn.isLocal() || (twFlags & TW_External))
        { // ensure local tmp in same directory as target
            StringBuffer dir;
            splitDirTail(primaryName, dir);
            addPathSepChar(dir);
            dir.append(pathTail(outLocationName));
            outLocationName.swapWith(dir);
        }
        assertex(outLocationName.length());
        ensureDirectoryForFile(outLocationName.str());
    }
    OwnedIFile file = createIFile(outLocationName.str());
    Owned<IFileIO> fileio;
    if (compress)
    {
        if (activity->getOptBool(THOROPT_COMP_FORCELZW, false))
            recordSize = 0; // by default if fixed length (recordSize set), row diff compression is used. This forces LZW
        fileio.setown(createCompressedFileWriter(file, recordSize, 0 != (twFlags & TW_Extend), true, ecomp));
        if (!fileio)
        {
            compress = false;
            Owned<IThorException> e = MakeActivityWarning(activity, TE_LargeBufferWarning, "Could not write file '%s' compressed", outLocationName.str());
            activity->fireException(e);
            fileio.setown(file->open((twFlags & TW_Extend)&&file->exists()?IFOwrite:IFOcreate));
        }
    }
    else
        fileio.setown(file->open((twFlags & TW_Extend)&&file->exists()?IFOwrite:IFOcreate));
    if (!fileio)
        throw MakeActivityException(activity, TE_FileCreationFailed, "Failed to create file for write (%s) error = %d", outLocationName.str(), GetLastError());
    ActPrintLog(activity, "Writing to file: %s, compress=%s, rdiff=%s", file->queryFilename(), compress ? "true" : "false", (compress && recordSize) ? "true" : "false");
    return new CWriteHandler(*activity, partDesc, file, fileio, iProgress, twFlags, aborted);
}

StringBuffer &locateFilePartPath(CActivityBase *activity, const char *logicalFilename, IPartDescriptor &partDesc, StringBuffer &filePath)
{
    unsigned location;
    OwnedIFile ifile;
    if (globals->getPropBool("@autoCopyBackup", true)?ensurePrimary(activity, partDesc, ifile, location, filePath):getBestFilePart(activity, partDesc, ifile, location, filePath, activity))
        ActPrintLog(activity, "reading physical file '%s' (logical file = %s)", filePath.str(), logicalFilename);
    else
    {
        StringBuffer locations;
        IException *e = MakeActivityException(activity, TE_FileNotFound, "No physical file part for logical file %s, found at given locations: %s (Error = %d)", logicalFilename, getFilePartLocations(partDesc, locations).str(), GetLastError());
        ActPrintLog(&activity->queryContainer(), e);
        throw e;
    }
    return filePath;
}


IRowStream *createSequentialPartHandler(CPartHandler *partHandler, IArrayOf<IPartDescriptor> &partDescs, bool grouped)
{
    class CSeqPartHandler : public CSimpleInterface, implements IRowStream
    {
        IArrayOf<IPartDescriptor> &partDescs;
        int part, parts;
        bool eof, grouped, someInGroup;
        Linked<CPartHandler> partHandler;

        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    public:
        CSeqPartHandler(CPartHandler *_partHandler, IArrayOf<IPartDescriptor> &_partDescs, bool _grouped) 
            : partDescs(_partDescs), partHandler(_partHandler), grouped(_grouped)
        {
            part = 0;
            parts = partDescs.ordinality();
            someInGroup = false;
            if (0==parts)
            {
                eof = true;
            }
            else
            {
                eof = false;
                partHandler->setPart(&partDescs.item(0), 0);
            }
        }
        virtual void stop()
        {
            if (partHandler)
            {
                partHandler->stop();
                partHandler.clear();
            }
        }
        const void *nextRow()
        {
            if (eof)
            {
                return NULL;
            }
            loop
            {
                OwnedConstThorRow row = partHandler->nextRow();
                if (row)
                {
                    someInGroup = true;
                    return row.getClear();
                }
                if (grouped && someInGroup)
                {
                    someInGroup = false;
                    return NULL;
                }
                partHandler->stop();
                ++part;
                if (part >= parts)
                {
                    partHandler.clear();
                    eof = true;
                    return NULL;
                }
                partHandler->setPart(&partDescs.item(part), part);
            }
        }
    };
    return new CSeqPartHandler(partHandler, partDescs, grouped);
}

